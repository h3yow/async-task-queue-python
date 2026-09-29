"""任务服务：对外协调层。

- submit: 参数校验 + 处理器类型校验 + 幂等去重（同参重放返回既有任务）；
- get_task / get_by_idempotency_key: 查询，未知任务抛 UnknownTaskError；
- cancel: 仅允许取消非终态任务；PENDING/RETRY_WAIT 直接落 CANCELLED，
  RUNNING 则通知 worker 协作式取消；
- start/stop: 管理存储连接与 worker 生命周期。
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from taskqueue.config import TaskQueueConfig
from taskqueue.dbutil import run_db
from taskqueue.errors import (
    IdempotencyConflictError,
    InvalidStateTransitionError,
    UnknownFlowError,
    UnknownTaskError,
    ValidationError,
)
from taskqueue.models import (
    TERMINAL_STATES,
    FlowState,
    FlowStatus,
    FlowTaskSpec,
    Task,
    TaskState,
)
from taskqueue.registry import HandlerRegistry
from taskqueue.retry import RetryPolicy
from taskqueue.store import IdempotencyKeyTaken, TaskStore
from taskqueue.worker import Worker

# 取消时可直接落库的“排队中”状态（BLOCKED 尚未占用任何执行资源）
_QUEUED_STATES = (TaskState.PENDING, TaskState.RETRY_WAIT, TaskState.BLOCKED)


class TaskService:
    def __init__(
        self,
        store: TaskStore,
        registry: HandlerRegistry,
        retry_policy: RetryPolicy,
        config: TaskQueueConfig,
        logger: logging.Logger,
    ) -> None:
        self.store = store
        self.registry = registry
        self.retry_policy = retry_policy
        self.config = config
        self.logger = logger
        self.worker = Worker(store, registry, retry_policy, config, logger)
        self._started = False

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        if self._started:
            return
        await run_db(self.store.connect)
        await self.worker.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self.worker.stop()
        # 必须等 worker 全部 DB 结算结束后，才在同一 actor 线程关闭连接
        await run_db(self.store.close)
        self._started = False

    # ---------- 提交 ----------

    def _check_common_params(
        self,
        task_type: str,
        payload: dict[str, Any] | None,
        priority: int,
        max_attempts: int | None,
        timeout: float | None,
    ) -> None:
        if not isinstance(task_type, str) or not task_type:
            raise ValidationError("task_type must be a non-empty string")
        if payload is not None and not isinstance(payload, dict):
            raise ValidationError("payload must be an object/dict")
        if not isinstance(priority, int) or isinstance(priority, bool):
            raise ValidationError("priority must be an integer")
        if max_attempts is not None and (
            not isinstance(max_attempts, int) or max_attempts < 1
        ):
            raise ValidationError("max_attempts must be a positive integer")
        if timeout is not None and (not isinstance(timeout, (int, float))
                                    or timeout <= 0):
            raise ValidationError("timeout must be a positive number")
        if task_type not in self.registry:
            raise ValidationError(f"unknown task_type: {task_type}")

    def _attempts_cap(self, max_attempts: int | None) -> int:
        return max_attempts if max_attempts is not None \
            else self.config.retry.max_attempts

    def _task_timeout(self, timeout: float | None) -> float:
        return float(timeout if timeout is not None
                     else self.config.default_task_timeout)

    async def submit(
        self,
        task_type: str,
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        priority: int = 0,
        max_attempts: int | None = None,
        timeout: float | None = None,
    ) -> Task:
        # --- 参数校验，失败原因可区分 ---
        self._check_common_params(task_type, payload, priority,
                                  max_attempts, timeout)

        payload = payload or {}
        attempts_cap = self._attempts_cap(max_attempts)
        task_timeout = self._task_timeout(timeout)

        # --- 幂等处理：同键先查既有任务 ---
        if idempotency_key is not None:
            existing = await self._thread(
                self.store.find_by_idempotency_key, idempotency_key
            )
            if existing is not None:
                same = (
                    existing.task_type == task_type
                    and existing.payload == payload
                )
                if not same:
                    raise IdempotencyConflictError(existing.id)
                self.logger.info(
                    "idempotent replay: returning existing task",
                    extra={"task_id": existing.id, "attempts": existing.attempts,
                           "decision": "IDEMPOTENT_REPLAY"},
                )
                return existing

        now = time.time()
        task = Task(
            id=uuid.uuid4().hex,
            task_type=task_type,
            payload=payload,
            idempotency_key=idempotency_key,
            state=TaskState.PENDING,
            priority=priority,
            attempts=0,
            max_attempts=attempts_cap,
            timeout=task_timeout,
            not_before=now,
            created_at=now,
            updated_at=now,
        )
        try:
            inserted = await self._thread(self.store.insert_task, task)
        except IdempotencyKeyTaken:
            # 与并发提交撞键：以数据库中既有任务为准（同参视为重放）
            existing = await self._thread(
                self.store.find_by_idempotency_key, idempotency_key
            )
            assert existing is not None
            if existing.task_type != task_type or existing.payload != payload:
                raise IdempotencyConflictError(existing.id) from None
            self.logger.info(
                "idempotent replay after concurrent insert",
                extra={"task_id": existing.id, "attempts": existing.attempts,
                       "decision": "IDEMPOTENT_REPLAY_RACE"},
            )
            return existing

        self.logger.info(
            "task accepted",
            extra={"task_id": inserted.id, "attempts": 0,
                   "decision": f"ACCEPTED priority={priority}"},
        )
        return inserted

    # ---------- 流程提交（带依赖编排） ----------

    async def submit_flow(self, specs: list[FlowTaskSpec]) -> FlowStatus:
        """一次提交一组任务及其依赖关系。

        - 无依赖的任务立即进入 PENDING；有依赖的进入 BLOCKED，
          不可被调度领取，也不占并发名额；
        - 全部任务与依赖边在一个事务内落库，要么全成要么全不成；
        - 校验失败（空流程/key 重复/依赖未知 key/成环/类型未注册）
          抛 ValidationError，不产生任何数据。
        """
        if not specs:
            raise ValidationError("flow must contain at least one task")

        keys: set[str] = set()
        deps_by_key: dict[str, list[str]] = {}
        for spec in specs:
            if not isinstance(spec.key, str) or not spec.key:
                raise ValidationError("flow task key must be a non-empty string")
            if spec.key in keys:
                raise ValidationError(f"duplicate flow task key: {spec.key}")
            keys.add(spec.key)
            self._check_common_params(
                spec.task_type, spec.payload, spec.priority,
                spec.max_attempts, spec.timeout,
            )
            deps = list(spec.depends_on or [])
            if len(set(deps)) != len(deps):
                raise ValidationError(
                    f"duplicate dependencies in flow task: {spec.key}")
            if spec.key in deps:
                raise ValidationError(
                    f"flow task cannot depend on itself: {spec.key}")
            deps_by_key[spec.key] = deps

        for spec in specs:
            for dep in deps_by_key[spec.key]:
                if dep not in keys:
                    raise ValidationError(
                        f"flow task {spec.key} depends on unknown key: {dep}")

        self._check_acyclic(deps_by_key)

        now = time.time()
        flow_id = uuid.uuid4().hex
        key_to_id = {key: uuid.uuid4().hex for key in keys}

        tasks: list[Task] = []
        edges: list[tuple[str, str]] = []
        for spec in specs:
            deps = deps_by_key[spec.key]
            tasks.append(Task(
                id=key_to_id[spec.key],
                task_type=spec.task_type,
                payload=spec.payload or {},
                idempotency_key=spec.idempotency_key,
                state=TaskState.BLOCKED if deps else TaskState.PENDING,
                priority=spec.priority,
                attempts=0,
                max_attempts=self._attempts_cap(spec.max_attempts),
                timeout=self._task_timeout(spec.timeout),
                not_before=now,
                created_at=now,
                updated_at=now,
                flow_id=flow_id,
                flow_key=spec.key,
            ))
            for dep in deps:
                edges.append((key_to_id[spec.key], key_to_id[dep]))

        try:
            await self._thread(self.store.insert_flow, tasks, edges)
        except IdempotencyKeyTaken as exc:
            raise IdempotencyConflictError(
                exc.idempotency_key or "<unknown>") from exc

        self.logger.info(
            "flow accepted",
            extra={"task_id": "-", "attempts": "-",
                   "decision": f"FLOW_ACCEPTED flow={flow_id} "
                               f"tasks={len(tasks)} edges={len(edges)}"},
        )
        return FlowStatus(flow_id=flow_id, state=FlowState.RUNNING, tasks=tasks)

    @staticmethod
    def _check_acyclic(deps_by_key: dict[str, list[str]]) -> None:
        """DFS 检测依赖环；发现环抛 ValidationError。"""
        done: set[str] = set()
        visiting: set[str] = set()

        def visit(key: str) -> None:
            if key in done:
                return
            if key in visiting:
                raise ValidationError(
                    f"dependency cycle detected involving: {key}")
            visiting.add(key)
            for up in deps_by_key[key]:
                visit(up)
            visiting.discard(key)
            done.add(key)

        for key in deps_by_key:
            visit(key)

    # ---------- 查询 ----------

    async def get_task(self, task_id: str) -> Task:
        task = await self._thread(self.store.get_task, task_id)
        if task is None:
            raise UnknownTaskError(task_id)
        return task

    async def get_by_idempotency_key(self, key: str) -> Task:
        task = await self._thread(self.store.find_by_idempotency_key, key)
        if task is None:
            raise UnknownTaskError(key)
        return task

    async def get_flow(self, flow_id: str) -> FlowStatus:
        """查询整条流程的整体状态与每个成员任务的明细。"""
        tasks = await self._thread(self.store.get_flow_tasks, flow_id)
        if not tasks:
            raise UnknownFlowError(flow_id)
        return FlowStatus(
            flow_id=flow_id, state=self._derive_flow_state(tasks), tasks=tasks
        )

    @staticmethod
    def _derive_flow_state(tasks: list[Task]) -> FlowState:
        states = {t.state for t in tasks}
        # 还有任务未终结：流程仍在推进，不能提前算成结束
        if states - TERMINAL_STATES:
            return FlowState.RUNNING
        if states == {TaskState.SUCCEEDED}:
            return FlowState.SUCCEEDED
        if states & {TaskState.FAILED, TaskState.DEAD_LETTER}:
            return FlowState.FAILED
        if TaskState.CANCELLED in states:
            return FlowState.CANCELLED
        # 全部 SKIPPED 却不能溯源到失败/取消上游的情形不应出现；
        # 保守归为 FAILED，绝不把未执行的流程算成成功
        return FlowState.FAILED

    async def _propagate(self, task_id: str) -> None:
        """任务进入终态后带动下游（放行或带停），并记录判定日志。"""
        decisions = await self._thread(
            self.store.propagate_from, task_id, time.time()
        )
        for tid, decision in decisions:
            self.logger.info(
                "flow dependency propagated",
                extra={"task_id": tid, "attempts": "-",
                       "decision": f"FLOW_{decision}"},
            )

    # ---------- 取消 ----------

    async def cancel(self, task_id: str) -> Task:
        task = await self.get_task(task_id)  # 未知 -> UnknownTaskError

        if task.state in TERMINAL_STATES:
            raise InvalidStateTransitionError(task_id, task.state.value, "cancel")

        now = time.time()
        if task.state in _QUEUED_STATES:
            # 排队中：原子置为 CANCELLED（带状态守卫，防止恰被领取）
            ok = await self._thread(
                self.store.update_state,
                task_id, _QUEUED_STATES, TaskState.CANCELLED, now,
                last_error="cancelled before execution",
                error_kind="CANCELLED",
            )
            if not ok:
                # 与调度器竞争失败：任务可能已进入 RUNNING，刷新后改走运行中取消
                task = await self.get_task(task_id)
                if task.state in TERMINAL_STATES:
                    raise InvalidStateTransitionError(
                        task_id, task.state.value, "cancel"
                    )
            else:
                self.logger.info(
                    "queued task cancelled",
                    extra={"task_id": task_id, "attempts": task.attempts,
                           "decision": "CANCELLED"},
                )
                # 取消成功即进入终态：带动其下游（带停）
                await self._propagate(task_id)
                return await self.get_task(task_id)

        # RUNNING：通知 worker 取消，由执行协程落 CANCELLED
        if self.worker.request_cancel_running(task_id):
            self.logger.info(
                "cancel requested for running task",
                extra={"task_id": task_id, "attempts": task.attempts,
                       "decision": "CANCEL_REQUESTED"},
            )
            return await self.get_task(task_id)

        # 兜底：状态非终态但 worker 未跟踪（极端竞态），直接尝试落库
        raise InvalidStateTransitionError(task_id, task.state.value, "cancel")

    # ---------- 工具 ----------

    @staticmethod
    async def _thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return await run_db(func, *args, **kwargs)
