"""任务服务：对外协调层。

- submit: 参数校验 + 处理器类型校验 + 幂等去重（同参重放返回既有任务）；
- get_task / get_by_idempotency_key: 查询，未知任务抛 UnknownTaskError；
- cancel: 仅允许取消非终态任务；PENDING/RETRY_WAIT 直接落 CANCELLED，
  RUNNING 则通知 worker 协作式取消；
- start/stop: 管理存储连接与 worker 生命周期。
"""
from __future__ import annotations

import logging
import sqlite3
import time
import uuid
from typing import Any

from taskqueue.config import TaskQueueConfig
from taskqueue.dbutil import run_db
from taskqueue.errors import (
    IdempotencyConflictError,
    InvalidStateTransitionError,
    UnknownTaskError,
    UnknownWorkflowError,
    ValidationError,
)
from taskqueue.models import (
    TERMINAL_STATES,
    WORKFLOW_FAILED,
    WORKFLOW_RUNNING,
    WORKFLOW_SUCCEEDED,
    Task,
    TaskState,
    Workflow,
    WorkflowTaskView,
    WorkflowView,
)
from taskqueue.registry import HandlerRegistry
from taskqueue.retry import RetryPolicy
from taskqueue.store import IdempotencyKeyTaken, TaskStore
from taskqueue.worker import Worker
from taskqueue.workflow import WorkflowSpec, validate_workflow_spec

# 取消时可直接落库的“排队中/阻塞中”状态
_QUEUED_STATES = (
    TaskState.PENDING,
    TaskState.RETRY_WAIT,
    TaskState.BLOCKED,
)


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

    def _validate_task_params(
        self,
        task_type: str,
        payload: dict[str, Any] | None,
        priority: int,
        max_attempts: int | None,
        timeout: float | None,
    ) -> tuple[dict[str, Any], int, float]:
        """校验并归一化单任务参数，返回 (payload, max_attempts, timeout)。"""
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

        payload = payload or {}
        attempts_cap = max_attempts if max_attempts is not None \
            else self.config.retry.max_attempts
        task_timeout = float(timeout if timeout is not None
                             else self.config.default_task_timeout)
        return payload, attempts_cap, task_timeout

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
        payload, attempts_cap, task_timeout = self._validate_task_params(
            task_type, payload, priority, max_attempts, timeout
        )

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

    # ---------- 流程提交 / 进度 ----------

    async def submit_workflow(self, spec: WorkflowSpec) -> WorkflowView:
        """一次提交一组任务及其依赖，返回流程初始进度视图。

        - 无依赖任务以 PENDING 落库，可立即被调度；
        - 有依赖任务以 BLOCKED 落库，不参与领取、不占并发名额；
        - 结构性错误（重复 key / 未知依赖 / 环）与未知处理器类型一律拒绝；
        - 任一任务幂等键与库中既有任务冲突，整批回滚（不落任何任务）。
        """
        validate_workflow_spec(spec)
        if spec.name is not None and (
            not isinstance(spec.name, str) or not spec.name
        ):
            raise ValidationError("workflow name must be a non-empty string")

        now = time.time()
        workflow_id = uuid.uuid4().hex
        tasks: list[Task] = []
        dependencies: list[tuple[str, str]] = []
        idem_keys: set[str] = set()

        for item in spec.tasks:
            payload, attempts_cap, task_timeout = self._validate_task_params(
                item.task_type, item.payload, item.priority,
                item.max_attempts, item.timeout,
            )
            if item.idempotency_key is not None:
                if item.idempotency_key in idem_keys:
                    raise ValidationError(
                        f"duplicate idempotency_key within workflow: "
                        f"{item.idempotency_key}"
                    )
                idem_keys.add(item.idempotency_key)

            has_deps = len(item.depends_on) > 0
            tasks.append(Task(
                id=uuid.uuid4().hex,
                task_type=item.task_type,
                payload=payload,
                idempotency_key=item.idempotency_key,
                state=TaskState.BLOCKED if has_deps else TaskState.PENDING,
                priority=item.priority,
                attempts=0,
                max_attempts=attempts_cap,
                timeout=task_timeout,
                not_before=now,
                created_at=now,
                updated_at=now,
                workflow_id=workflow_id,
                task_key=item.key,
            ))
            for dep in item.depends_on:
                dependencies.append((item.key, dep))

        workflow = Workflow(id=workflow_id, name=spec.name, created_at=now)
        try:
            await self._thread(
                self.store.insert_workflow, workflow, tasks, dependencies
            )
        except sqlite3.IntegrityError:
            # 主要是幂等键与库中既有任务冲突：整批回滚，按冲突拒绝
            raise IdempotencyConflictError(
                next((t.id for t in tasks), workflow_id)
            ) from None

        self.logger.info(
            "workflow accepted",
            extra={"task_id": workflow_id, "attempts": len(tasks),
                   "decision": f"WORKFLOW_ACCEPTED tasks={len(tasks)} "
                               f"edges={len(dependencies)}"},
        )
        return await self.get_workflow(workflow_id)

    async def get_workflow(self, workflow_id: str) -> WorkflowView:
        workflow = await self._thread(self.store.get_workflow, workflow_id)
        if workflow is None:
            raise UnknownWorkflowError(workflow_id)
        tasks = await self._thread(self.store.list_workflow_tasks, workflow_id)
        deps = await self._thread(
            self.store.list_workflow_dependencies, workflow_id
        )

        depends_map: dict[str, list[str]] = {}
        for task_key, dep_key in deps:
            depends_map.setdefault(task_key, []).append(dep_key)

        key_by_id = {t.id: t.task_key for t in tasks}
        views: list[WorkflowTaskView] = []
        state_counts: dict[str, int] = {}
        for task in tasks:
            state_counts[task.state.value] = \
                state_counts.get(task.state.value, 0) + 1
            skipped_by_key = (
                key_by_id.get(task.skipped_by_task_id)
                if task.skipped_by_task_id is not None
                else None
            )
            views.append(WorkflowTaskView(
                task=task,
                depends_on=depends_map.get(task.task_key or "", []),
                skipped_by_key=skipped_by_key,
            ))

        states = {t.state for t in tasks}
        if states <= TERMINAL_STATES:
            if states <= {TaskState.SUCCEEDED}:
                wf_state = WORKFLOW_SUCCEEDED
            else:
                wf_state = WORKFLOW_FAILED
        else:
            wf_state = WORKFLOW_RUNNING

        return WorkflowView(
            workflow=workflow,
            tasks=views,
            state=wf_state,
            state_counts=state_counts,
        )

    # ---------- 取消 ----------

    async def cancel(self, task_id: str) -> Task:
        task = await self.get_task(task_id)  # 未知 -> UnknownTaskError

        if task.state in TERMINAL_STATES:
            raise InvalidStateTransitionError(task_id, task.state.value, "cancel")

        now = time.time()
        if task.state in _QUEUED_STATES:
            # 排队中/阻塞中：原子置为 CANCELLED（带状态守卫，防止恰被领取）；
            # 若属于某条流程，在同一事务内把未开始的下游级联带停为 SKIPPED。
            settlement = await self._thread(
                self.store.settle_terminal,
                task_id, _QUEUED_STATES, TaskState.CANCELLED, now,
                last_error="cancelled before execution",
                error_kind="CANCELLED",
            )
            if not settlement.changed:
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
                           "decision": f"CANCELLED skipped="
                                       f"{len(settlement.skipped)}"},
                )
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
