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
    UnknownTaskError,
    ValidationError,
)
from taskqueue.models import TERMINAL_STATES, Task, TaskState
from taskqueue.registry import HandlerRegistry
from taskqueue.retry import RetryPolicy
from taskqueue.store import IdempotencyKeyTaken, TaskStore
from taskqueue.worker import Worker

# 取消时可直接落库的“排队中”状态
_QUEUED_STATES = (TaskState.PENDING, TaskState.RETRY_WAIT)


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
