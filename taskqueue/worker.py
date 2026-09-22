"""异步执行工作器。

职责:
- 轮询到期任务，按优先级领取；用 asyncio.Semaphore 限制并发执行数；
- 慢任务/长任务在独立 task 中运行，不阻塞调度循环与其他任务；
- 每条任务独立超时（asyncio.wait_for），超时与取消后释放信号量；
- 根据异常类别与尝试次数决定: 成功 / 退避重试 / 不可重试失败 / 死信；
- 关键判定全部落日志，带 task_id、attempts、decision。
"""
from __future__ import annotations

import asyncio
import logging
import time

from taskqueue.config import TaskQueueConfig
from taskqueue.dbutil import run_db
from taskqueue.errors import (
    ERROR_KIND_CANCELLED,
    ERROR_KIND_NON_RETRYABLE,
    ERROR_KIND_RETRYABLE,
    ERROR_KIND_TIMEOUT,
    ERROR_KIND_UNKNOWN_TYPE,
    NonRetryableTaskError,
    RetryableTaskError,
    TaskExecutionError,
)
from taskqueue.models import Task, TaskState
from taskqueue.registry import ExecutionContext, HandlerNotFoundError, HandlerRegistry
from taskqueue.retry import RetryPolicy
from taskqueue.store import TaskStore, new_owner_id

# 租约时长取“默认超时”与一个下限中的较大者，并留余量
_LEASE_MARGIN = 5.0


class Worker:
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
        self.owner_id = new_owner_id()

        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        # 运行中任务的取消事件，供 cancel 主动触发
        self._cancel_events: dict[str, asyncio.Event] = {}
        # 所有“外层执行任务”（含阻塞在信号量上、尚未开始执行的）都必须被跟踪，
        # 停机时等待它们自然结算结束后再关闭数据库，杜绝 use-after-free。
        self._runners: set[asyncio.Task[None]] = set()
        # 真正在跑处理器的内层任务
        self._inflight: set[asyncio.Task[None]] = set()
        self._stop = asyncio.Event()
        self._poll_task: asyncio.Task[None] | None = None
        self._running_count = 0

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        # 启动恢复：上次进程遗留的 RUNNING 任务退回 PENDING，按重试策略重新判定
        recovered = await run_db(self.store.recover_orphans, time.time())
        for tid in recovered:
            self.logger.warning(
                "recovered orphaned running task after restart",
                extra={"task_id": tid, "attempts": "-", "decision": "RECOVER"},
            )
        self._stop.clear()
        self._poll_task = asyncio.create_task(self._poll_loop(), name="worker-poll")
        self.logger.info(
            "worker started",
            extra={"task_id": "-", "attempts": "-", "decision": "START"},
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        # 通知所有已创建取消事件的任务尽快退出；正在执行的任务会正常结算落库，
        # 阻塞在信号量上的任务获得额度后会被直接结算为 CANCELLED。
        for ev in self._cancel_events.values():
            ev.set()

        # 等待全部 runner 自然结束（不要 cancel 它们：否则可能在 SQLite 落库
        # 中途被打断，且连接随后被关闭导致 use-after-free）。给一个宽限上限。
        if self._runners:
            await asyncio.wait_for(
                asyncio.gather(*self._runners, return_exceptions=True),
                timeout=self.config.shutdown_grace,
            )
        self.logger.info(
            "worker stopped",
            extra={"task_id": "-", "attempts": "-", "decision": "STOP"},
        )

    def request_cancel_running(self, task_id: str) -> bool:
        """请求取消一个正在执行的任务。返回该任务当前是否在执行。"""
        ev = self._cancel_events.get(task_id)
        if ev is None:
            return False
        ev.set()
        return True

    def is_running(self, task_id: str) -> bool:
        return task_id in self._cancel_events

    # ---------- 调度循环 ----------

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            scheduled = False
            # 只要还有并发额度且有到期任务，就立即再领，避免空等一个 poll 间隔
            if self._running_count < self.config.max_concurrency:
                task = await run_db(
                    self.store.claim_next_due,
                    self.owner_id,
                    time.time(),
                    max(self.config.default_task_timeout, 1.0) + _LEASE_MARGIN,
                )
                if task is not None:
                    scheduled = True
                    runner = asyncio.create_task(
                        self._guarded_run(task), name=f"run-{task.id}"
                    )
                    self._runners.add(runner)
                    runner.add_done_callback(self._runners.discard)
            if not scheduled:
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.config.poll_interval
                    )
                except (TimeoutError, asyncio.CancelledError):
                    pass

    async def _guarded_run(self, task: Task) -> None:
        """获取信号量并执行，确保任何情况下信号量、计数与连接都安全。

        取消事件在获取信号量之前创建，因此即便任务已被领取（DB 为 RUNNING）
        但仍在排队等额度，停机/取消也能通知到它。
        """
        cancel_event = asyncio.Event()
        self._cancel_events[task.id] = cancel_event
        try:
            await self._semaphore.acquire()
            try:
                self._running_count += 1
                # 停机期间才获得额度：不再执行处理器，直接结算 CANCELLED，
                # 避免在关闭数据库前后访问它。
                if self._stop.is_set() or cancel_event.is_set():
                    await self._settle(
                        task,
                        ERROR_KIND_CANCELLED,
                        "cancelled before execution started",
                        cancelled=True,
                    )
                    return

                async_job = asyncio.create_task(
                    self._execute(task, cancel_event)
                )
                self._inflight.add(async_job)
                try:
                    await async_job
                except asyncio.CancelledError:
                    # 优雅停机导致的取消：保持 RUNNING，重启后按孤儿恢复
                    raise
                except Exception:
                    self.logger.exception(
                        "unexpected worker crash",
                        extra={"task_id": task.id, "attempts": task.attempts,
                               "decision": "WORKER_ERROR"},
                    )
                finally:
                    self._inflight.discard(async_job)
            finally:
                self._running_count -= 1
                self._semaphore.release()
        finally:
            self._cancel_events.pop(task.id, None)

    # ---------- 单任务执行与判定 ----------

    async def _execute(self, task: Task, cancel_event: asyncio.Event) -> None:
        self.logger.info(
            "dispatching task",
            extra={"task_id": task.id, "attempts": task.attempts,
                   "decision": "RUN"},
        )
        result = None
        error_kind: str
        error_message: str

        try:
            handler = self.registry.get(task.task_type)
        except HandlerNotFoundError:
            error_kind = ERROR_KIND_UNKNOWN_TYPE
            error_message = f"no handler for task_type={task.task_type}"
            await self._settle(task, error_kind, error_message, retryable=False)
            return

        ctx = ExecutionContext(task_id=task.id, attempt=task.attempts,
                               cancel_event=cancel_event)
        try:
            result = await asyncio.wait_for(
                handler(task.payload, ctx), timeout=task.timeout
            )
        except asyncio.CancelledError:
            # 优雅停机导致的协程取消：保持 RUNNING，重启后按孤儿恢复
            self.logger.info(
                "task coroutine cancelled during shutdown",
                extra={"task_id": task.id, "attempts": task.attempts,
                       "decision": "SHUTDOWN"},
            )
            raise
        except TimeoutError:
            await self._settle(
                task, ERROR_KIND_TIMEOUT,
                f"task exceeded timeout of {task.timeout}s", retryable=True
            )
            return
        except NonRetryableTaskError as exc:
            await self._settle(
                task, ERROR_KIND_NON_RETRYABLE, str(exc), retryable=False
            )
            return
        except RetryableTaskError as exc:
            await self._settle(
                task, ERROR_KIND_RETRYABLE, str(exc), retryable=True
            )
            return
        except TaskExecutionError as exc:
            # 自定义 TaskExecutionError 子类按其 error_kind 判定
            retryable = exc.error_kind == ERROR_KIND_RETRYABLE
            await self._settle(task, exc.error_kind, str(exc),
                               retryable=retryable)
            return
        except Exception as exc:  # noqa: BLE001
            # 未声明的异常：保守地视为可重试（受 max_attempts 约束）
            await self._settle(
                task, ERROR_KIND_RETRYABLE, repr(exc), retryable=True
            )
            return

        # 被主动取消（在返回前检查）
        if cancel_event.is_set():
            await self._settle(
                task, ERROR_KIND_CANCELLED, "cancelled while running",
                cancelled=True,
            )
            return

        await self._mark_succeeded(task, result)

    async def _mark_succeeded(self, task: Task, result: object) -> None:
        ok = await self._update(task, TaskState.SUCCEEDED, result=result)
        if ok:
            self.logger.info(
                "task succeeded",
                extra={"task_id": task.id, "attempts": task.attempts,
                       "decision": "SUCCEEDED"},
            )
        else:
            # 状态已被并发改动（例如取消/恢复），不能静默成功
            self.logger.error(
                "task produced result but state was no longer RUNNING; "
                "result discarded to avoid silent success",
                extra={"task_id": task.id, "attempts": task.attempts,
                       "decision": "SUCCESS_REJECTED"},
            )

    async def _update(
        self, task: Task, new_state: TaskState, **fields: object
    ) -> bool:
        return await run_db(
            self.store.update_state,
            task.id,
            TaskState.RUNNING,
            new_state,
            time.time(),
            **fields,
        )

    async def _settle(
        self,
        task: Task,
        error_kind: str,
        error_message: str,
        *,
        retryable: bool = False,
        cancelled: bool = False,
    ) -> None:
        """根据类别与剩余次数做最终判定。"""
        attempts = task.attempts
        error_fields = {"last_error": error_message, "error_kind": error_kind}

        if cancelled:
            ok = await self._update(task, TaskState.CANCELLED, **error_fields)
            self._log_outcome(task, attempts, "CANCELLED", ok, error_message)
            return

        if not retryable:
            ok = await self._update(task, TaskState.FAILED, **error_fields)
            self._log_outcome(task, attempts, "FAILED_NON_RETRYABLE", ok,
                              error_message)
            return

        # 可重试：判断是否还有下一次机会
        if self.retry_policy.should_retry(attempts, task.max_attempts):
            delay = self.retry_policy.next_delay(attempts)
            not_before = time.time() + delay
            ok = await self._update(
                task, TaskState.RETRY_WAIT, not_before=not_before, **error_fields
            )
            if ok:
                self.logger.warning(
                    "task failed and scheduled for retry",
                    extra={"task_id": task.id, "attempts": attempts,
                           "decision": f"RETRY in {delay:.3f}s"},
                )
            else:
                self.logger.error(
                    "could not move task to RETRY_WAIT due to concurrent change",
                    extra={"task_id": task.id, "attempts": attempts,
                           "decision": "RETRY_REJECTED"},
                )
            return

        # 重试耗尽 -> 死信
        ok = await self._update(task, TaskState.DEAD_LETTER, **error_fields)
        self._log_outcome(task, attempts, "DEAD_LETTER", ok, error_message)

    def _log_outcome(
        self, task: Task, attempts: int, decision: str, ok: bool, message: str
    ) -> None:
        suffix = "" if ok else " (update rejected by concurrent state change)"
        self.logger.warning(
            "task settled: %s reason=%s%s",
            decision, message, suffix,
            extra={"task_id": task.id, "attempts": attempts, "decision": decision},
        )
