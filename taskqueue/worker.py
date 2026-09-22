"""Scheduler + worker pool.

* Up to ``concurrency`` tasks run at once; a slow task occupies only its own
  slot and never blocks dispatch of the others.
* Tasks are claimed atomically (:meth:`TaskStore.claim_next`) ordered by
  priority then due time, so concurrent workers/processes never run the same
  task twice.
* Handlers run inside ``asyncio.wait_for``; timeout or cancellation cancels
  the handler coroutine immediately and releases the slot.
* Failures are retried with exponential backoff; exhausted attempts land in
  DEAD_LETTER. Every decision is logged with task id, attempt count and the
  reason it was based on.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING

from .errors import InvalidStateTransition, UnknownTaskType
from .models import RetryPolicy, TaskState
from .store import StoreClosed

if TYPE_CHECKING:
    from .registry import TaskRegistry
    from .store import TaskStore

logger = logging.getLogger("taskqueue.worker")


class TaskWorker:
    def __init__(
        self,
        store: TaskStore,
        registry: TaskRegistry,
        *,
        concurrency: int = 4,
        default_timeout: float = 30.0,
        retry_policy: RetryPolicy | None = None,
        poll_interval: float = 0.05,
        shutdown_grace: float = 2.0,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.store = store
        self.registry = registry
        self.concurrency = concurrency
        self.default_timeout = default_timeout
        self.retry_policy = retry_policy or RetryPolicy()
        self.poll_interval = poll_interval
        self.shutdown_grace = shutdown_grace

        self.run_token = f"{uuid.uuid4().hex[:12]}"
        self._wakeup = asyncio.Event()
        self._dispatcher: asyncio.Task[None] | None = None
        self._running: set[asyncio.Task[None]] = set()
        self._stopping = asyncio.Event()

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> dict[str, int]:
        summary = await self.store.recover_interrupted()
        if any(summary.values()):
            logger.info(
                "startup recovery complete run_token=%s rescheduled=%d dead_letter=%d cancelled=%d",
                self.run_token,
                summary["rescheduled"],
                summary["dead_letter"],
                summary["cancelled"],
            )
        self._dispatcher = asyncio.create_task(self._dispatch_loop(), name="tq-dispatch")
        return summary

    async def stop(self) -> None:
        """Stop claiming new work and release in-flight handlers.

        In-flight tasks are *not* marked succeeded or failed: they stay
        RUNNING on disk and are reconciled (retry/dead-letter) on the next
        startup, exactly like a crashed process.
        """
        self._stopping.set()
        self._wakeup.set()

        # Cancel in-flight handlers first and let the execution tasks unwind
        # (each one wakes the dispatcher in its finally). Their CancelledError
        # path performs no sqlite writes, so closing afterwards is safe.
        for task in list(self._running):
            task.cancel()
        if self._running:
            await asyncio.wait(list(self._running), timeout=self.shutdown_grace)
        self._running.clear()

        # Only then stop the dispatch loop. If it happens to be parked inside
        # a (uncancellable) sqlite thread call, give that call time to finish
        # before cancelling; TaskStore.close() additionally drains the store
        # lock so the connection can never be closed mid-statement.
        if self._dispatcher is not None:
            await asyncio.sleep(0)
            self._dispatcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dispatcher

    def wake(self) -> None:
        """Hint the dispatcher that new work / state changes may exist."""
        self._wakeup.set()

    @property
    def active_count(self) -> int:
        return len(self._running)

    # -------------------------------------------------------------- dispatch
    async def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                while len(self._running) < self.concurrency:
                    rec = await self.store.claim_next(self.run_token)
                    if rec is None:
                        break
                    task = asyncio.create_task(self._execute(rec), name=f"tq-{rec.id}")
                    self._running.add(task)
                    task.add_done_callback(self._running.discard)

                # Sleep until a wakeup, a retry becomes due, or the poll interval.
                delay = self.poll_interval
                next_due = await self.store.next_run_after()
                if next_due is not None:
                    delay = max(0.01, min(delay, next_due - time.time()))
                if len(self._running) >= self.concurrency:
                    delay = self.poll_interval
            except StoreClosed:
                # Shutdown raced with dispatch; nothing more to do.
                return

            try:
                if self._stopping.is_set():
                    break
                await asyncio.wait_for(self._wakeup.wait(), timeout=max(delay, 0.01))
                self._wakeup.clear()
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                if self._stopping.is_set():
                    raise
                raise

    # ------------------------------------------------------------ execution
    async def _execute(self, rec) -> None:  # rec: TaskRecord
        task_id = rec.id
        try:
            # Cancel may have been requested between claim and execution.
            if rec.cancel_requested:
                await self._safe_cancel(task_id, "cancel flag present at dispatch time")
                return

            try:
                handler = self.registry.get(rec.task_type)
            except UnknownTaskType as exc:  # unknown task type -> dead letter
                error = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "task=%s attempt=%d/%d dead-letter basis=no_handler reason=%s",
                    task_id, rec.attempts, rec.max_attempts, error,
                )
                await self.store.mark_dead_letter(task_id, error)
                return

            payload = json.loads(rec.payload) if rec.payload else {}
            logger.info(
                "task=%s attempt=%d/%d state=RUNNING type=%s priority=%d",
                task_id, rec.attempts, rec.max_attempts, rec.task_type, rec.priority,
            )

            result = await self._run_handler(task_id, handler, payload)
            result_json = json.dumps(result, ensure_ascii=False, default=str)

            latest = await self.store.get(task_id)
            if latest.cancel_requested or latest.state != TaskState.RUNNING:
                await self._safe_cancel(task_id, "cancel observed after handler returned")
                return

            await self.store.mark_succeeded(task_id, result_json)
            logger.info(
                "task=%s attempt=%d/%d state=SUCCEEDED basis=handler_returned",
                task_id, rec.attempts, rec.max_attempts,
            )
        except asyncio.CancelledError:
            # Dispatcher/process shutdown: leave state RUNNING for recovery.
            logger.warning(
                "task=%s attempt=%d/%d interrupted basis=cancellation",
                task_id, rec.attempts, rec.max_attempts,
            )
            raise
        except StoreClosed:
            # The store was closed during shutdown; leave RUNNING on disk so
            # the next startup's recovery reconciles this attempt.
            logger.warning(
                "task=%s attempt=%d/%d interrupted basis=store_closed",
                task_id, rec.attempts, rec.max_attempts,
            )
        except Exception as exc:  # noqa: BLE001 - every failure gets a decision
            try:
                await self._handle_failure(rec, exc)
            except StoreClosed:
                logger.warning(
                    "task=%s attempt=%d/%d interrupted basis=store_closed "
                    "reason=%s",
                    task_id, rec.attempts, rec.max_attempts, exc,
                )
        finally:
            self.wake()

    async def _run_handler(self, task_id, handler, payload) -> object:
        handler_task = asyncio.create_task(handler(payload))
        poller = asyncio.create_task(self._cancel_watcher(task_id, handler_task))
        try:
            return await asyncio.wait_for(handler_task, timeout=self.default_timeout)
        finally:
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller

    async def _cancel_watcher(
        self, task_id: str, handler_task: asyncio.Task[object]
    ) -> None:
        """Poll the persisted cancel flag and cancel the handler cooperatively."""
        while not handler_task.done():
            try:
                if await self.store.is_cancel_requested(task_id):
                    handler_task.cancel()
                    return
            except InvalidStateTransition:
                return
            except StoreClosed:
                return
            await asyncio.sleep(min(self.poll_interval, 0.1))

    async def _handle_failure(self, rec, exc: BaseException) -> None:
        task_id = rec.id
        attempts = rec.attempts
        error = f"{type(exc).__name__}: {exc}"

        # Re-read in case cancel/timeout raced with an explicit cancel request.
        latest = await self.store.get(task_id)
        cancelled = latest.cancel_requested or latest.state != TaskState.RUNNING
        if cancelled:
            await self._safe_cancel(task_id, f"cancel observed after failure: {error}")
            return

        # Decision uses the task's own max_attempts (configurable per submit);
        # the delay shape comes from the worker-level RetryPolicy.
        if attempts < rec.max_attempts:
            delay = self.retry_policy.backoff_delay(attempts)
            run_after = time.time() + delay
            await self.store.mark_retrying(task_id, error, run_after)
            logger.warning(
                "task=%s attempt=%d/%d state=SCHEDULED retry_in=%.3fs "
                "basis=retryable_failure reason=%s",
                task_id, attempts, rec.max_attempts, delay, error,
            )
        else:
            reason = f"{error} (attempts exhausted: {attempts}/{rec.max_attempts})"
            await self.store.mark_dead_letter(task_id, reason)
            logger.error(
                "task=%s attempt=%d/%d state=DEAD_LETTER basis=attempts_exhausted reason=%s",
                task_id, attempts, rec.max_attempts, error,
            )

    async def _safe_cancel(self, task_id: str, reason: str) -> None:
        try:
            await self.store.mark_cancelled(task_id)
            logger.info("task=%s state=CANCELLED basis=%s", task_id, reason)
        except InvalidStateTransition:
            logger.info(
                "task=%s cancel ignored basis=already_terminal reason=%s",
                task_id, reason,
            )
