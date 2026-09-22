"""Public facade: submit / query / cancel over a persistent store and worker."""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from .errors import InvalidStateTransition
from .models import RetryPolicy, TaskRecord, TaskState
from .registry import TaskRegistry
from .store import TaskStore
from .worker import TaskWorker


class TaskService:
    def __init__(
        self,
        db_path: str,
        *,
        concurrency: int = 4,
        default_timeout: float = 30.0,
        retry_policy: RetryPolicy | None = None,
        poll_interval: float = 0.05,
    ) -> None:
        self.db_path = db_path
        self.store = TaskStore(db_path)
        self.registry = TaskRegistry()
        self.worker = TaskWorker(
            self.store,
            self.registry,
            concurrency=concurrency,
            default_timeout=default_timeout,
            retry_policy=retry_policy,
            poll_interval=poll_interval,
        )

    async def start(self) -> dict[str, int]:
        await self.store.init()
        return await self.worker.start()

    async def close(self) -> None:
        await self.worker.stop()
        await self.store.close()

    # ------------------------------------------------------------ handlers
    def register_handler(self, name: str, handler: Any) -> None:
        self.registry.register(name, handler)

    # ------------------------------------------------------------ commands
    async def submit(
        self,
        task_type: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        priority: int = 0,
        max_attempts: int = 1,
    ) -> tuple[TaskRecord, bool]:
        """Submit a task.

        Returns ``(record, created)``. A repeated idempotency key returns the
        existing record with ``created=False``; it never causes a second
        execution.
        """
        # Validate task type eagerly so callers get 422 instead of a dead
        # letter for a simple typo.
        self.registry.get(task_type)
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        if idempotency_key is not None:
            existing = await self.store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing, False

        payload_json = json.dumps(payload or {}, ensure_ascii=False, default=str)
        rec = TaskRecord.new(
            uuid.uuid4().hex,
            task_type,
            payload_json,
            idempotency_key=idempotency_key,
            priority=priority,
            max_attempts=max_attempts,
        )
        try:
            await self.store.insert(rec)
        except sqlite3.IntegrityError:
            # Concurrent submit with the same idempotency key: first writer
            # wins, everyone else gets the same task back.
            if idempotency_key is None:
                raise
            existing = await self.store.find_by_idempotency_key(idempotency_key)
            assert existing is not None
            return existing, False
        self.worker.wake()
        return rec, True

    async def get_task(self, task_id: str) -> TaskRecord:
        return await self.store.get(task_id)

    async def find_by_idempotency_key(self, key: str) -> TaskRecord | None:
        return await self.store.find_by_idempotency_key(key)

    async def cancel(self, task_id: str) -> TaskRecord:
        """Request cancellation; illegal for unknown/terminal tasks."""
        rec = await self.store.get(task_id)  # raises TaskNotFound if unknown
        if not rec.state.can_cancel:
            raise InvalidStateTransition(
                f"cannot cancel task {task_id}: state {rec.state.value} is terminal",
                task_id=task_id,
                from_state=rec.state.value,
                to_state=TaskState.CANCELLED.value,
            )
        rec = await self.store.request_cancel(task_id)
        self.worker.wake()
        # Queued tasks: make the cancellation immediately visible as a state
        # change where possible. RUNNING tasks finish via the cancel watcher.
        if rec.state in (TaskState.PENDING, TaskState.SCHEDULED):
            try:
                rec = await self.store.mark_cancelled(task_id)
            except InvalidStateTransition:
                # The task was claimed between flag-set and here; the persisted
                # cancel flag will interrupt the handler via the watcher.
                rec = await self.store.get(task_id)
        return rec

    async def list_tasks(
        self,
        *,
        state: TaskState | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[TaskRecord]:
        return await self.store.list_tasks(state=state, limit=limit, offset=offset)

    async def list_dead_letters(self, *, limit: int = 100) -> list[TaskRecord]:
        return await self.store.list_tasks(state=TaskState.DEAD_LETTER, limit=limit)

    async def stats(self) -> dict[str, int]:
        return await self.store.count_by_state()
