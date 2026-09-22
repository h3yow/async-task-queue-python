"""SQLite-backed persistent task storage.

Concurrency model
-----------------
* A single SQLite connection is shared inside the process and every public
  operation is serialized by an asyncio.Lock, so the state machine in
  :mod:`taskqueue.models` is enforced atomically even under concurrent
  submit/query/cancel calls.
* Blocking sqlite calls run in worker threads (``asyncio.to_thread``) so the
  event loop is never stalled on fsync.
* WAL journaling + ``BEGIN IMMEDIATE`` claims make the persisted state safe
  across process restarts. A task is only ever marked RUNNING by the
  ``claim_next`` transaction, and is only marked SUCCEEDED from RUNNING.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any

from .errors import InvalidStateTransition, TaskNotFound
from .models import TaskRecord, TaskState

# Interruption reason recorded when a RUNNING task survives a process restart.
RESTART_ERROR = "task interrupted by process restart before completion"
CANCEL_ERROR = "task cancelled before completion"


class StoreClosed(RuntimeError):
    """Raised when an operation reaches the lock after TaskStore.close()."""


class _ThreadResult:
    __slots__ = ("error", "future", "result")

    def __init__(self, future: Any) -> None:
        self.future = future
        self.result: list[Any] = []
        self.error: list[BaseException] = []

_COLUMNS = (
    "id",
    "idempotency_key",
    "task_type",
    "payload",
    "state",
    "priority",
    "attempts",
    "max_attempts",
    "run_after",
    "result",
    "error",
    "run_token",
    "cancel_requested",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
)


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._closed = False
        self._lock = asyncio.Lock()
        # In-flight sqlite worker-thread calls, tracked independently of the
        # asyncio lock: cancelling an awaiter releases the asyncio lock but
        # cannot stop an already-running thread. close() waits on this
        # condition until every thread has finished touching the connection.
        self._inflight = 0
        self._idle = threading.Condition()

    # ------------------------------------------------------------------ setup
    async def init(self) -> None:
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # autocommit; transactions managed explicitly
        )
        conn.row_factory = sqlite3.Row
        if self.db_path != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id               TEXT PRIMARY KEY,
                idempotency_key  TEXT UNIQUE,
                task_type        TEXT NOT NULL,
                payload          TEXT NOT NULL DEFAULT '',
                state            TEXT NOT NULL,
                priority         INTEGER NOT NULL DEFAULT 0,
                attempts         INTEGER NOT NULL DEFAULT 0,
                max_attempts     INTEGER NOT NULL DEFAULT 1,
                run_after        REAL NOT NULL DEFAULT 0,
                result           TEXT,
                error            TEXT,
                run_token        TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                created_at       REAL NOT NULL,
                updated_at       REAL NOT NULL,
                started_at       REAL,
                finished_at      REAL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dispatch "
            "ON tasks(state, run_after, priority)"
        )
        self._conn = conn

    async def close(self) -> None:
        async with self._lock:
            conn = self._conn
            if conn is None:
                return
            # Set the flag *while holding the lock*: any operation that later
            # acquires the lock sees it and bails out before touching sqlite.
            self._closed = True

        # Wait (off the asyncio lock, in a thread) for any sqlite call whose
        # awaiter was cancelled mid-flight to actually finish in its worker
        # thread. Cancelling an awaiter releases the asyncio lock but does not
        # stop the running statement, so closing immediately could free the
        # connection out from under that thread and segfault.
        await asyncio.to_thread(self._wait_idle)

        async with self._lock:
            await asyncio.to_thread(conn.close)
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None or self._closed:
            raise RuntimeError("TaskStore is not initialised (or already closed)")
        return self._conn

    async def _ensure_open_locked(self) -> None:
        """Called immediately after acquiring ``self._lock``.

        A coroutine can reach the lock *after* ``close()`` even though it was
        scheduled earlier (``asyncio.to_thread`` futures are not cancelled
        when their awaiter is). Bail out rather than touch a freed connection.
        """
        if self._closed or self._conn is None:
            raise StoreClosed("TaskStore has been closed")

    def _run(self, func: Callable[..., Any], *args: Any) -> _ThreadResult:
        """Schedule a blocking sqlite call, counting it thread-safely."""
        tr = _ThreadResult(None)

        def work() -> None:
            with self._idle:
                self._inflight += 1
            try:
                tr.result.append(func(*args))
            except BaseException as exc:  # noqa: BLE001 - re-raised on awaiter
                tr.error.append(exc)
            finally:
                with self._idle:
                    self._inflight -= 1
                    if self._inflight == 0:
                        self._idle.notify_all()

        tr.future = asyncio.to_thread(work)
        return tr

    async def _sql(self, func: Callable[..., Any], *args: Any) -> Any:
        tr = self._run(func, *args)
        # Cancellation here does NOT cancel the thread; it will finish and
        # decrement _inflight regardless. We surface the exception afterwards.
        await tr.future
        if tr.error:
            raise tr.error[0]
        return tr.result[0] if tr.result else None

    def _wait_idle(self, timeout: float = 10.0) -> bool:
        with self._idle:
            return self._idle.wait_for(lambda: self._inflight == 0, timeout)

    # -------------------------------------------------------------- mapping
    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> TaskRecord:
        data = {key: row[key] for key in _COLUMNS}
        data["state"] = TaskState(row["state"])
        data["cancel_requested"] = bool(row["cancel_requested"])
        return TaskRecord(**data)

    # ------------------------------------------------------------ basic CRUD
    async def insert(self, rec: TaskRecord) -> None:
        """Insert a new task. Caller resolves idempotency conflicts beforehand."""
        async with self._lock:
            await self._ensure_open_locked()
            await self._sql(self._insert, rec)

    def _insert(self, rec: TaskRecord) -> None:
        cancel = 1 if rec.cancel_requested else 0
        self.conn.execute(
            """
            INSERT INTO tasks (
                id, idempotency_key, task_type, payload, state, priority,
                attempts, max_attempts, run_after, result, error, run_token,
                cancel_requested, created_at, updated_at, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rec.id,
                rec.idempotency_key,
                rec.task_type,
                rec.payload,
                rec.state.value,
                rec.priority,
                rec.attempts,
                rec.max_attempts,
                rec.run_after,
                rec.result,
                rec.error,
                rec.run_token,
                cancel,
                rec.created_at,
                rec.updated_at,
                rec.started_at,
                rec.finished_at,
            ),
        )

    async def get(self, task_id: str) -> TaskRecord:
        async with self._lock:
            await self._ensure_open_locked()
            r = await self._sql(
                lambda: self.conn.execute(
                    "SELECT * FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
            )
        if r is None:
            raise TaskNotFound(f"no task with id {task_id!r}", task_id=task_id)
        return self._row_to_record(r)

    async def find_by_idempotency_key(self, key: str) -> TaskRecord | None:
        async with self._lock:
            await self._ensure_open_locked()
            r = await self._sql(
                lambda: self.conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
                ).fetchone()
            )
        return None if r is None else self._row_to_record(r)

    async def list_tasks(
        self,
        *,
        state: TaskState | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[TaskRecord]:
        sql = "SELECT * FROM tasks"
        params: list[Any] = []
        if state is not None:
            sql += " WHERE state = ?"
            params.append(state.value)
        sql += " ORDER BY created_at DESC, id ASC LIMIT ? OFFSET ?"
        params.extend((limit, offset))
        async with self._lock:
            await self._ensure_open_locked()
            rows = await self._sql(
                lambda: self.conn.execute(sql, tuple(params)).fetchall()
            )
        return [self._row_to_record(r) for r in rows]

    async def count_by_state(self) -> dict[str, int]:
        async with self._lock:
            await self._ensure_open_locked()
            rows = await self._sql(
                lambda: self.conn.execute(
                    "SELECT state, COUNT(*) AS n FROM tasks GROUP BY state"
                ).fetchall()
            )
        return {r["state"]: r["n"] for r in rows}

    # -------------------------------------------------------- guarded update
    async def _update(
        self,
        task_id: str,
        expected: Iterable[TaskState],
        changes: dict[str, Any],
        *,
        reason: str,
    ) -> TaskRecord:
        """Apply ``changes`` only while the task is in one of ``expected``.

        Returns the resulting record. Raises TaskNotFound /
        InvalidStateTransition otherwise. This is the single choke point that
        enforces the state machine.
        """
        expected_set = set(expected)
        async with self._lock:
            await self._ensure_open_locked()
            rec = await self._sql(self._get_locked, task_id)
            if rec.state not in expected_set:
                new_state = changes.get("state")
                raise InvalidStateTransition(
                    f"{reason}: task {task_id} is {rec.state.value}, "
                    f"expected one of {sorted(s.value for s in expected_set)}",
                    task_id=task_id,
                    from_state=rec.state.value,
                    to_state=new_state.value if isinstance(new_state, TaskState) else new_state,
                )

            sets: list[str] = []
            values: list[Any] = []
            for key, value in changes.items():
                if key == "cancel_requested":
                    sets.append(f"{key} = ?")
                    values.append(1 if value else 0)
                    continue
                sets.append(f"{key} = ?")
                values.append(
                    value.value if isinstance(value, TaskState) else value
                )
            values.append(task_id)
            sql = f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?"
            await self._sql(self.conn.execute, sql, tuple(values))
            return await self._sql(self._get_locked, task_id)

    def _get_locked(self, task_id: str) -> TaskRecord:
        row = self.conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise TaskNotFound(f"no task with id {task_id!r}", task_id=task_id)
        return self._row_to_record(row)

    # ------------------------------------------------------------- dispatch
    async def claim_next(self, run_token: str, *, now: float | None = None) -> TaskRecord | None:
        """Atomically take the highest-priority due task and mark it RUNNING.

        Only PENDING/SCHEDULED tasks whose ``run_after`` has passed are
        eligible. The attempt counter is incremented here, exactly once per
        physical attempt, so a crash after claim still accounts for the try.
        """
        now = time.time() if now is None else now

        def _claim() -> TaskRecord | None:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                # PENDING (fresh) tasks are ready immediately and ordered
                # purely by priority; SCHEDULED (retry) tasks additionally
                # wait for their backoff. Using 0 as the due key for PENDING
                # prevents an earlier submission timestamp from beating a
                # higher-priority task.
                row = conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE state IN ('PENDING', 'SCHEDULED')
                      AND (state = 'PENDING' OR run_after <= ?)
                    ORDER BY priority DESC,
                             CASE state WHEN 'PENDING' THEN 0 ELSE run_after END ASC,
                             created_at ASC, id ASC
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None
                rec = self._row_to_record(row)
                attempts = rec.attempts + 1
                conn.execute(
                    """
                    UPDATE tasks
                    SET state = 'RUNNING', attempts = ?, run_token = ?,
                        started_at = COALESCE(started_at, ?), updated_at = ?
                    WHERE id = ? AND state IN ('PENDING', 'SCHEDULED')
                    """,
                    (attempts, run_token, now, now, rec.id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            rec.state = TaskState.RUNNING
            rec.attempts = attempts
            rec.run_token = run_token
            rec.started_at = rec.started_at or now
            rec.updated_at = now
            return rec

        async with self._lock:
            await self._ensure_open_locked()
            return await self._sql(_claim)

    async def peek_due(self, *, now: float | None = None) -> TaskRecord | None:
        """Return the next due task without claiming it (for the wakeup timer)."""
        now = time.time() if now is None else now
        async with self._lock:
            await self._ensure_open_locked()
            r = await self._sql(
                lambda: self.conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE state IN ('PENDING', 'SCHEDULED')
                      AND (state = 'PENDING' OR run_after <= ?)
                    ORDER BY priority DESC,
                             CASE state WHEN 'PENDING' THEN 0 ELSE run_after END ASC,
                             created_at ASC, id ASC
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()
            )
        return None if r is None else self._row_to_record(r)

    async def next_run_after(self) -> float | None:
        """Earliest run_after among waiting tasks; None if nothing is waiting."""
        async with self._lock:
            await self._ensure_open_locked()
            r = await self._sql(
                lambda: self.conn.execute(
                    """
                    SELECT MIN(run_after) AS m FROM tasks
                    WHERE state IN ('PENDING', 'SCHEDULED')
                    """
                ).fetchone()
            )
        return None if r is None or r["m"] is None else float(r["m"])

    # ------------------------------------------------ terminal transitions
    async def mark_succeeded(self, task_id: str, result: str | None) -> TaskRecord:
        now = time.time()
        return await self._update(
            task_id,
            (TaskState.RUNNING,),
            {
                "state": TaskState.SUCCEEDED,
                "result": result,
                "error": None,
                "finished_at": now,
                "updated_at": now,
            },
            reason="cannot mark task succeeded",
        )

    async def mark_retrying(
        self, task_id: str, error: str, run_after: float
    ) -> TaskRecord:
        now = time.time()
        return await self._update(
            task_id,
            (TaskState.RUNNING,),
            {
                "state": TaskState.SCHEDULED,
                "error": error,
                "run_after": run_after,
                "run_token": None,
                "updated_at": now,
            },
            reason="cannot schedule retry",
        )

    async def mark_dead_letter(self, task_id: str, error: str) -> TaskRecord:
        now = time.time()
        # RUNNING normally; SCHEDULED/PENDING allowed for recovery edge cases.
        return await self._update(
            task_id,
            (TaskState.RUNNING, TaskState.SCHEDULED, TaskState.PENDING),
            {
                "state": TaskState.DEAD_LETTER,
                "error": error,
                "run_token": None,
                "finished_at": now,
                "updated_at": now,
            },
            reason="cannot move task to dead letter",
        )

    async def mark_cancelled(self, task_id: str, reason: str = CANCEL_ERROR) -> TaskRecord:
        now = time.time()
        return await self._update(
            task_id,
            (TaskState.PENDING, TaskState.SCHEDULED, TaskState.RUNNING),
            {
                "state": TaskState.CANCELLED,
                "error": reason,
                "run_token": None,
                "finished_at": now,
                "updated_at": now,
            },
            reason="cannot cancel task",
        )

    async def request_cancel(self, task_id: str) -> TaskRecord:
        """Persist the cancel intent; rejected for terminal/unknown tasks."""
        now = time.time()
        return await self._update(
            task_id,
            (TaskState.PENDING, TaskState.SCHEDULED, TaskState.RUNNING),
            {"cancel_requested": 1, "updated_at": now},
            reason="cannot request cancellation",
        )

    async def is_cancel_requested(self, task_id: str) -> bool:
        rec = await self._get_locked_public(task_id)
        return rec.cancel_requested

    async def _get_locked_public(self, task_id: str) -> TaskRecord:
        async with self._lock:
            await self._ensure_open_locked()
            return await self._sql(self._get_locked, task_id)

    # ------------------------------------------------------------- recovery
    async def recover_interrupted(self) -> dict[str, int]:
        """Reconcile state left by a previous process on startup.

        * RUNNING tasks never finished, so they must NOT be marked succeeded.
          The interrupted attempt is already counted; retry while attempts
          remain, otherwise dead-letter with the restart reason.
        * PENDING/SCHEDULED tasks whose cancel was requested before the crash
          become CANCELLED.
        """
        summary = {"rescheduled": 0, "dead_letter": 0, "cancelled": 0}

        def _recover() -> dict[str, int]:
            conn = self.conn
            now = time.time()
            conn.execute("BEGIN IMMEDIATE")
            try:
                interrupted = conn.execute(
                    "SELECT * FROM tasks WHERE state = 'RUNNING'"
                ).fetchall()
                for row in interrupted:
                    rec = self._row_to_record(row)
                    if rec.cancel_requested:
                        conn.execute(
                            "UPDATE tasks SET state = 'CANCELLED', error = ?, "
                            "run_token = NULL, finished_at = ?, updated_at = ? WHERE id = ?",
                            (CANCEL_ERROR, now, now, rec.id),
                        )
                        summary["cancelled"] += 1
                    elif rec.attempts < rec.max_attempts:
                        conn.execute(
                            "UPDATE tasks SET state = 'SCHEDULED', run_after = ?, "
                            "error = ?, run_token = NULL, updated_at = ? WHERE id = ?",
                            (now, RESTART_ERROR, now, rec.id),
                        )
                        summary["rescheduled"] += 1
                    else:
                        conn.execute(
                            "UPDATE tasks SET state = 'DEAD_LETTER', error = ?, "
                            "run_token = NULL, finished_at = ?, updated_at = ? WHERE id = ?",
                            (RESTART_ERROR, now, now, rec.id),
                        )
                        summary["dead_letter"] += 1

                stale_cancels = conn.execute(
                    "SELECT id FROM tasks WHERE cancel_requested = 1 "
                    "AND state IN ('PENDING', 'SCHEDULED')"
                ).fetchall()
                for row in stale_cancels:
                    conn.execute(
                        "UPDATE tasks SET state = 'CANCELLED', error = ?, "
                        "finished_at = ?, updated_at = ? WHERE id = ?",
                        (CANCEL_ERROR, now, now, row["id"]),
                    )
                    summary["cancelled"] += 1
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            return summary

        async with self._lock:
            await self._ensure_open_locked()
            return await self._sql(_recover)
