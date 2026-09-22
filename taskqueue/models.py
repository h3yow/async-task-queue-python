"""Domain models: task states, retry policy and task records.

State machine
-------------

::

    PENDING ──► RUNNING ──► SUCCEEDED
      │            │
      │            ├─► SCHEDULED (retryable failure, waiting for backoff)
      │            │       │
      │            └───────┘
      │
      ├─► CANCELLED   (cancel requested before/during execution)
      └─► DEAD_LETTER (all retries exhausted, or interrupted with no retries)

    RUNNING (after a crash / process restart) ──► SCHEDULED | DEAD_LETTER

* PENDING      - accepted, waiting for a free worker slot
* SCHEDULED    - waiting until ``run_after`` for a retry
* RUNNING      - a worker is executing the task
* SUCCEEDED    - terminal, result stored
* FAILED       - terminal marker only used when a task is rejected; in practice
                 exhausted failures go to DEAD_LETTER
* CANCELLED    - terminal, cancel completed before any further attempt
* DEAD_LETTER  - terminal for this queue; ``error`` records the reason and
                 ``attempts`` records how many attempts were made
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum


class TaskState(str, Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATES

    @property
    def can_cancel(self) -> bool:
        """Cancellation is legal only while the task has not produced a result."""
        return self in (TaskState.PENDING, TaskState.SCHEDULED, TaskState.RUNNING)


TERMINAL_STATES = frozenset(
    {
        TaskState.SUCCEEDED,
        TaskState.FAILED,
        TaskState.CANCELLED,
        TaskState.DEAD_LETTER,
    }
)


# Legal transitions; used by the store to reject illegal state changes.
LEGAL_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset(
        {
            TaskState.RUNNING,
            TaskState.CANCELLED,
            TaskState.DEAD_LETTER,
        }
    ),
    TaskState.SCHEDULED: frozenset(
        {
            TaskState.RUNNING,
            TaskState.CANCELLED,
            TaskState.DEAD_LETTER,
        }
    ),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCEEDED,
            TaskState.SCHEDULED,
            TaskState.CANCELLED,
            TaskState.DEAD_LETTER,
        }
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
    TaskState.DEAD_LETTER: frozenset(),
}


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry policy with exponential backoff.

    ``max_attempts`` counts the first attempt, so ``max_attempts=3`` means one
    initial attempt plus up to two retries.
    """

    max_attempts: int = 1
    base_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")

    def should_retry(self, attempts: int) -> bool:
        """Return True if another attempt is allowed after ``attempts`` tries."""
        return attempts < self.max_attempts

    def backoff_delay(self, attempts: int) -> float:
        """Delay (seconds) before attempt number ``attempts + 1``."""
        if attempts < 1:
            return 0.0
        raw = self.base_delay * (self.multiplier ** (attempts - 1))
        return min(self.max_delay, raw)


@dataclass(slots=True)
class TaskRecord:
    id: str
    idempotency_key: str | None
    task_type: str
    payload: str
    state: TaskState = TaskState.PENDING
    priority: int = 0
    attempts: int = 0
    max_attempts: int = 1
    run_after: float = 0.0
    result: str | None = None
    error: str | None = None
    # Token of the worker/process that currently owns a RUNNING task.
    # Used to detect tasks interrupted by a crash on restart.
    run_token: str | None = None
    # Persisted cancel intent: lets a running/queued task be cancelled even
    # though the worker coroutine is busy or the process restarts.
    cancel_requested: bool = False
    created_at: float = 0.0
    updated_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None

    @classmethod
    def new(
        cls,
        task_id: str,
        task_type: str,
        payload: str,
        *,
        idempotency_key: str | None = None,
        priority: int = 0,
        max_attempts: int = 1,
    ) -> TaskRecord:
        now = time.time()
        return cls(
            id=task_id,
            idempotency_key=idempotency_key,
            task_type=task_type,
            payload=payload,
            state=TaskState.PENDING,
            priority=priority,
            attempts=0,
            max_attempts=max_attempts,
            run_after=now,
            created_at=now,
            updated_at=now,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "idempotency_key": self.idempotency_key,
            "task_type": self.task_type,
            "payload": self.payload,
            "state": self.state.value,
            "priority": self.priority,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "run_after": self.run_after,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }
