"""任务领域模型（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any


class TaskState(str, enum.Enum):
    PENDING = "PENDING"
    RETRY_WAIT = "RETRY_WAIT"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


TERMINAL_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED, TaskState.DEAD_LETTER}
)


@dataclass
class Task:
    id: str
    task_type: str
    payload: dict[str, Any]
    idempotency_key: str | None
    state: TaskState
    priority: int
    attempts: int = 0
    max_attempts: int = 3
    timeout: float = 60.0
    not_before: float = 0.0
    result: Any = None
    last_error: str | None = None
    error_kind: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    lease_owner: str | None = None
    lease_expires_at: float | None = None
