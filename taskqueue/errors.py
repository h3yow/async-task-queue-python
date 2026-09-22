"""Distinguishable failure reasons for task operations."""

from __future__ import annotations


class TaskError(Exception):
    """Base error. Each subclass has a stable ``code`` for clients/tests."""

    code: str = "task_error"
    http_status: int = 400

    def __init__(self, message: str = "", *, task_id: str | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.task_id = task_id

    def to_dict(self) -> dict[str, str | None]:
        return {"code": self.code, "message": self.message, "task_id": self.task_id}


class TaskNotFound(TaskError):
    """No task exists for the given id (or idempotency key)."""

    code = "task_not_found"
    http_status = 404


class DuplicateIdempotencyKey(TaskError):
    """A task with the same idempotency key already exists.

    This is *not* an execution error: the caller should reuse ``task_id`` from
    the existing task. Raised only in strict mode; concurrent submit normally
    returns the existing record instead.
    """

    code = "duplicate_idempotency_key"
    http_status = 409

    def __init__(self, message: str = "", *, task_id: str | None = None) -> None:
        super().__init__(message, task_id=task_id)


class InvalidStateTransition(TaskError):
    """The requested operation is illegal in the task's current state."""

    code = "invalid_state_transition"
    http_status = 409

    def __init__(
        self,
        message: str = "",
        *,
        task_id: str | None = None,
        from_state: str | None = None,
        to_state: str | None = None,
    ) -> None:
        super().__init__(message, task_id=task_id)
        self.from_state = from_state
        self.to_state = to_state

    def to_dict(self) -> dict[str, str | None]:
        data = super().to_dict()
        data["from_state"] = self.from_state
        data["to_state"] = self.to_state
        return data


class TaskCancelled(TaskError):
    """Raised inside a handler/worker when cancellation takes effect."""

    code = "task_cancelled"
    http_status = 409


class UnknownTaskType(TaskError):
    """No handler is registered for the submitted task type."""

    code = "unknown_task_type"
    http_status = 422
