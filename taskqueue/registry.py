"""Registry mapping task type names to async handler callables."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from .errors import UnknownTaskType

# A handler receives the decoded JSON payload dict and returns any JSON-able
# result. It is executed inside ``asyncio.wait_for`` so it must be cancellation
# aware: blocking work should be offloaded with ``asyncio.to_thread``.
TaskHandler = Callable[[dict[str, Any]], Awaitable[Any]]


class TaskRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(
        self,
        name: str,
        handler: TaskHandler,
        *,
        override: bool = False,
    ) -> None:
        if not name:
            raise ValueError("task type name must not be empty")
        if not override and name in self._handlers:
            raise ValueError(f"handler already registered for {name!r}")
        self._handlers[name] = handler

    def get(self, name: str) -> TaskHandler:
        try:
            return self._handlers[name]
        except KeyError:
            raise UnknownTaskType(f"no handler registered for task type {name!r}") from None

    def names(self) -> list[str]:
        return sorted(self._handlers)
