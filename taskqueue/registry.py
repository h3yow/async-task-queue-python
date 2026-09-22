"""任务处理器注册表。

处理器签名: ``async def handler(payload, context) -> result``
其中 context 是 :class:`ExecutionContext`，用于让长任务感知取消信号与超时。
"""
from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class ExecutionContext:
    task_id: str
    attempt: int
    cancel_event: Any  # asyncio.Event，任务可在循环中 await is_set()

    @property
    def cancelled(self) -> bool:
        return bool(self.cancel_event and self.cancel_event.is_set())


TaskHandler = Callable[[dict[str, Any], ExecutionContext], Awaitable[Any]]


class HandlerNotFoundError(Exception):
    """提交了未注册的任务类型。"""

    def __init__(self, task_type: str):
        super().__init__(f"no handler registered for task_type={task_type}")
        self.task_type = task_type


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        if not callable(handler):
            raise TypeError(f"handler for {task_type} must be callable")
        if not inspect.iscoroutinefunction(handler):
            raise TypeError(f"handler for {task_type} must be an async function")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError:
            raise HandlerNotFoundError(task_type) from None

    def __contains__(self, task_type: str) -> bool:
        return task_type in self._handlers
