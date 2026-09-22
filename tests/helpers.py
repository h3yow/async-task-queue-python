"""测试辅助：可编排的处理器与状态等待。"""
from __future__ import annotations

import asyncio

from taskqueue.errors import NonRetryableTaskError, RetryableTaskError
from taskqueue.models import Task, TaskState
from taskqueue.service import TaskService

TERMINAL = {
    TaskState.SUCCEEDED,
    TaskState.FAILED,
    TaskState.CANCELLED,
    TaskState.DEAD_LETTER,
}


class CallRecorder:
    """跨“进程”记录每个任务类型的真实执行次数（写入文件持久化）。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = asyncio.Lock()

    def _read(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        try:
            with open(self.path) as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=")
                        counts[k] = int(v)
        except FileNotFoundError:
            pass
        return counts

    def _write(self, counts: dict[str, int]) -> None:
        with open(self.path, "w") as f:
            f.write("\n".join(f"{k}={v}" for k, v in counts.items()))

    async def bump(self, key: str) -> int:
        async with self._lock:
            counts = await asyncio.to_thread(self._read)
            counts[key] = counts.get(key, 0) + 1
            await asyncio.to_thread(self._write, counts)
            return counts[key]

    def count(self, key: str) -> int:
        return self._read().get(key, 0)


def register_handlers(registry, recorder: CallRecorder, gates: dict | None = None):
    """注册一组用于测试的处理器。

    - succeed: 直接回显 payload
    - flaky: 前 fail_times 次抛可重试错误，之后成功
    - always_retry_fail: 永远抛可重试错误
    - non_retryable: 抛不可重试错误
    - slow: 阻塞直到被取消
    """
    gates = gates or {}

    async def succeed(payload, ctx):
        await recorder.bump("succeed")
        return {"echo": payload}

    async def flaky(payload, ctx):
        n = await recorder.bump("flaky")
        if n <= int(payload.get("fail_times", 2)):
            raise RetryableTaskError(f"transient failure #{n}")
        return {"attempt": n}

    async def always_retry_fail(payload, ctx):
        n = await recorder.bump("always_retry_fail")
        raise RetryableTaskError(f"persistent failure #{n}")

    async def non_retryable(payload, ctx):
        await recorder.bump("non_retryable")
        raise NonRetryableTaskError("bad request payload")

    async def slow(payload, ctx):
        gate = gates.get("slow_started")
        if gate is not None:
            gate.set()
        # 可被取消快速唤醒；否则等到超时
        try:
            await asyncio.wait_for(ctx.cancel_event.wait(), timeout=30)
        except TimeoutError:
            pass
        await recorder.bump("slow")
        return "slow-finished"

    registry.register("succeed", succeed)
    registry.register("flaky", flaky)
    registry.register("always_retry_fail", always_retry_fail)
    registry.register("non_retryable", non_retryable)
    registry.register("slow", slow)


async def wait_for_state(
    service: TaskService, task_id: str, states, timeout: float = 5.0
) -> Task:
    target = states if isinstance(states, (set, tuple, list)) else (states,)
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        task = await service.get_task(task_id)
        if task.state in target:
            return task
        if loop.time() > deadline:
            raise AssertionError(
                f"task {task_id} did not reach {target} within {timeout}s; "
                f"last state={task.state} attempts={task.attempts}"
            )
        await asyncio.sleep(0.01)
