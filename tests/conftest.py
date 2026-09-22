"""Shared pytest fixtures and helpers."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from taskqueue.models import RetryPolicy
from taskqueue.service import TaskService


def make_service(db_path: str, **kwargs: Any) -> TaskService:
    kwargs.setdefault("concurrency", 2)
    kwargs.setdefault("default_timeout", 2.0)
    kwargs.setdefault("poll_interval", 0.02)
    kwargs.setdefault(
        "retry_policy",
        RetryPolicy(max_attempts=1, base_delay=0.02, max_delay=1.0, multiplier=2.0),
    )
    return TaskService(db_path, **kwargs)


class HandlerStats:
    """Tracks real handler invocations so duplicate execution is detectable."""

    def __init__(self) -> None:
        self.calls: dict[str, int] = {}
        self.records: dict[str, list[dict[str, Any]]] = {}
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def _key(self, payload: dict[str, Any]) -> str:
        return str(payload.get("key", "default"))

    async def succeed(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = self._key(payload)
        self.calls[key] = self.calls.get(key, 0) + 1
        self.records.setdefault(key, []).append(dict(payload))
        return {"ok": True, "key": key, "calls": self.calls[key]}

    async def always_fail(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = self._key(payload)
        self.calls[key] = self.calls.get(key, 0) + 1
        self.records.setdefault(key, []).append(dict(payload))
        raise RuntimeError(f"persistent failure for {key}")

    async def flaky(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = self._key(payload)
        self.calls[key] = self.calls.get(key, 0) + 1
        self.records.setdefault(key, []).append(dict(payload))
        fail_times = int(payload.get("fail_times", 1))
        if self.calls[key] <= fail_times:
            raise RuntimeError(f"flaky failure #{self.calls[key]} for {key}")
        return {"ok": True, "calls": self.calls[key]}

    async def gated_slow(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = self._key(payload)
        self.calls[key] = self.calls.get(key, 0) + 1
        self.started.set()
        try:
            await asyncio.wait_for(self.release.wait(), timeout=10.0)
        except TimeoutError:
            raise AssertionError("gated_slow was not released in time") from None
        return {"released": True}

    async def sleep_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = self._key(payload)
        self.calls[key] = self.calls.get(key, 0) + 1
        await asyncio.sleep(float(payload.get("seconds", 0.2)))
        return {"slept": payload.get("seconds", 0.2)}


def register_handlers(service: TaskService, stats: HandlerStats) -> None:
    service.register_handler("succeed", stats.succeed)
    service.register_handler("always_fail", stats.always_fail)
    service.register_handler("flaky", stats.flaky)
    service.register_handler("gated_slow", stats.gated_slow)
    service.register_handler("sleep", stats.sleep_task)


async def wait_for_state(
    service: TaskService,
    task_id: str,
    states: set[str],
    *,
    timeout: float = 5.0,
    poll: float = 0.01,
):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        rec = await service.get_task(task_id)
        if rec.state.value in states:
            return rec
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(
                f"task {task_id} did not reach {states}; "
                f"last state={rec.state.value} attempts={rec.attempts} error={rec.error}"
            )
        await asyncio.sleep(poll)


@pytest_asyncio.fixture
async def service(tmp_path: Path):
    svc = make_service(str(tmp_path / "tasks.db"))
    stats = HandlerStats()
    register_handlers(svc, stats)
    await svc.start()
    try:
        yield svc, stats
    finally:
        await svc.close()


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "restart.db")
