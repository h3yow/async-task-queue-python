"""FastAPI application wiring the persistent async task queue.

Configuration via environment variables:

* ``TASKQUEUE_DB``        - SQLite path (default ``./tasks.db``; ``:memory:``
                            only makes sense for tests)
* ``TASKQUEUE_CONCURRENCY`` - max concurrent tasks (default 4)
* ``TASKQUEUE_TIMEOUT``   - per-task timeout seconds (default 30)
* ``TASKQUEUE_BASE_DELAY``/``TASKQUEUE_MAX_DELAY``/``TASKQUEUE_MULTIPLIER`` -
  exponential backoff settings
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from fastapi import FastAPI

from taskqueue.api import create_router, create_stats_router
from taskqueue.models import RetryPolicy
from taskqueue.service import TaskService

logging.basicConfig(
    level=os.environ.get("TASKQUEUE_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("taskqueue.app")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw is not None else default


def create_app() -> FastAPI:
    service = TaskService(
        db_path=os.environ.get("TASKQUEUE_DB", "./tasks.db"),
        concurrency=int(os.environ.get("TASKQUEUE_CONCURRENCY", "4")),
        default_timeout=_env_float("TASKQUEUE_TIMEOUT", 30.0),
        retry_policy=RetryPolicy(
            max_attempts=int(os.environ.get("TASKQUEUE_MAX_ATTEMPTS", "1")),
            base_delay=_env_float("TASKQUEUE_BASE_DELAY", 0.2),
            max_delay=_env_float("TASKQUEUE_MAX_DELAY", 60.0),
            multiplier=_env_float("TASKQUEUE_MULTIPLIER", 2.0),
        ),
        poll_interval=_env_float("TASKQUEUE_POLL_INTERVAL", 0.05),
    )

    # ---------------- example handlers ----------------
    async def echo(payload: dict[str, Any]) -> dict[str, Any]:
        return {"echoed": payload}

    async def flaky(payload: dict[str, Any]) -> dict[str, Any]:
        """Fails ``fail_times`` times then succeeds (demonstrates retries)."""
        key = payload["idempotency"] if "idempotency" in payload else payload.get("key", "demo")
        state = flaky_state.setdefault(key, 0)
        flaky_state[key] = state + 1
        if state < int(payload.get("fail_times", 1)):
            raise RuntimeError(f"flaky failure #{state + 1} for {key}")
        return {"ok": True, "attempts": flaky_state[key]}

    async def slow(payload: dict[str, Any]) -> dict[str, Any]:
        """Sleeps to prove a slow task does not block other slots."""
        await asyncio.sleep(float(payload.get("seconds", 1.0)))
        return {"slept": payload.get("seconds", 1.0)}

    flaky_state: dict[str, Any] = {}
    service.register_handler("echo", echo)
    service.register_handler("flaky", flaky)
    service.register_handler("slow", slow)

    async def lifespan(app: FastAPI) -> Any:
        summary = await service.start()
        logger.info("task service started: %s", summary)
        yield
        await service.close()

    app = FastAPI(title="Persistent Async Task Queue", lifespan=lifespan)
    app.include_router(create_router(service))
    app.include_router(create_stats_router(service))
    app.state.service = service

    @app.get("/")
    def read_root() -> dict[str, str]:
        return {"service": "async-task-queue", "docs": "/docs"}

    return app


app = create_app()
