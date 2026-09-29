"""FastAPI 应用组装。

通过 :func:`create_app` 构建应用并在 lifespan 中启动/停止 TaskService；
测试或调用方可注入自定义 config 与 registry（含任务处理器）。
"""
from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI

from taskqueue.api import router as task_router
from taskqueue.api import workflow_router
from taskqueue.config import TaskQueueConfig
from taskqueue.logging_setup import configure_logging
from taskqueue.registry import HandlerRegistry
from taskqueue.retry import RetryPolicy
from taskqueue.service import TaskService
from taskqueue.store import TaskStore

RegistryConfigurator = Callable[[HandlerRegistry], None]


def create_app(
    config: TaskQueueConfig | None = None,
    registry: HandlerRegistry | None = None,
    configure: RegistryConfigurator | None = None,
    logger: logging.Logger | None = None,
) -> FastAPI:
    config = config or TaskQueueConfig(db_path=os.environ.get(
        "TASKQUEUE_DB", "taskqueue.db"))
    registry = registry or HandlerRegistry()
    if configure is not None:
        configure(registry)
    logger = logger or configure_logging()

    store = TaskStore(config.db_path)
    service = TaskService(store, registry, RetryPolicy(config.retry),
                          config, logger)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(title="Async Task Queue", version="0.1.0", lifespan=lifespan)
    app.state.config = config
    app.state.service = service

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(task_router)
    app.include_router(workflow_router)
    return app
