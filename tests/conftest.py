"""共享 pytest 夹具。"""
from __future__ import annotations

import logging
import os

import pytest

from taskqueue.config import RetryConfig, TaskQueueConfig
from taskqueue.registry import HandlerRegistry
from taskqueue.retry import RetryPolicy
from taskqueue.service import TaskService
from taskqueue.store import TaskStore


@pytest.fixture
def db_path(tmp_path: str) -> str:
    return os.path.join(str(tmp_path), "tasks.db")


@pytest.fixture
def config(db_path: str) -> TaskQueueConfig:
    return TaskQueueConfig(
        db_path=db_path,
        max_concurrency=4,
        poll_interval=0.005,
        default_task_timeout=5.0,
        retry=RetryConfig(max_attempts=3, base_delay=0.01,
                          multiplier=2.0, max_delay=1.0),
    )


@pytest.fixture
def registry() -> HandlerRegistry:
    return HandlerRegistry()


@pytest.fixture
def logger() -> logging.Logger:
    log = logging.getLogger("taskqueue.test")
    log.setLevel(logging.CRITICAL)
    log.propagate = False
    return log


@pytest.fixture
def service(config, registry, logger) -> TaskService:
    return TaskService(
        TaskStore(config.db_path),
        registry,
        RetryPolicy(config.retry),
        config,
        logger,
    )
