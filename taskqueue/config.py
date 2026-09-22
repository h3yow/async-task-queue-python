"""配置定义。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RetryConfig:
    """重试退避配置。

    第 n 次失败后的等待:
        delay = min(max_delay, base_delay * multiplier ** (n - 1))
    任务总执行次数上限为 max_attempts，超出后进入死信（DEAD_LETTER）。
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 30.0
    multiplier: float = 2.0


@dataclass(frozen=True)
class TaskQueueConfig:
    """任务队列运行参数。"""

    db_path: str = "taskqueue.db"
    #: 最大并发执行数；慢任务不会突破该上限，也不会阻塞调度循环
    max_concurrency: int = 4
    #: 无到期任务时轮询休眠间隔（秒）
    poll_interval: float = 0.05
    #: 单任务默认超时（秒），可在提交时覆盖
    default_task_timeout: float = 60.0
    #: 停机时等待在执行任务完成结算的最长宽限时间（秒）
    shutdown_grace: float = 15.0

    retry: RetryConfig = field(default_factory=RetryConfig)
