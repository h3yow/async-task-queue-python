"""重试退避策略。

第 n 次失败后（attempts 为已尝试次数，从 1 开始）的等待时间:
    delay = min(max_delay, base_delay * multiplier ** (attempts - 1))
"""
from __future__ import annotations

from taskqueue.config import RetryConfig


class RetryPolicy:
    def __init__(self, config: RetryConfig):
        self.config = config

    def next_delay(self, attempts: int) -> float:
        """返回第 ``attempts`` 次尝试失败后的退避秒数。"""
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        raw = self.config.base_delay * (self.config.multiplier ** (attempts - 1))
        return min(self.config.max_delay, raw)

    def should_retry(self, attempts: int, max_attempts: int) -> bool:
        """已尝试 ``attempts`` 次后是否还应再试。"""
        return attempts < max_attempts
