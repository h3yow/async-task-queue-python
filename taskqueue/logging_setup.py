"""日志配置。

提供统一格式的日志，业务代码统一使用字段:
    task_id   任务标识
    attempts  尝试次数
    decision  判定依据（如 ACCEPTED / RETRY / DEAD_LETTER / CANCEL 等）
"""
from __future__ import annotations

import logging

_LOG_FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] "
    "%(message)s task_id=%(task_id)s attempts=%(attempts)s decision=%(decision)s"
)


class _TaskFilter(logging.Filter):
    """为任务相关日志记录补齐默认字段，避免非任务日志缺少占位符。"""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "task_id"):
            record.task_id = "-"
        if not hasattr(record, "attempts"):
            record.attempts = "-"
        if not hasattr(record, "decision"):
            record.decision = "-"
        return True


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """配置并返回任务队列使用的根 logger。"""
    logger = logging.getLogger("taskqueue")
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        handler.addFilter(_TaskFilter())
        logger.addHandler(handler)
    return logger
