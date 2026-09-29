"""可区分的失败原因。

分为两大类:
1. 客户端/状态类错误（提交/查询/取消阶段）
2. 任务执行阶段抛出的错误，决定重试还是直接失败
"""
from __future__ import annotations

# 执行错误的错误类别标识，写入 Task.error_kind 便于查询区分
ERROR_KIND_RETRYABLE = "RETRYABLE"
ERROR_KIND_NON_RETRYABLE = "NON_RETRYABLE"
ERROR_KIND_TIMEOUT = "TIMEOUT"
ERROR_KIND_CANCELLED = "CANCELLED"
ERROR_KIND_UNKNOWN_TYPE = "UNKNOWN_TASK_TYPE"


class TaskQueueError(Exception):
    """任务队列领域错误基类。"""


class UnknownTaskError(TaskQueueError):
    """查询/取消了不存在的任务。"""

    def __init__(self, task_id: str):
        super().__init__(f"unknown task: {task_id}")
        self.task_id = task_id


class UnknownFlowError(TaskQueueError):
    """查询了不存在的流程。"""

    def __init__(self, flow_id: str):
        super().__init__(f"unknown flow: {flow_id}")
        self.flow_id = flow_id


class InvalidStateTransitionError(TaskQueueError):
    """对处于非法状态的任务执行操作（如取消已终结任务）。"""

    def __init__(self, task_id: str, current_state: str, operation: str):
        super().__init__(
            f"invalid state transition: task={task_id} state={current_state} op={operation}"
        )
        self.task_id = task_id
        self.current_state = current_state
        self.operation = operation


class IdempotencyConflictError(TaskQueueError):
    """同一幂等键以不同的任务类型/负载重复提交。

    若完全同参重复提交则视为正常重放，返回已有任务，不会抛出本异常。
    """

    def __init__(self, existing_task_id: str):
        super().__init__(
            f"idempotency key already bound to task {existing_task_id} "
            "with a different task_type/payload"
        )
        self.existing_task_id = existing_task_id


class ValidationError(TaskQueueError):
    """提交参数非法。"""


# ---------- 执行阶段错误（由任务处理器抛出） ----------


class TaskExecutionError(Exception):
    """任务执行错误基类，携带错误类别。"""

    error_kind = ERROR_KIND_RETRYABLE


class RetryableTaskError(TaskExecutionError):
    """可重试错误：按退避策略重试，超出上限进入死信。"""

    error_kind = ERROR_KIND_RETRYABLE


class NonRetryableTaskError(TaskExecutionError):
    """不可重试错误：立即标记 FAILED。"""

    error_kind = ERROR_KIND_NON_RETRYABLE
