"""任务领域模型。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskState(str, enum.Enum):
    PENDING = "PENDING"
    #: 等待上游依赖完成；不可被调度领取，不占并发名额
    BLOCKED = "BLOCKED"
    RETRY_WAIT = "RETRY_WAIT"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"
    #: 因上游未成功（失败/取消/死信/被跳过）而终止，未被执行，也不算成功
    SKIPPED = "SKIPPED"


TERMINAL_STATES = frozenset(
    {
        TaskState.SUCCEEDED,
        TaskState.FAILED,
        TaskState.CANCELLED,
        TaskState.DEAD_LETTER,
        TaskState.SKIPPED,
    }
)

#: 上游进入这些终态时，下游必须被带停（SKIPPED），不得继续执行
UPSTREAM_FAILURE_STATES = frozenset(
    {
        TaskState.FAILED,
        TaskState.CANCELLED,
        TaskState.DEAD_LETTER,
        TaskState.SKIPPED,
    }
)


@dataclass
class Task:
    id: str
    task_type: str
    payload: dict[str, Any]
    idempotency_key: str | None
    state: TaskState
    priority: int
    attempts: int = 0
    max_attempts: int = 3
    timeout: float = 60.0
    not_before: float = 0.0
    result: Any = None
    last_error: str | None = None
    error_kind: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    #: 所属流程 id；普通单任务为 None
    flow_id: str | None = None
    #: 在流程内由提交方指定的名字（同一流程内唯一）
    flow_key: str | None = None
    #: SKIPPED 时记录把它带停的上游任务 id
    blocked_by: str | None = None


@dataclass
class FlowTaskSpec:
    """流程提交时单个任务的规格（key 在流程内唯一，依赖按 key 引用）。"""

    key: str
    task_type: str
    payload: dict[str, Any] | None = None
    depends_on: list[str] = field(default_factory=list)
    priority: int = 0
    max_attempts: int | None = None
    timeout: float | None = None
    idempotency_key: str | None = None


class FlowState(str, enum.Enum):
    """流程整体状态（由成员任务状态推导，不单独落库）。"""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class FlowStatus:
    """一条流程的整体进度视图。"""

    flow_id: str
    state: FlowState
    tasks: list[Task] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.tasks)

    def count(self, state: TaskState) -> int:
        return sum(1 for t in self.tasks if t.state is state)
