"""任务领域模型。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskState(str, enum.Enum):
    PENDING = "PENDING"
    #: 流程任务：依赖尚未全部成功，不可被调度、不占并发名额
    BLOCKED = "BLOCKED"
    RETRY_WAIT = "RETRY_WAIT"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"
    #: 流程任务：因上游以非成功终态结束而被带停，自身从未执行
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

#: 流程整体状态（聚合自各任务状态）
WORKFLOW_RUNNING = "RUNNING"
WORKFLOW_SUCCEEDED = "SUCCEEDED"
WORKFLOW_FAILED = "FAILED"


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
    # ---- 流程编排（普通任务这三个字段均为 None/空，行为不变）----
    #: 所属流程 id（普通任务为 None）
    workflow_id: str | None = None
    #: 流程内客户端可读的唯一键
    task_key: str | None = None
    #: 被带停时，触发它的上游任务 id
    skipped_by_task_id: str | None = None


@dataclass
class Workflow:
    """一条任务流程的元数据。"""

    id: str
    created_at: float
    name: str | None = None


@dataclass
class WorkflowTaskView:
    """流程视角下的单个任务视图（含依赖键与带停来源）。"""

    task: Task
    depends_on: list[str] = field(default_factory=list)
    #: 带停来源任务在流程内的 key（仅 SKIPPED 时有值）
    skipped_by_key: str | None = None


@dataclass
class WorkflowView:
    """整条流程的进度视图。"""

    workflow: Workflow
    tasks: list[WorkflowTaskView]
    #: RUNNING / SUCCEEDED / FAILED
    state: str = WORKFLOW_RUNNING
    #: 各状态任务数，键为 TaskState.value
    state_counts: dict[str, int] = field(default_factory=dict)
