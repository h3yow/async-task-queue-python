"""带依赖的流程提交规格与 DAG 校验。

一个流程由若干 :class:`WorkflowTaskSpec` 组成，任务之间用流程内唯一的
``key`` 标识，通过 ``depends_on`` 声明先后依赖。校验在提交前完成：
- key 非空且在流程内唯一；
- depends_on 只能引用流程内已存在的 key，且不能依赖自己；
- 依赖图必须是有向无环图（DAG），存在环直接拒绝。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from taskqueue.errors import ValidationError


@dataclass
class WorkflowTaskSpec:
    """流程中的单个任务规格。"""

    key: str
    task_type: str
    depends_on: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str | None = None
    priority: int = 0
    max_attempts: int | None = None
    timeout: float | None = None


@dataclass
class WorkflowSpec:
    """一次流程提交的完整规格。"""

    tasks: list[WorkflowTaskSpec]
    name: str | None = None


def validate_workflow_spec(spec: WorkflowSpec) -> None:
    """对流程规格做结构性校验。处理器类型等运行期校验由 service 负责。"""
    if not isinstance(spec.tasks, list) or not spec.tasks:
        raise ValidationError("workflow must contain at least one task")

    seen: set[str] = set()
    for item in spec.tasks:
        if not isinstance(item.key, str) or not item.key:
            raise ValidationError("each workflow task requires a non-empty key")
        if item.key in seen:
            raise ValidationError(f"duplicate task key in workflow: {item.key}")
        seen.add(item.key)
        if not isinstance(item.depends_on, list):
            raise ValidationError(f"depends_on of {item.key} must be a list")

    # 依赖引用合法性
    adjacency: dict[str, list[str]] = {key: [] for key in seen}
    for item in spec.tasks:
        for dep in item.depends_on:
            if dep not in seen:
                raise ValidationError(
                    f"task {item.key} depends on unknown task key: {dep}"
                )
            if dep == item.key:
                raise ValidationError(f"task {item.key} cannot depend on itself")
            adjacency[dep].append(item.key)

    _ensure_acyclic(seen, adjacency)


def _ensure_acyclic(nodes: set[str], adjacency: dict[str, list[str]]) -> None:
    """Kahn 拓扑排序：无法消费完全部节点即说明存在环。"""
    indegree: dict[str, int] = {key: 0 for key in nodes}
    for outs in adjacency.values():
        for dst in outs:
            indegree[dst] += 1

    queue = [key for key, deg in indegree.items() if deg == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for nxt in adjacency[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if visited != len(nodes):
        cyclic = sorted(key for key, deg in indegree.items() if deg > 0)
        raise ValidationError(
            f"workflow dependency graph contains a cycle involving: {cyclic}"
        )
