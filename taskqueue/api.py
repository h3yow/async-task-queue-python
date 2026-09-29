"""FastAPI 路由：任务提交 / 查询 / 幂等查询 / 取消。

错误统一映射为可区分的状态码与 error 字段:
- UnknownTaskError              -> 404 unknown_task
- InvalidStateTransitionError   -> 409 invalid_state
- IdempotencyConflictError      -> 409 idempotency_conflict
- ValidationError               -> 400 validation_error
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from taskqueue.errors import (
    IdempotencyConflictError,
    InvalidStateTransitionError,
    UnknownTaskError,
    UnknownWorkflowError,
    ValidationError,
)
from taskqueue.models import Task, TaskState
from taskqueue.workflow import WorkflowSpec, WorkflowTaskSpec

router = APIRouter(prefix="/tasks", tags=["tasks"])
workflow_router = APIRouter(prefix="/workflows", tags=["workflows"])


# ---------- 请求 / 响应模型 ----------


class SubmitRequest(BaseModel):
    task_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=200)
    priority: int = 0
    max_attempts: int | None = Field(default=None, ge=1)
    timeout: float | None = Field(default=None, gt=0)


class TaskResponse(BaseModel):
    id: str
    task_type: str
    payload: dict[str, Any]
    idempotency_key: str | None
    state: TaskState
    priority: int
    attempts: int
    max_attempts: int
    timeout: float
    not_before: float
    result: Any | None
    last_error: str | None
    error_kind: str | None
    created_at: float
    updated_at: float
    started_at: float | None
    finished_at: float | None
    workflow_id: str | None = None
    task_key: str | None = None
    skipped_by_task_id: str | None = None

    @classmethod
    def from_task(cls, task: Task) -> TaskResponse:
        return cls(
            id=task.id,
            task_type=task.task_type,
            payload=task.payload,
            idempotency_key=task.idempotency_key,
            state=task.state,
            priority=task.priority,
            attempts=task.attempts,
            max_attempts=task.max_attempts,
            timeout=task.timeout,
            not_before=task.not_before,
            result=task.result,
            last_error=task.last_error,
            error_kind=task.error_kind,
            created_at=task.created_at,
            updated_at=task.updated_at,
            started_at=task.started_at,
            finished_at=task.finished_at,
            workflow_id=task.workflow_id,
            task_key=task.task_key,
            skipped_by_task_id=task.skipped_by_task_id,
        )


# ---------- 流程请求 / 响应模型 ----------


class WorkflowTaskRequest(BaseModel):
    key: str = Field(..., min_length=1)
    task_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    idempotency_key: str | None = Field(default=None, max_length=200)
    priority: int = 0
    max_attempts: int | None = Field(default=None, ge=1)
    timeout: float | None = Field(default=None, gt=0)


class WorkflowSubmitRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    tasks: list[WorkflowTaskRequest] = Field(..., min_length=1)


class WorkflowTaskItem(BaseModel):
    key: str | None
    depends_on: list[str]
    skipped_by_key: str | None
    task: TaskResponse

    @classmethod
    def from_view(cls, view) -> WorkflowTaskItem:
        return cls(
            key=view.task.task_key,
            depends_on=view.depends_on,
            skipped_by_key=view.skipped_by_key,
            task=TaskResponse.from_task(view.task),
        )


class WorkflowResponse(BaseModel):
    id: str
    name: str | None
    created_at: float
    state: str
    state_counts: dict[str, int]
    tasks: list[WorkflowTaskItem]

    @classmethod
    def from_view(cls, view) -> WorkflowResponse:
        return cls(
            id=view.workflow.id,
            name=view.workflow.name,
            created_at=view.workflow.created_at,
            state=view.state,
            state_counts=view.state_counts,
            tasks=[WorkflowTaskItem.from_view(t) for t in view.tasks],
        )


def _get_service(request: Request):
    return request.app.state.service


def _raise_domain_error(exc: Exception) -> None:
    if isinstance(exc, UnknownTaskError):
        raise HTTPException(status_code=404, detail={
            "error": "unknown_task", "message": str(exc),
            "task_id": getattr(exc, "task_id", None),
        })
    if isinstance(exc, UnknownWorkflowError):
        raise HTTPException(status_code=404, detail={
            "error": "unknown_workflow", "message": str(exc),
            "workflow_id": getattr(exc, "workflow_id", None),
        })
    if isinstance(exc, InvalidStateTransitionError):
        raise HTTPException(status_code=409, detail={
            "error": "invalid_state", "message": str(exc),
            "task_id": exc.task_id, "current_state": exc.current_state,
            "operation": exc.operation,
        })
    if isinstance(exc, IdempotencyConflictError):
        raise HTTPException(status_code=409, detail={
            "error": "idempotency_conflict", "message": str(exc),
            "existing_task_id": exc.existing_task_id,
        })
    if isinstance(exc, ValidationError):
        raise HTTPException(status_code=400, detail={
            "error": "validation_error", "message": str(exc),
        })
    raise exc


# ---------- 路由 ----------


@router.post("", response_model=TaskResponse, status_code=201)
async def submit_task(req: SubmitRequest, request: Request) -> TaskResponse:
    service = _get_service(request)
    try:
        task = await service.submit(
            task_type=req.task_type,
            payload=req.payload,
            idempotency_key=req.idempotency_key,
            priority=req.priority,
            max_attempts=req.max_attempts,
            timeout=req.timeout,
        )
    except (UnknownTaskError, InvalidStateTransitionError,
            IdempotencyConflictError, ValidationError) as exc:
        _raise_domain_error(exc)
        raise  # 不可达，仅为类型检查
    return TaskResponse.from_task(task)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, request: Request) -> TaskResponse:
    service = _get_service(request)
    try:
        task = await service.get_task(task_id)
    except UnknownTaskError as exc:
        _raise_domain_error(exc)
        raise
    return TaskResponse.from_task(task)


@router.get("/by-idempotency-key/{key:path}", response_model=TaskResponse)
async def get_by_idempotency_key(key: str, request: Request) -> TaskResponse:
    service = _get_service(request)
    try:
        task = await service.get_by_idempotency_key(key)
    except UnknownTaskError as exc:
        _raise_domain_error(exc)
        raise
    return TaskResponse.from_task(task)


@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(task_id: str, request: Request) -> TaskResponse:
    service = _get_service(request)
    try:
        task = await service.cancel(task_id)
    except (UnknownTaskError, InvalidStateTransitionError) as exc:
        _raise_domain_error(exc)
        raise
    return TaskResponse.from_task(task)


# ---------- 流程路由 ----------


@workflow_router.post("", response_model=WorkflowResponse, status_code=201)
async def submit_workflow(
    req: WorkflowSubmitRequest, request: Request
) -> WorkflowResponse:
    service = _get_service(request)
    spec = WorkflowSpec(
        name=req.name,
        tasks=[
            WorkflowTaskSpec(
                key=item.key,
                task_type=item.task_type,
                depends_on=list(item.depends_on),
                payload=item.payload,
                idempotency_key=item.idempotency_key,
                priority=item.priority,
                max_attempts=item.max_attempts,
                timeout=item.timeout,
            )
            for item in req.tasks
        ],
    )
    try:
        view = await service.submit_workflow(spec)
    except (UnknownWorkflowError, IdempotencyConflictError,
            ValidationError) as exc:
        _raise_domain_error(exc)
        raise  # 不可达，仅为类型检查
    return WorkflowResponse.from_view(view)


@workflow_router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(workflow_id: str, request: Request) -> WorkflowResponse:
    service = _get_service(request)
    try:
        view = await service.get_workflow(workflow_id)
    except UnknownWorkflowError as exc:
        _raise_domain_error(exc)
        raise
    return WorkflowResponse.from_view(view)
