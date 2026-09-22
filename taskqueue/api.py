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
    ValidationError,
)
from taskqueue.models import Task, TaskState

router = APIRouter(prefix="/tasks", tags=["tasks"])


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
        )


def _get_service(request: Request):
    return request.app.state.service


def _raise_domain_error(exc: Exception) -> None:
    if isinstance(exc, UnknownTaskError):
        raise HTTPException(status_code=404, detail={
            "error": "unknown_task", "message": str(exc),
            "task_id": getattr(exc, "task_id", None),
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
