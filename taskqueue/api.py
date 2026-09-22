"""FastAPI router exposing the task service."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from .errors import TaskError
from .models import TaskState
from .service import TaskService


class SubmitRequest(BaseModel):
    task_type: str = Field(..., examples=["echo"])
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=200)
    priority: int = Field(default=0, ge=-1_000_000, le=1_000_000)
    max_attempts: int = Field(default=1, ge=1, le=1000)


def create_router(service: TaskService) -> APIRouter:
    router = APIRouter(prefix="/tasks", tags=["tasks"])

    def raise_task_error(exc: TaskError) -> HTTPException:
        return HTTPException(status_code=exc.http_status, detail=exc.to_dict())

    @router.post("", status_code=202)
    async def submit_task(req: SubmitRequest) -> dict[str, Any]:
        try:
            rec, created = await service.submit(
                req.task_type,
                req.payload,
                idempotency_key=req.idempotency_key,
                priority=req.priority,
                max_attempts=req.max_attempts,
            )
        except TaskError as exc:
            raise raise_task_error(exc) from exc
        body = rec.to_dict()
        body["created"] = created
        return body

    @router.get("/{task_id}")
    async def get_task(task_id: str) -> dict[str, Any]:
        try:
            rec = await service.get_task(task_id)
        except TaskError as exc:
            raise raise_task_error(exc) from exc
        return rec.to_dict()

    @router.get("/by-idempotency-key/{key}")
    async def get_by_key(key: str) -> dict[str, Any]:
        rec = await service.find_by_idempotency_key(key)
        if rec is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "task_not_found",
                    "message": f"no task with idempotency key {key!r}",
                    "task_id": None,
                },
            )
        return rec.to_dict()

    @router.post("/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict[str, Any]:
        try:
            rec = await service.cancel(task_id)
        except TaskError as exc:
            raise raise_task_error(exc) from exc
        return rec.to_dict()

    @router.get("")
    async def list_tasks(
        state: Annotated[TaskState | None, Query()] = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict[str, Any]]:
        recs = await service.list_tasks(state=state, limit=limit, offset=offset)
        return [r.to_dict() for r in recs]

    @router.get("/dead-letter/list")
    async def list_dead_letters(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[dict[str, Any]]:
        recs = await service.list_dead_letters(limit=limit)
        return [r.to_dict() for r in recs]

    return router


def create_stats_router(service: TaskService) -> APIRouter:
    router = APIRouter(tags=["meta"])

    @router.get("/stats")
    async def stats() -> dict[str, Any]:
        return {"counts": await service.stats(), "active_workers": service.worker.active_count}

    return router
