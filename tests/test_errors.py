"""Unknown tasks and illegal state transitions must be rejected with
distinguishable error codes."""

from __future__ import annotations

import pytest
from conftest import wait_for_state

from taskqueue.errors import InvalidStateTransition, TaskNotFound
from taskqueue.models import TaskState


async def test_get_unknown_task_raises_not_found(service):
    svc, _ = service
    with pytest.raises(TaskNotFound) as exc:
        await svc.get_task("does-not-exist")
    assert exc.value.code == "task_not_found"
    assert exc.value.http_status == 404
    assert exc.value.task_id == "does-not-exist"


async def test_cancel_unknown_task_raises_not_found(service):
    svc, _ = service
    with pytest.raises(TaskNotFound) as exc:
        await svc.cancel("ghost")
    assert exc.value.code == "task_not_found"


async def test_submit_unknown_type_rejected(service):
    svc, _ = service
    from taskqueue.errors import UnknownTaskType

    with pytest.raises(UnknownTaskType) as exc:
        await svc.submit("not-a-real-handler", {})
    assert exc.value.code == "unknown_task_type"
    assert exc.value.http_status == 422


async def test_cancel_terminal_task_rejected(service):
    svc, stats = service
    rec, _ = await svc.submit("succeed", {"key": "done"})
    await wait_for_state(svc, rec.id, {"SUCCEEDED"})

    with pytest.raises(InvalidStateTransition) as exc:
        await svc.cancel(rec.id)
    assert exc.value.code == "invalid_state_transition"
    assert exc.value.from_state == TaskState.SUCCEEDED.value
    assert exc.value.to_state == TaskState.CANCELLED.value
    assert exc.value.http_status == 409
    # Nothing extra ran.
    assert stats.calls["done"] == 1


async def test_illegal_direct_transition_rejected_at_store(service):
    svc, _ = service
    rec, _ = await svc.submit("succeed", {"key": "z"})
    await wait_for_state(svc, rec.id, {"SUCCEEDED"})
    with pytest.raises(InvalidStateTransition) as exc:
        await svc.store.mark_retrying(rec.id, "nope", 0.0)
    assert exc.value.code == "invalid_state_transition"
    assert exc.value.from_state == "SUCCEEDED"
