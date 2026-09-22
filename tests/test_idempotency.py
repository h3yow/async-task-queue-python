"""Concurrent submission with the same idempotency key must execute exactly once."""

from __future__ import annotations

import asyncio

from conftest import wait_for_state


async def test_sequential_duplicate_idempotency_key_returns_same_task(service):
    svc, stats = service
    rec1, created1 = await svc.submit(
        "succeed", {"key": "a"}, idempotency_key="idem-1"
    )
    rec2, created2 = await svc.submit(
        "succeed", {"key": "a-different-payload"}, idempotency_key="idem-1"
    )

    assert created1 is True
    assert created2 is False
    assert rec1.id == rec2.id
    assert rec2.payload == rec1.payload  # second payload is ignored, not executed

    await wait_for_state(svc, rec1.id, {"SUCCEEDED"})
    assert stats.calls["a"] == 1  # no duplicate execution
    assert "a-different-payload" not in stats.calls


async def test_concurrent_duplicate_submission_executes_once(service):
    svc, stats = service

    async def submit() -> str:
        rec, created = await svc.submit(
            "succeed", {"key": "race"}, idempotency_key="race-key"
        )
        results[created] = results.get(created, 0) + 1
        return rec.id

    results: dict[bool, int] = {}
    ids = await asyncio.gather(*(submit() for _ in range(20)))

    # Exactly one creation, everyone observes the same id.
    assert results == {True: 1, False: 19}
    assert len(set(ids)) == 1

    await wait_for_state(svc, ids[0], {"SUCCEEDED"})
    # The key guarantee: one idempotency key => one physical execution.
    assert stats.calls["race"] == 1


async def test_distinct_keys_execute_independently(service):
    svc, stats = service
    r1, _ = await svc.submit("succeed", {"key": "x"}, idempotency_key="kx")
    r2, _ = await svc.submit("succeed", {"key": "y"}, idempotency_key="ky")
    await asyncio.gather(
        wait_for_state(svc, r1.id, {"SUCCEEDED"}),
        wait_for_state(svc, r2.id, {"SUCCEEDED"}),
    )
    assert stats.calls == {"x": 1, "y": 1}


async def test_find_by_idempotency_key(service):
    svc, _ = service
    rec, _ = await svc.submit("succeed", {"key": "q"}, idempotency_key="lookup")
    found = await svc.find_by_idempotency_key("lookup")
    assert found is not None and found.id == rec.id
    assert await svc.find_by_idempotency_key("missing") is None
