"""Concurrency cap, priority scheduling, slow-task isolation, timeout/cancel."""

from __future__ import annotations

import asyncio

from conftest import wait_for_state


async def test_concurrency_cap_respected(service):
    svc, stats = service  # concurrency=2
    # Block the two slots with gated slow tasks.
    r1, _ = await svc.submit("gated_slow", {"key": "g1"})
    r2, _ = await svc.submit("gated_slow", {"key": "g2"})
    await asyncio.wait_for(stats.started.wait(), timeout=2.0)
    # only one event though; wait until both calls counted
    deadline = asyncio.get_running_loop().time() + 2.0
    while stats.calls.get("g1", 0) + stats.calls.get("g2", 0) < 2:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("two gated tasks never started")
        await asyncio.sleep(0.01)

    r3, _ = await svc.submit("succeed", {"key": "fast-queued"})
    await asyncio.sleep(0.15)
    waiting = await svc.get_task(r3.id)
    assert waiting.state.value == "PENDING", "third task must wait for a free slot"

    stats.release.set()
    await wait_for_state(svc, r1.id, {"SUCCEEDED"})
    await wait_for_state(svc, r2.id, {"SUCCEEDED"})
    await wait_for_state(svc, r3.id, {"SUCCEEDED"}, timeout=3.0)


async def test_slow_task_does_not_block_others(service):
    svc, stats = service
    # One slot occupied by a long sleep; the other slot keeps processing.
    slow, _ = await svc.submit("sleep", {"key": "slow", "seconds": 0.8})
    fast_ids = []
    for i in range(3):
        rec, _ = await svc.submit("succeed", {"key": f"f{i}"})
        fast_ids.append(rec.id)

    for tid in fast_ids:
        await wait_for_state(svc, tid, {"SUCCEEDED"}, timeout=3.0)
    await wait_for_state(svc, slow.id, {"SUCCEEDED"}, timeout=3.0)
    assert all(stats.calls[f"f{i}"] == 1 for i in range(3))
    assert stats.calls["slow"] == 1


async def test_priority_ordering(service):
    svc, stats = service
    # Fill both slots with gated tasks.
    gate1, _ = await svc.submit("gated_slow", {"key": "p-gate1"})
    gate2, _ = await svc.submit("gated_slow", {"key": "p-gate2"})
    while stats.calls.get("p-gate1", 0) + stats.calls.get("p-gate2", 0) < 2:
        await asyncio.sleep(0.01)

    low, _ = await svc.submit("succeed", {"key": "low"}, priority=0)
    high, _ = await svc.submit("succeed", {"key": "high"}, priority=100)
    await asyncio.sleep(0.05)
    stats.release.set()

    await wait_for_state(svc, low.id, {"SUCCEEDED"}, timeout=3.0)
    high_rec = await wait_for_state(svc, high.id, {"SUCCEEDED"}, timeout=3.0)
    low_rec = await svc.get_task(low.id)
    # high priority must have been dispatched/completed no later than low
    assert high_rec.updated_at <= low_rec.updated_at, (
        f"high finished {high_rec.updated_at} after low {low_rec.updated_at}"
    )
    await wait_for_state(svc, gate1.id, {"SUCCEEDED"})
    await wait_for_state(svc, gate2.id, {"SUCCEEDED"})


async def test_timeout_releases_slot_and_retries_then_dead_letters(service):
    svc, stats = service
    # default_timeout=2s; sleep 5s exceeds it. Give 2 attempts.
    rec, _ = await svc.submit(
        "sleep", {"key": "timed-out", "seconds": 5.0}, max_attempts=2
    )
    final = await wait_for_state(svc, rec.id, {"DEAD_LETTER", "SUCCEEDED"}, timeout=8.0)
    assert final.state.value == "DEAD_LETTER"
    assert final.attempts == 2
    assert "TimeoutError" in final.error

    # Slot was released: a later task still runs to success.
    after, _ = await svc.submit("succeed", {"key": "after-timeout"})
    await wait_for_state(svc, after.id, {"SUCCEEDED"}, timeout=3.0)
    assert stats.calls["after-timeout"] == 1


async def test_cancel_queued_task(service):
    svc, stats = service
    g1, _ = await svc.submit("gated_slow", {"key": "cq1"})
    g2, _ = await svc.submit("gated_slow", {"key": "cq2"})
    while stats.calls.get("cq1", 0) + stats.calls.get("cq2", 0) < 2:
        await asyncio.sleep(0.01)

    queued, _ = await svc.submit("succeed", {"key": "queued-victim"})
    await asyncio.sleep(0.05)
    cancelled = await svc.cancel(queued.id)
    assert cancelled.state.value == "CANCELLED"
    assert stats.calls.get("queued-victim", 0) == 0  # never executed
    stats.release.set()
    await wait_for_state(svc, g1.id, {"SUCCEEDED"})
    await wait_for_state(svc, g2.id, {"SUCCEEDED"})


async def test_cancel_running_task_interrupts_handler(service):
    svc, stats = service
    rec, _ = await svc.submit("gated_slow", {"key": "cancel-me"})
    await asyncio.wait_for(stats.started.wait(), timeout=2.0)

    cancelled = await svc.cancel(rec.id)
    assert cancelled.state.value == "RUNNING"  # flag set, handler still in flight
    stats.release.set()

    final = await wait_for_state(
        svc, rec.id, {"CANCELLED", "SUCCEEDED", "DEAD_LETTER"}, timeout=3.0
    )
    assert final.state.value == "CANCELLED"
    assert stats.calls["cancel-me"] == 1  # interrupted, never re-run
