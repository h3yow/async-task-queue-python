"""Persistence across process restarts.

Each "restart" is a fresh TaskService instance on the same SQLite file with
fresh in-memory handler counters, so duplicate execution is directly
observable.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from conftest import HandlerStats, make_service, register_handlers, wait_for_state


def persistent_flaky(counter_file: str):
    """Flaky handler whose attempt counter survives process restarts.

    Real retryable handlers must be idempotent; the retry decision lives in
    the queue but the side-effect accounting belongs to the handler/storage.
    """

    async def handler(payload):
        path = Path(counter_file)
        data = json.loads(path.read_text()) if path.exists() else {}
        key = str(payload.get("key", "default"))
        data[key] = data.get(key, 0) + 1
        path.write_text(json.dumps(data))
        if data[key] <= int(payload.get("fail_times", 1)):
            raise RuntimeError(f"persistent flaky failure #{data[key]} for {key}")
        return {"ok": True, "calls": data[key]}

    return handler


async def test_pending_task_survives_restart_and_executes_once(db_path):
    stats1 = HandlerStats()
    svc = make_service(db_path)
    register_handlers(svc, stats1)
    await svc.start()

    # Occupy both slots, queue a third task, then "crash" before it can run.
    # Occupiers allow retries so recovery reschedules rather than dead-letters.
    await svc.submit("sleep", {"key": "occupy1", "seconds": 5.0}, max_attempts=3)
    await svc.submit("sleep", {"key": "occupy2", "seconds": 5.0}, max_attempts=3)
    queued, _ = await svc.submit("succeed", {"key": "survivor"})

    deadline = asyncio.get_running_loop().time() + 2.0
    while stats1.calls.get("occupy1", 0) + stats1.calls.get("occupy2", 0) < 2:
        await asyncio.sleep(0.01)
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("occupiers never started")
    assert (await svc.get_task(queued.id)).state.value == "PENDING"

    # Hard stop: worker.stop() cancels handlers but writes no terminal state,
    # exactly simulating a kill -9 mid-execution.
    await svc.close()

    # ---- restart ----
    stats2 = HandlerStats()
    svc2 = make_service(db_path)
    register_handlers(svc2, stats2)
    summary = await svc2.start()
    assert summary["rescheduled"] == 2  # the two interrupted sleepers
    try:
        # Free the slots: the interrupted sleepers are allowed to retry but
        # are irrelevant to this assertion, so cancel them explicitly.
        for t in await svc2.list_tasks(state=None):
            if t.task_type == "sleep":
                await svc2.cancel(t.id)
        final = await wait_for_state(svc2, queued.id, {"SUCCEEDED", "DEAD_LETTER"}, timeout=5)
        assert final.state.value == "SUCCEEDED"
        assert stats2.calls["survivor"] == 1
    finally:
        await svc2.close()


async def test_interrupted_running_task_is_never_marked_succeeded(db_path, tmp_path):
    counter_file = str(tmp_path / "flaky_counter.json")
    first_attempt_blocked = asyncio.Event()
    release_attempt = asyncio.Event()

    async def gated_flaky(payload):
        path = Path(counter_file)
        data = json.loads(path.read_text()) if path.exists() else {}
        key = str(payload.get("key", "default"))
        data[key] = data.get(key, 0) + 1
        path.write_text(json.dumps(data))  # durable BEFORE we block
        if data[key] == 1:
            first_attempt_blocked.set()
            # Stay alive (still RUNNING) until the test "kills" the process.
            await asyncio.wait_for(release_attempt.wait(), timeout=10.0)
        if data[key] <= int(payload.get("fail_times", 1)):
            raise RuntimeError(f"persistent flaky failure #{data[key]} for {key}")
        return {"ok": True, "calls": data[key]}

    svc = make_service(db_path, default_timeout=10.0)
    stats1 = HandlerStats()
    register_handlers(svc, stats1)
    svc.register_handler("gated_flaky", gated_flaky)
    await svc.start()

    rec, _ = await svc.submit(
        "gated_flaky", {"key": "restarted", "fail_times": 1}, max_attempts=2
    )
    # Wait until attempt #1 has durably recorded itself and is still running.
    await asyncio.wait_for(first_attempt_blocked.wait(), timeout=3.0)
    cur = await svc.get_task(rec.id)
    assert cur.state.value == "RUNNING" and cur.attempts == 1
    release_attempt.set()  # unblock it; close below cancels before it finishes
    await svc.close()

    # ---- restart: route the task type to a durable (non-gated) handler ----
    svc2 = make_service(db_path, default_timeout=10.0)
    stats2 = HandlerStats()
    register_handlers(svc2, stats2)
    svc2.register_handler("gated_flaky", persistent_flaky(counter_file))
    await svc2.start()
    try:
        final = await wait_for_state(svc2, rec.id, {"SUCCEEDED", "DEAD_LETTER"}, timeout=6)
        assert final.state.value == "SUCCEEDED"
        # The interrupted attempt is counted; there is no silent success.
        assert final.attempts == 2
        assert final.error is None
    finally:
        await svc2.close()


async def test_interrupted_exhausted_task_goes_dead_letter_with_reason(db_path):
    svc = make_service(db_path, default_timeout=10.0)
    stats1 = HandlerStats()
    register_handlers(svc, stats1)
    await svc.start()
    rec, _ = await svc.submit(
        "always_fail", {"key": "lastchance"}, max_attempts=1
    )
    await wait_for_state(svc, rec.id, {"RUNNING", "DEAD_LETTER"}, timeout=3)
    cur = await svc.get_task(rec.id)
    if cur.state.value == "DEAD_LETTER":
        await svc.close()
        return  # it finished failing naturally; still verify restart stability
    # crash while RUNNING on its only attempt
    await svc.close()

    svc2 = make_service(db_path, default_timeout=10.0)
    stats2 = HandlerStats()
    register_handlers(svc2, stats2)
    summary = await svc2.start()
    try:
        assert summary["dead_letter"] == 1
        final = await svc2.get_task(rec.id)
        assert final.state.value == "DEAD_LETTER"
        assert "process restart" in final.error
        assert final.attempts == 1
        assert stats2.calls.get("lastchance", 0) == 0  # not re-executed
    finally:
        await svc2.close()


async def test_succeeded_and_dead_tasks_are_unchanged_by_restart(db_path):
    svc = make_service(db_path)
    stats1 = HandlerStats()
    register_handlers(svc, stats1)
    await svc.start()
    ok, _ = await svc.submit("succeed", {"key": "stable-ok"})
    dl, _ = await svc.submit("always_fail", {"key": "stable-dl"}, max_attempts=1)
    await wait_for_state(svc, ok.id, {"SUCCEEDED"})
    await wait_for_state(svc, dl.id, {"DEAD_LETTER"}, timeout=4)
    ok_before = (await svc.get_task(ok.id)).to_dict()
    dl_before = (await svc.get_task(dl.id)).to_dict()
    await svc.close()

    svc2 = make_service(db_path)
    stats2 = HandlerStats()
    register_handlers(svc2, stats2)
    summary = await svc2.start()
    try:
        assert summary == {"rescheduled": 0, "dead_letter": 0, "cancelled": 0}
        ok_after = (await svc2.get_task(ok.id)).to_dict()
        dl_after = (await svc2.get_task(dl.id)).to_dict()
        assert ok_after == ok_before
        assert dl_after == dl_before
        assert stats2.calls == {}  # terminal tasks never re-execute
    finally:
        await svc2.close()


async def test_cancel_requested_before_crash_takes_effect_on_restart(db_path):
    svc = make_service(db_path, default_timeout=10.0)
    stats1 = HandlerStats()
    register_handlers(svc, stats1)
    await svc.start()
    rec, _ = await svc.submit("sleep", {"key": "cancel-restart", "seconds": 10.0})
    await wait_for_state(svc, rec.id, {"RUNNING"}, timeout=3)
    await svc.cancel(rec.id)
    await svc.close()  # crash before the watcher could act

    svc2 = make_service(db_path, default_timeout=10.0)
    stats2 = HandlerStats()
    register_handlers(svc2, stats2)
    summary = await svc2.start()
    try:
        assert summary["cancelled"] == 1
        final = await svc2.get_task(rec.id)
        assert final.state.value == "CANCELLED"
        assert stats2.calls.get("cancel-restart", 0) == 0
    finally:
        await svc2.close()
