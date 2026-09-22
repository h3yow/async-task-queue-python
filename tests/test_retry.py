"""Retry-then-success and retry-exhaustion/dead-letter semantics."""

from __future__ import annotations

import logging

from conftest import wait_for_state


async def test_succeeds_after_retries(service):
    svc, stats = service
    rec, _ = await svc.submit(
        "flaky", {"key": "flaky-ok", "fail_times": 2}, max_attempts=3
    )
    final = await wait_for_state(
        svc, rec.id, {"SUCCEEDED", "DEAD_LETTER"}, timeout=5
    )
    assert final.state.value == "SUCCEEDED"
    assert final.attempts == 3, "attempt counter reflects all physical tries"
    assert stats.calls["flaky-ok"] == 3
    assert final.error is None
    assert final.result is not None


async def test_one_shot_failure_goes_dead_letter(service):
    svc, stats = service
    rec, _ = await svc.submit(
        "always_fail", {"key": "boom1"}, max_attempts=1
    )
    final = await wait_for_state(svc, rec.id, {"DEAD_LETTER"})
    assert final.attempts == 1
    assert stats.calls["boom1"] == 1
    assert "RuntimeError" in final.error
    assert "attempts exhausted: 1/1" in final.error

    dls = await svc.list_dead_letters()
    assert any(d.id == rec.id for d in dls)


async def test_exhausted_retries_record_reason_and_attempts(service, caplog):
    svc, stats = service
    rec, _ = await svc.submit(
        "always_fail", {"key": "boom3"}, max_attempts=3
    )
    with caplog.at_level(logging.WARNING, logger="taskqueue.worker"):
        final = await wait_for_state(svc, rec.id, {"DEAD_LETTER"}, timeout=8)

    assert final.attempts == 3
    assert stats.calls["boom3"] == 3
    assert "attempts exhausted: 3/3" in final.error

    # Logs must contain task id, attempt counts and the decision basis.
    messages = [r.getMessage() for r in caplog.records]
    retry_logs = [m for m in messages if "state=SCHEDULED" in m]
    dl_logs = [m for m in messages if "state=DEAD_LETTER" in m]
    assert len(retry_logs) == 2  # retries after attempt 1 and 2
    assert all(rec.id in m for m in retry_logs + dl_logs)
    assert all("basis=retryable_failure" in m for m in retry_logs)
    assert any("basis=attempts_exhausted" in m and "attempt=3/3" in m for m in dl_logs)


async def test_scheduling_error_and_running_failure_do_not_succeed(service):
    svc, _stats = service
    # Every failure path must keep the task out of SUCCEEDED.
    rec, _ = await svc.submit(
        "always_fail", {"key": "never"}, max_attempts=2
    )
    final = await wait_for_state(svc, rec.id, {"DEAD_LETTER"}, timeout=6)
    assert final.state.value == "DEAD_LETTER"
    assert final.finished_at is not None
