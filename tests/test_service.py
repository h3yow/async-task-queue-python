"""任务编排核心场景。

覆盖需求:
- 提交 / 查询 / 取消，未知任务与非法状态转换被拒绝且原因可区分；
- 同一幂等键并发提交不产生重复执行；
- 可重试失败按退避重试后成功；
- 重试耗尽进入死信，可查询原因与尝试次数；
- 不可重试错误立即 FAILED。
"""
from __future__ import annotations

import asyncio
import os

import pytest

from taskqueue.errors import (
    IdempotencyConflictError,
    InvalidStateTransitionError,
    UnknownTaskError,
    ValidationError,
)
from taskqueue.models import TaskState
from tests.helpers import CallRecorder, register_handlers, wait_for_state


@pytest.fixture
def recorder(tmp_path):
    rec = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
    return rec


@pytest.fixture
async def started(service, registry, recorder):
    register_handlers(registry, recorder)
    await service.start()
    yield service
    await service.stop()


# ---------- 基础：提交 / 查询 / 成功 ----------


async def test_submit_and_succeed(started):
    task = await started.submit("succeed", {"v": 1})
    assert task.state == TaskState.PENDING
    done = await wait_for_state(started, task.id, TaskState.SUCCEEDED)
    assert done.result == {"echo": {"v": 1}}
    assert done.attempts == 1
    assert done.finished_at is not None


async def test_get_unknown_task_rejected(started):
    with pytest.raises(UnknownTaskError):
        await started.get_task("does-not-exist")


async def test_submit_unknown_type_rejected(started):
    with pytest.raises(ValidationError):
        await started.submit("not-registered", {})


# ---------- 幂等 ----------


async def test_idempotent_serial_submit_runs_once(started, recorder):
    a = await started.submit("succeed", {"x": 1}, idempotency_key="key-1")
    b = await started.submit("succeed", {"x": 1}, idempotency_key="key-1")
    assert a.id == b.id
    await wait_for_state(started, a.id, TaskState.SUCCEEDED)
    # 等待足够时间确认只执行一次
    await asyncio.sleep(0.05)
    assert recorder.count("succeed") == 1


async def test_idempotent_conflicting_payload_rejected(started):
    a = await started.submit("succeed", {"x": 1}, idempotency_key="key-2")
    with pytest.raises(IdempotencyConflictError) as ei:
        await started.submit("succeed", {"x": 2}, idempotency_key="key-2")
    assert ei.value.existing_task_id == a.id


async def test_concurrent_same_idempotency_key_runs_once(started, recorder):
    # 同一幂等键并发提交：必须只有一个任务被创建、执行一次
    results = await asyncio.gather(*[
        started.submit("succeed", {"same": True}, idempotency_key="race-key")
        for _ in range(20)
    ])
    ids = {t.id for t in results}
    assert len(ids) == 1, f"expected single task, got {ids}"
    tid = ids.pop()
    await wait_for_state(started, tid, TaskState.SUCCEEDED)
    await asyncio.sleep(0.05)
    assert recorder.count("succeed") == 1


async def test_concurrent_distinct_keys_all_run(started, recorder):
    # 不同幂等键互不影响
    results = await asyncio.gather(*[
        started.submit("succeed", {"i": i}, idempotency_key=f"k-{i}")
        for i in range(10)
    ])
    assert len({t.id for t in results}) == 10
    for t in results:
        await wait_for_state(started, t.id, TaskState.SUCCEEDED)
    await asyncio.sleep(0.05)
    assert recorder.count("succeed") == 10


# ---------- 重试 ----------


async def test_retry_then_success(started, recorder):
    task = await started.submit("flaky", {"fail_times": 2})
    done = await wait_for_state(
        started, task.id, [TaskState.SUCCEEDED, TaskState.DEAD_LETTER]
    )
    assert done.state == TaskState.SUCCEEDED
    assert done.attempts == 3  # 两次失败 + 一次成功
    assert done.result == {"attempt": 3}
    assert recorder.count("flaky") == 3


async def test_retry_waits_use_backoff(started):
    # 失败后应进入 RETRY_WAIT 并带有未来的 not_before
    import time
    task = await started.submit("flaky", {"fail_times": 5}, max_attempts=5)
    await wait_for_state(started, task.id, [TaskState.RETRY_WAIT, TaskState.SUCCEEDED])
    waiting = await started.get_task(task.id)
    if waiting.state == TaskState.RETRY_WAIT:
        assert waiting.not_before > time.time()
        assert waiting.attempts == 1
        assert waiting.error_kind == "RETRYABLE"
        assert "transient" in (waiting.last_error or "")


# ---------- 死信 ----------


async def test_retries_exhausted_go_to_dead_letter(started, recorder):
    task = await started.submit("always_retry_fail", {}, max_attempts=3)
    dead = await wait_for_state(started, task.id, TaskState.DEAD_LETTER)
    assert dead.attempts == 3
    assert dead.error_kind == "RETRYABLE"
    assert "persistent failure" in dead.last_error
    assert recorder.count("always_retry_fail") == 3
    # 死信为终态，等待后不再被执行
    await asyncio.sleep(0.1)
    assert recorder.count("always_retry_fail") == 3


async def test_dead_letter_queryable_reason_and_attempts(started):
    task = await started.submit("always_retry_fail", {}, max_attempts=2)
    dead = await wait_for_state(started, task.id, TaskState.DEAD_LETTER)
    assert dead.attempts == 2
    # 原因可区分：error_kind 与 last_error 均可查询
    assert dead.error_kind == "RETRYABLE"
    assert dead.last_error


async def test_non_retryable_fails_immediately(started, recorder):
    task = await started.submit("non_retryable", {})
    done = await wait_for_state(started, task.id, TaskState.FAILED)
    assert done.attempts == 1
    assert done.error_kind == "NON_RETRYABLE"
    assert "bad request" in done.last_error
    await asyncio.sleep(0.05)
    assert recorder.count("non_retryable") == 1


# ---------- 取消与非法状态 ----------


async def test_cancel_running_task(started):
    gates = {"slow_started": asyncio.Event()}
    # 重新注册 slow 以便共享 gate：直接用 service 的 registry 覆盖注册
    register_handlers(started.registry, CallRecorder(
        os.path.join(os.path.dirname(started.config.db_path), "calls2.log")),
        gates=gates)
    task = await started.submit("slow", {})
    await asyncio.wait_for(gates["slow_started"].wait(), 2)
    running = await started.get_task(task.id)
    assert running.state == TaskState.RUNNING

    await started.cancel(task.id)
    cancelled = await wait_for_state(started, task.id, TaskState.CANCELLED)
    assert cancelled.error_kind == "CANCELLED"


async def test_cancel_terminal_task_rejected(started):
    task = await started.submit("succeed", {})
    await wait_for_state(started, task.id, TaskState.SUCCEEDED)
    with pytest.raises(InvalidStateTransitionError) as ei:
        await started.cancel(task.id)
    assert ei.value.current_state == "SUCCEEDED"
    assert ei.value.operation == "cancel"


async def test_cancel_unknown_task_rejected(started):
    with pytest.raises(UnknownTaskError):
        await started.cancel("ghost-id")
