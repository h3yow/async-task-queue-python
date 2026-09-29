"""流程（依赖编排）场景测试。

覆盖需求:
- 依赖满足才执行：BLOCKED 任务不被调度、不占并发名额；
- 上游失败/取消/死信时下游被带停（SKIPPED），原因与来源上游可区分，且可级联；
- 流程整体进度与各任务明细可查；
- 进程重启后流程继续推进，已成功的上游不重复执行；
- 提交流程时的参数校验（环/未知依赖/重复 key 等）；
- 普通单任务行为不受影响（由既有测试套件保证不回归）。
"""
from __future__ import annotations

import asyncio
import dataclasses
import os
import time

import pytest

from taskqueue.errors import UnknownFlowError, ValidationError
from taskqueue.models import FlowState, FlowTaskSpec, Task, TaskState
from taskqueue.retry import RetryPolicy
from taskqueue.service import TaskService
from taskqueue.store import TaskStore
from tests.helpers import CallRecorder, register_handlers, wait_for_state


@pytest.fixture
def recorder(tmp_path):
    return CallRecorder(os.path.join(str(tmp_path), "calls.log"))


@pytest.fixture
async def started(service, registry, recorder):
    register_handlers(registry, recorder)
    await service.start()
    yield service
    await service.stop()


def spec(key: str, task_type: str = "succeed", **kw) -> FlowTaskSpec:
    return FlowTaskSpec(key=key, task_type=task_type, **kw)


def by_key(flow) -> dict[str, Task]:
    return {t.flow_key: t for t in flow.tasks}


async def wait_for_flow(service, flow_id, states, timeout=5.0):
    target = states if isinstance(states, (set, tuple, list)) else (states,)
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        flow = await service.get_flow(flow_id)
        if flow.state in target:
            return flow
        if loop.time() > deadline:
            raise AssertionError(
                f"flow {flow_id} did not reach {target} within {timeout}s; "
                f"state={flow.state} "
                f"tasks={[(t.flow_key, t.state.value) for t in flow.tasks]}"
            )
        await asyncio.sleep(0.01)


# ---------- 依赖满足才执行 ----------


async def test_flow_executes_in_dependency_order(started):
    flow = await started.submit_flow([
        spec("a"), spec("b", depends_on=["a"]), spec("c", depends_on=["b"]),
    ])
    tasks = by_key(flow)
    assert tasks["a"].state == TaskState.PENDING
    assert tasks["b"].state == TaskState.BLOCKED
    assert tasks["c"].state == TaskState.BLOCKED

    done = await wait_for_flow(started, flow.flow_id, FlowState.SUCCEEDED)
    t = by_key(done)
    assert all(x.state == TaskState.SUCCEEDED for x in t.values())
    assert all(x.attempts == 1 for x in t.values())
    # 下游只在上游落终态之后才开始
    assert t["b"].started_at >= t["a"].finished_at
    assert t["c"].started_at >= t["b"].finished_at


async def test_diamond_flow_waits_for_all_upstreams(started):
    flow = await started.submit_flow([
        spec("a"),
        spec("b", depends_on=["a"]),
        spec("c", depends_on=["a"]),
        spec("d", depends_on=["b", "c"]),
    ])
    done = await wait_for_flow(started, flow.flow_id, FlowState.SUCCEEDED)
    t = by_key(done)
    assert t["d"].state == TaskState.SUCCEEDED
    assert t["d"].started_at >= t["b"].finished_at
    assert t["d"].started_at >= t["c"].finished_at


async def test_blocked_task_does_not_hold_concurrency_slot(
    config, registry, recorder, logger
):
    """并发额度为 1 时，BLOCKED 任务不得堵住同流程内可运行的任务。"""
    register_handlers(registry, recorder)
    cfg = dataclasses.replace(config, max_concurrency=1)
    svc = TaskService(
        TaskStore(cfg.db_path), registry, RetryPolicy(cfg.retry), cfg, logger
    )
    await svc.start()
    try:
        flow = await svc.submit_flow([spec("a"), spec("b", depends_on=["a"])])
        standalone = await svc.submit("succeed", {"s": 1})
        # 若 BLOCKED 占用并发名额，这里会死锁直到超时
        done = await wait_for_flow(svc, flow.flow_id, FlowState.SUCCEEDED)
        assert all(t.state == TaskState.SUCCEEDED for t in done.tasks)
        await wait_for_state(svc, standalone.id, TaskState.SUCCEEDED)
    finally:
        await svc.stop()


# ---------- 上游未成功 -> 下游带停 ----------


async def test_upstream_failure_skips_downstream_with_reason(started, recorder):
    flow = await started.submit_flow([
        spec("a", "non_retryable"),
        spec("b", depends_on=["a"]),
        spec("c", depends_on=["b"]),
    ])
    done = await wait_for_flow(started, flow.flow_id, FlowState.FAILED)
    t = by_key(done)

    assert t["a"].state == TaskState.FAILED
    assert t["a"].error_kind == "NON_RETRYABLE"

    # 直接被失败上游带停：来源与原因可区分
    assert t["b"].state == TaskState.SKIPPED
    assert t["b"].blocked_by == t["a"].id
    assert t["b"].error_kind == "UPSTREAM_FAILED"
    assert t["b"].finished_at is not None
    assert t["b"].attempts == 0  # 从未执行

    # 级联：被跳过的上游同样带停下游
    assert t["c"].state == TaskState.SKIPPED
    assert t["c"].blocked_by == t["b"].id
    assert t["c"].error_kind == "UPSTREAM_SKIPPED"

    # 下游处理器从未被执行
    assert recorder.count("succeed") == 0


async def test_upstream_dead_letter_skips_downstream(started):
    flow = await started.submit_flow([
        spec("a", "always_retry_fail", max_attempts=2),
        spec("b", depends_on=["a"]),
    ])
    done = await wait_for_flow(started, flow.flow_id, FlowState.FAILED)
    t = by_key(done)
    assert t["a"].state == TaskState.DEAD_LETTER
    assert t["a"].attempts == 2
    assert t["b"].state == TaskState.SKIPPED
    assert t["b"].blocked_by == t["a"].id
    assert t["b"].error_kind == "UPSTREAM_DEAD_LETTER"


async def test_upstream_cancelled_while_running_skips_downstream(
    service, registry, recorder
):
    gate = asyncio.Event()
    register_handlers(registry, recorder, gates={"slow_started": gate})
    await service.start()
    try:
        flow = await service.submit_flow([
            spec("a", "slow"), spec("b", depends_on=["a"]),
        ])
        t0 = by_key(flow)
        await asyncio.wait_for(gate.wait(), timeout=2.0)  # a 已开始执行

        cancelled = await service.cancel(t0["a"].id)
        assert cancelled.state in (TaskState.RUNNING, TaskState.CANCELLED)

        done = await wait_for_flow(service, flow.flow_id, FlowState.CANCELLED)
        t = by_key(done)
        assert t["a"].state == TaskState.CANCELLED
        assert t["b"].state == TaskState.SKIPPED
        assert t["b"].blocked_by == t["a"].id
        assert t["b"].error_kind == "UPSTREAM_CANCELLED"
    finally:
        await service.stop()


async def test_cancel_blocked_task_directly(service, registry, recorder):
    gate = asyncio.Event()
    register_handlers(registry, recorder, gates={"slow_started": gate})
    await service.start()
    try:
        flow = await service.submit_flow([
            spec("a", "slow"), spec("b", depends_on=["a"]),
        ])
        t0 = by_key(flow)
        await asyncio.wait_for(gate.wait(), timeout=2.0)

        # b 还在 BLOCKED：可以直接取消，且不算成功
        cancelled = await service.cancel(t0["b"].id)
        assert cancelled.state == TaskState.CANCELLED
        assert cancelled.finished_at is not None

        await service.cancel(t0["a"].id)  # 收尾，让 a 退出
        done = await wait_for_flow(service, flow.flow_id, FlowState.CANCELLED)
        t = by_key(done)
        assert t["b"].state == TaskState.CANCELLED
        assert t["a"].state == TaskState.CANCELLED
    finally:
        await service.stop()


# ---------- 流程进度视图 ----------


async def test_flow_progress_view_mid_flight(service, registry, recorder):
    gate = asyncio.Event()
    register_handlers(registry, recorder, gates={"slow_started": gate})
    await service.start()
    try:
        flow = await service.submit_flow([
            spec("a", "slow"), spec("b", depends_on=["a"]),
        ])
        await asyncio.wait_for(gate.wait(), timeout=2.0)

        mid = await service.get_flow(flow.flow_id)
        assert mid.state == FlowState.RUNNING
        assert mid.total == 2
        t = by_key(mid)
        assert t["a"].state == TaskState.RUNNING
        assert t["a"].attempts == 1
        assert t["b"].state == TaskState.BLOCKED
        assert t["b"].attempts == 0

        await service.cancel(t["a"].id)
        done = await wait_for_flow(service, flow.flow_id, FlowState.CANCELLED)
        assert by_key(done)["b"].state == TaskState.SKIPPED
    finally:
        await service.stop()


async def test_get_unknown_flow_rejected(started):
    with pytest.raises(UnknownFlowError):
        await started.get_flow("no-such-flow")


# ---------- 提交校验 ----------


async def test_flow_validation_rejects_bad_submissions(started):
    with pytest.raises(ValidationError):
        await started.submit_flow([])
    with pytest.raises(ValidationError):  # 重复 key
        await started.submit_flow([spec("a"), spec("a")])
    with pytest.raises(ValidationError):  # 依赖未知 key
        await started.submit_flow([spec("a", depends_on=["ghost"])])
    with pytest.raises(ValidationError):  # 自依赖
        await started.submit_flow([spec("a", depends_on=["a"])])
    with pytest.raises(ValidationError):  # 环
        await started.submit_flow([
            spec("a", depends_on=["b"]), spec("b", depends_on=["a"]),
        ])
    with pytest.raises(ValidationError):  # 未注册类型
        await started.submit_flow([spec("a", "not-registered")])


async def test_plain_task_has_no_flow_fields(started):
    task = await started.submit("succeed", {"v": 1})
    assert task.flow_id is None
    assert task.flow_key is None
    done = await wait_for_state(started, task.id, TaskState.SUCCEEDED)
    assert done.result == {"echo": {"v": 1}}


# ---------- 重启恢复 ----------


def _register_tracked(registry, recorder):
    """按 payload["name"] 计数的处理器，用于跨“进程”断言执行次数。"""

    async def tracked(payload, ctx):
        await recorder.bump(payload["name"])
        return {"done": payload["name"]}

    registry.register("tracked", tracked)


def _seed_flow(db_path: str, tasks: list[Task], deps: list[tuple[str, str]]):
    store = TaskStore(db_path)
    store.connect()
    store.insert_flow(tasks, deps)
    store.close()


def _seeded_task(
    task_id: str, key: str, state: TaskState, flow_id: str, now: float,
    **kw,
) -> Task:
    fields: dict = {
        "id": task_id, "task_type": "tracked", "payload": {"name": key},
        "idempotency_key": None, "state": state, "priority": 0, "attempts": 0,
        "max_attempts": 3, "timeout": 5.0, "not_before": now,
        "created_at": now, "updated_at": now, "flow_id": flow_id,
        "flow_key": key,
    }
    fields.update(kw)
    return Task(**fields)


async def test_restart_continues_flow_without_rerunning_succeeded(
    service, registry, recorder, config
):
    """崩溃发生在“上游已成功、传播未完成”之间：重启后放行下游且不重跑上游。"""
    _register_tracked(registry, recorder)
    now = time.time()
    _seed_flow(
        config.db_path,
        [
            _seeded_task("t-a", "a", TaskState.SUCCEEDED, "flow-1", now,
                         finished_at=now, result={"done": "a"}),
            _seeded_task("t-b", "b", TaskState.BLOCKED, "flow-1", now),
            _seeded_task("t-e", "e", TaskState.PENDING, "flow-1", now),
            _seeded_task("t-f", "f", TaskState.BLOCKED, "flow-1", now),
        ],
        [("t-b", "t-a"), ("t-f", "t-e")],
    )

    await service.start()
    try:
        done = await wait_for_flow(service, "flow-1", FlowState.SUCCEEDED)
        assert all(t.state == TaskState.SUCCEEDED for t in done.tasks)
    finally:
        await service.stop()

    # 已成功的 a 绝不重跑；未完成的 b/e/f 各执行一次
    assert recorder.count("a") == 0
    assert recorder.count("b") == 1
    assert recorder.count("e") == 1
    assert recorder.count("f") == 1


async def test_restart_skips_downstream_of_failed_upstream(
    service, registry, recorder, config
):
    """崩溃发生在“上游已失败、传播未完成”之间：重启后下游被带停且原因可查。"""
    _register_tracked(registry, recorder)
    now = time.time()
    _seed_flow(
        config.db_path,
        [
            _seeded_task("t-h", "h", TaskState.FAILED, "flow-2", now,
                         finished_at=now, last_error="boom",
                         error_kind="NON_RETRYABLE"),
            _seeded_task("t-i", "i", TaskState.BLOCKED, "flow-2", now),
        ],
        [("t-i", "t-h")],
    )

    await service.start()
    try:
        done = await wait_for_flow(service, "flow-2", FlowState.FAILED)
        t = by_key(done)
        assert t["i"].state == TaskState.SKIPPED
        assert t["i"].blocked_by == "t-h"
        assert t["i"].error_kind == "UPSTREAM_FAILED"
    finally:
        await service.stop()

    assert recorder.count("i") == 0  # 下游从未执行


async def test_restart_recovers_running_upstream_and_continues(
    service, registry, recorder, config
):
    """上游在执行中崩溃：孤儿恢复重跑成功后，下游继续推进。"""
    _register_tracked(registry, recorder)
    now = time.time()
    _seed_flow(
        config.db_path,
        [
            _seeded_task(
                "t-j", "j", TaskState.RUNNING, "flow-3", now,
                attempts=1, lease_owner="dead-process",
                lease_expires_at=now + 999,
            ),
            _seeded_task("t-k", "k", TaskState.BLOCKED, "flow-3", now),
        ],
        [("t-k", "t-j")],
    )

    await service.start()
    try:
        done = await wait_for_flow(service, "flow-3", FlowState.SUCCEEDED)
        t = by_key(done)
        assert t["j"].state == TaskState.SUCCEEDED
        assert t["k"].state == TaskState.SUCCEEDED
        assert t["k"].started_at >= t["j"].finished_at
    finally:
        await service.stop()

    # 崩溃的那次尝试不算执行；新进程各跑一次
    assert recorder.count("j") == 1
    assert recorder.count("k") == 1
