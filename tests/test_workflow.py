"""带依赖的流程编排测试。

覆盖需求:
- 依赖全部成功后下游才执行；未满足的任务 BLOCKED、不占并发名额、不堵住同流程其他任务；
- 上游失败/死信/取消时，未开始的下游被带停为 SKIPPED，且能查到直接来源与可区分原因；
- 流程整体进度视图（状态计数、逐任务状态/次数/结束方式）；
- 结构性校验（重复 key、未知依赖、环）被拒绝；
- 不带依赖的普通任务行为不变。
"""
from __future__ import annotations

import asyncio
import os

import pytest

from taskqueue.errors import (
    IdempotencyConflictError,
    ValidationError,
)
from taskqueue.models import TaskState
from taskqueue.workflow import WorkflowSpec, WorkflowTaskSpec
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


def _spec(tasks, name=None):
    return WorkflowSpec(
        name=name,
        tasks=[WorkflowTaskSpec(**t) for t in tasks],
    )


async def _wait_workflow(svc, wf_id, timeout=5.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        view = await svc.get_workflow(wf_id)
        if view.state != "RUNNING":
            return view
        if loop.time() > deadline:
            raise AssertionError(
                f"workflow {wf_id} did not finish; counts={view.state_counts}"
            )
        await asyncio.sleep(0.01)


def _by_key(view):
    return {tv.task.task_key: tv for tv in view.tasks}


# ---------- 依赖满足才执行 ----------


async def test_diamond_workflow_runs_in_dependency_order(started):
    # a 成功后 b、c 并行，d 等 b、c 全部成功
    view0 = await started.submit_workflow(_spec([
        {"key": "a", "task_type": "succeed", "payload": {"n": 1}},
        {"key": "b", "task_type": "succeed", "depends_on": ["a"]},
        {"key": "c", "task_type": "succeed", "depends_on": ["a"]},
        {"key": "d", "task_type": "succeed", "depends_on": ["b", "c"]},
    ]))
    wf_id = view0.workflow.id

    # 初始：a 可跑，b/c/d 阻塞
    assert view0.state_counts.get("PENDING") == 1
    assert view0.state_counts.get("BLOCKED") == 3

    view = await _wait_workflow(started, wf_id)
    assert view.state == "SUCCEEDED"
    assert view.state_counts == {"SUCCEEDED": 4}
    nodes = _by_key(view)
    assert nodes["a"].task.result == {"echo": {"n": 1}}
    assert nodes["d"].depends_on == ["b", "c"]
    for key in ("a", "b", "c", "d"):
        assert nodes[key].task.state == TaskState.SUCCEEDED
        assert nodes[key].task.attempts == 1


async def test_downstream_not_runnable_until_all_upstreams_succeed(started):
    # 两个上游：a 立即成功，hold 被 gate 卡住；d 必须等两者都成功
    gate = asyncio.Event()

    async def hold(payload, ctx):
        await gate.wait()
        return "held"

    started.registry.register("hold", hold)

    view0 = await started.submit_workflow(_spec([
        {"key": "a", "task_type": "succeed"},
        {"key": "hold", "task_type": "hold"},
        {"key": "d", "task_type": "succeed", "depends_on": ["a", "hold"]},
    ]))
    nodes0 = _by_key(view0)
    assert nodes0["d"].task.state == TaskState.BLOCKED

    # a 先完成，但 hold 未完成 -> d 仍 BLOCKED
    await wait_for_state(started, nodes0["a"].task.id, TaskState.SUCCEEDED)
    await asyncio.sleep(0.1)
    mid = await started.get_workflow(view0.workflow.id)
    assert _by_key(mid)["d"].task.state == TaskState.BLOCKED

    gate.set()
    view = await _wait_workflow(started, view0.workflow.id)
    assert _by_key(view)["d"].task.state == TaskState.SUCCEEDED


async def test_blocked_tasks_do_not_consume_concurrency(started):
    # 自包含的阻塞处理器：用 started 计数与 barrier 精确等待“4 个槽全部占满”，
    # 避免复用共享 slow gate 带来的跨测试残留与时序竞态。
    max_conc = started.config.max_concurrency
    active = 0
    all_blockers_started = asyncio.Event()
    release = asyncio.Event()

    async def blocker(payload, ctx):
        nonlocal active
        active += 1
        if active == max_conc:
            all_blockers_started.set()
        try:
            await asyncio.wait_for(release.wait(), timeout=5)
        except TimeoutError:
            pass
        active -= 1
        return "blocked"

    quick_done = asyncio.Event()

    async def quick(payload, ctx):
        quick_done.set()
        return "quick"

    started.registry.register("blocker", blocker)
    started.registry.register("quick", quick)

    # 占满全部并发槽的 blocker，外加一个依赖 b0 的 BLOCKED 下游
    view0 = await started.submit_workflow(_spec([
        {"key": f"b{i}", "task_type": "blocker"} for i in range(max_conc)
    ] + [
        {"key": "blocked", "task_type": "quick", "depends_on": ["b0"]},
    ]))
    await asyncio.wait_for(all_blockers_started.wait(), 2)

    # 此时无空闲槽：BLOCKED 下游不可被领取（不占名额）；再放一个独立 quick
    standalone = await started.submit("quick", {})
    await asyncio.sleep(0.15)
    pending = await started.get_task(standalone.id)
    assert pending.state == TaskState.PENDING, \
        f"standalone should be queued while slots are full, got {pending.state}"
    assert not quick_done.is_set()

    # 放行 blocker：槽释放后，独立 quick 与（b0 成功后激活的）blocked 都应推进
    release.set()
    await wait_for_state(started, standalone.id, TaskState.SUCCEEDED)
    assert quick_done.is_set()
    view = await _wait_workflow(started, view0.workflow.id)
    assert view.state == "SUCCEEDED"
    assert _by_key(view)["blocked"].task.state == TaskState.SUCCEEDED


async def test_blocked_downstream_is_skipped_when_upstream_cancelled(started):
    # 占满槽位时取消一个尚未开始的流程根任务，其 BLOCKED 下游被带停。
    max_conc = started.config.max_concurrency
    active = 0
    all_started = asyncio.Event()

    async def blocker(payload, ctx):
        nonlocal active
        active += 1
        if active == max_conc:
            all_started.set()
        await asyncio.wait_for(ctx.cancel_event.wait(), timeout=5)
        active -= 1
        return "blocked"

    async def quick(payload, ctx):
        return "quick"

    started.registry.register("blocker", blocker)
    started.registry.register("quick", quick)

    fillers = [await started.submit("blocker", {}) for _ in range(max_conc)]
    await asyncio.wait_for(all_started.wait(), 2)

    view0 = await started.submit_workflow(_spec([
        {"key": "root", "task_type": "quick"},
        {"key": "leaf", "task_type": "quick", "depends_on": ["root"]},
    ]))
    # 无空闲槽，root 应仍排队
    await asyncio.sleep(0.1)
    root_id = _by_key(await started.get_workflow(view0.workflow.id))[
        "root"].task.id
    assert (await started.get_task(root_id)).state == TaskState.PENDING

    # 取消排队中的 root -> leaf 同事务级联 SKIPPED
    await started.cancel(root_id)
    view = await _wait_workflow(started, view0.workflow.id)
    nodes = _by_key(view)
    assert nodes["root"].task.state == TaskState.CANCELLED
    assert nodes["leaf"].task.state == TaskState.SKIPPED
    assert nodes["leaf"].task.error_kind == "UPSTREAM_CANCELLED"
    assert nodes["leaf"].skipped_by_key == "root"

    for f in fillers:
        await started.cancel(f.id)


# ---------- 上游失败 / 死信 / 取消带停下游，原因可区分 ----------


async def test_upstream_failure_skips_downstream_with_reason(started):
    view0 = await started.submit_workflow(_spec([
        {"key": "x", "task_type": "non_retryable"},
        {"key": "y", "task_type": "succeed", "depends_on": ["x"]},
        {"key": "z", "task_type": "succeed", "depends_on": ["y"]},
        # 独立分支不受影响
        {"key": "w", "task_type": "succeed"},
    ]))
    view = await _wait_workflow(started, view0.workflow.id)
    assert view.state == "FAILED"
    nodes = _by_key(view)

    x, y, z, w = (nodes[k].task for k in ("x", "y", "z", "w"))
    assert x.state == TaskState.FAILED
    assert x.error_kind == "NON_RETRYABLE"
    assert y.state == TaskState.SKIPPED
    assert z.state == TaskState.SKIPPED
    # 从未执行
    assert y.attempts == 0 and z.attempts == 0
    assert w.state == TaskState.SUCCEEDED

    # 直接来源与原因都可查询
    assert nodes["y"].skipped_by_key == "x"
    assert y.skipped_by_task_id == x.id
    assert y.error_kind == "UPSTREAM_FAILED"
    assert nodes["z"].skipped_by_key == "y"
    assert z.error_kind == "UPSTREAM_FAILED"


async def test_upstream_dead_letter_skips_with_distinct_kind(started):
    # max_attempts=2 且永远可重试失败 -> DEAD_LETTER
    view0 = await started.submit_workflow(_spec([
        {"key": "root", "task_type": "always_retry_fail", "max_attempts": 2},
        {"key": "child", "task_type": "succeed", "depends_on": ["root"]},
    ]))
    view = await _wait_workflow(started, view0.workflow.id)
    nodes = _by_key(view)
    assert nodes["root"].task.state == TaskState.DEAD_LETTER
    child = nodes["child"].task
    assert child.state == TaskState.SKIPPED
    assert child.error_kind == "UPSTREAM_DEAD_LETTER"
    assert nodes["child"].skipped_by_key == "root"
    assert child.attempts == 0


async def test_upstream_timeout_skips_with_distinct_kind(started):
    async def hangs(payload, ctx):
        await asyncio.sleep(10)

    started.registry.register("hangs", hangs)
    # 超时 0.05s，max_attempts=1：超时一次即进死信（error_kind=TIMEOUT）
    view0 = await started.submit_workflow(_spec([
        {"key": "slow", "task_type": "hangs", "timeout": 0.05,
         "max_attempts": 1},
        {"key": "after", "task_type": "succeed", "depends_on": ["slow"]},
    ]))
    view = await _wait_workflow(started, view0.workflow.id)
    nodes = _by_key(view)
    assert nodes["slow"].task.state == TaskState.DEAD_LETTER
    assert nodes["slow"].task.error_kind == "TIMEOUT"
    assert nodes["after"].task.state == TaskState.SKIPPED
    assert nodes["after"].task.error_kind == "UPSTREAM_TIMEOUT"
    assert nodes["after"].skipped_by_key == "slow"


async def test_cancel_blocked_or_queued_task_cascades_skip(started):
    # a 可跑但阻塞在取消事件上，b 依赖 a；取消运行中的 a 后 b 被带停
    async def hold(payload, ctx):
        await asyncio.wait_for(ctx.cancel_event.wait(), timeout=5)
        return "held"

    started.registry.register("hold", hold)
    view0 = await started.submit_workflow(_spec([
        {"key": "a", "task_type": "hold"},
        {"key": "b", "task_type": "succeed", "depends_on": ["a"]},
    ]))
    a_id = _by_key(view0)["a"].task.id
    # 等 a 进入 RUNNING 再取消（走运行中协作取消）
    await wait_for_state(started, a_id, TaskState.RUNNING)
    await started.cancel(a_id)

    view = await _wait_workflow(started, view0.workflow.id)
    nodes = _by_key(view)
    assert nodes["a"].task.state == TaskState.CANCELLED
    assert nodes["b"].task.state == TaskState.SKIPPED
    assert nodes["b"].task.error_kind == "UPSTREAM_CANCELLED"
    assert nodes["b"].skipped_by_key == "a"


# ---------- 流程进度视图 ----------


async def test_workflow_progress_view_and_unknown_404(started):
    view0 = await started.submit_workflow(_spec(
        [{"key": "a", "task_type": "succeed"}], name="wf-name"
    ))
    assert view0.workflow.name == "wf-name"
    assert view0.state in ("RUNNING", "SUCCEEDED")
    view = await _wait_workflow(started, view0.workflow.id)
    item = _by_key(view)["a"]
    assert item.task.finished_at is not None
    assert item.task.last_error is None

    from taskqueue.errors import UnknownWorkflowError
    with pytest.raises(UnknownWorkflowError):
        await started.get_workflow("no-such-workflow")


# ---------- 结构校验 ----------


async def test_workflow_validation_errors(started):
    # 未知依赖
    with pytest.raises(ValidationError):
        await started.submit_workflow(_spec(
            [{"key": "a", "task_type": "succeed", "depends_on": ["ghost"]}]
        ))
    # 重复 key
    with pytest.raises(ValidationError):
        await started.submit_workflow(_spec([
            {"key": "a", "task_type": "succeed"},
            {"key": "a", "task_type": "succeed"},
        ]))
    # 环
    with pytest.raises(ValidationError):
        await started.submit_workflow(_spec([
            {"key": "a", "task_type": "succeed", "depends_on": ["b"]},
            {"key": "b", "task_type": "succeed", "depends_on": ["a"]},
        ]))
    # 未知处理器类型
    with pytest.raises(ValidationError):
        await started.submit_workflow(_spec(
            [{"key": "a", "task_type": "not-registered"}]
        ))
    # 批内重复幂等键
    with pytest.raises(ValidationError):
        await started.submit_workflow(_spec([
            {"key": "a", "task_type": "succeed", "idempotency_key": "dup"},
            {"key": "b", "task_type": "succeed", "idempotency_key": "dup"},
        ]))


async def test_workflow_idempotency_conflict_with_existing(started):
    # 与库中既有任务（普通任务）撞幂等键且不同参 -> 冲突，整批不落
    await started.submit("succeed", {"x": 1}, idempotency_key="global-key")
    with pytest.raises(IdempotencyConflictError):
        await started.submit_workflow(_spec([
            {"key": "a", "task_type": "succeed", "payload": {"x": 2},
             "idempotency_key": "global-key"},
        ]))


# ---------- 普通任务不回归 ----------


async def test_plain_tasks_without_dependencies_unaffected(started):
    task = await started.submit("succeed", {"v": 9})
    done = await wait_for_state(started, task.id, TaskState.SUCCEEDED)
    assert done.workflow_id is None
    assert done.task_key is None
    assert done.result == {"echo": {"v": 9}}
