"""并发上限、优先级调度、超时与资源释放。"""
from __future__ import annotations

import asyncio
import os

from taskqueue.config import RetryConfig, TaskQueueConfig
from taskqueue.models import TaskState
from taskqueue.retry import RetryPolicy
from taskqueue.service import TaskService
from taskqueue.store import TaskStore
from tests.helpers import wait_for_state


def _service(db_path, registry, max_concurrency=2, timeout=5.0, max_attempts=1):
    import logging
    cfg = TaskQueueConfig(
        db_path=db_path, max_concurrency=max_concurrency, poll_interval=0.005,
        default_task_timeout=timeout,
        retry=RetryConfig(max_attempts=max_attempts, base_delay=0.01),
    )
    log = logging.getLogger("taskqueue.sched")
    log.setLevel(logging.CRITICAL)
    log.propagate = False
    return TaskService(TaskStore(db_path), registry, RetryPolicy(cfg.retry),
                       cfg, log)


async def test_concurrency_is_capped_and_slow_tasks_do_not_block(tmp_path, registry):
    db_path = os.path.join(str(tmp_path), "s.db")
    active = peak = 0
    gate = asyncio.Event()

    async def blocker(payload, ctx):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.wait_for(gate.wait(), timeout=5)
        except TimeoutError:
            pass
        active -= 1
        return "blocked"

    quick_ran = asyncio.Event()

    async def quick(payload, ctx):
        quick_ran.set()
        return "quick"

    registry.register("blocker", blocker)
    registry.register("quick", quick)

    svc = _service(db_path, registry, max_concurrency=2)
    await svc.start()
    try:
        blockers = [await svc.submit("blocker", {}) for _ in range(4)]
        await asyncio.sleep(0.15)
        assert peak == 2, f"concurrency exceeded cap: {peak}"
        assert active == 2

        # 高优先级 quick 排在 blocker 之后；放行后必须能被执行（不被永久阻塞）
        q = await svc.submit("quick", {}, priority=100)
        gate.set()
        qt = await wait_for_state(svc, q.id, TaskState.SUCCEEDED)
        assert qt.result == "quick"
        assert quick_ran.is_set()

        for b in blockers:
            bt = await wait_for_state(svc, b.id, TaskState.SUCCEEDED)
            assert bt.state == TaskState.SUCCEEDED
    finally:
        await svc.stop()


async def test_priority_ordering(tmp_path, registry):
    db_path = os.path.join(str(tmp_path), "p.db")
    order: list[int] = []
    release = asyncio.Event()
    n_running = 0
    reached_two = asyncio.Event()

    def make_handler(prio):
        async def handler(payload, ctx):
            nonlocal n_running
            n_running += 1
            if n_running >= 2:
                reached_two.set()
            # 前两个占住并发槽；后续任务排队等放行，放行后按优先级领取
            await release.wait()
            order.append(prio)
            n_running -= 1
            return prio
        return handler

    for p in (0, 5, 10):
        registry.register(f"job{p}", make_handler(p))

    svc = _service(db_path, registry, max_concurrency=2)
    await svc.start()
    try:
        # 先占满 2 个并发槽（两个 job0）
        holder1 = await svc.submit("job0", {})
        holder2 = await svc.submit("job0", {})
        await asyncio.wait_for(reached_two.wait(), 2)

        # 槽满后，再按“低优先级先提交、高优先级后提交”入队
        t5 = await svc.submit("job5", {}, priority=5)
        t10 = await svc.submit("job10", {}, priority=10)

        # 必须确认两个排队任务都已持久化为 PENDING（在队列中）后，
        # 再释放槽位，避免与调度器唤醒产生 FIFO/优先级竞态
        async def _both_queued():
            while True:
                a = await svc.get_task(t5.id)
                b = await svc.get_task(t10.id)
                if a.state == TaskState.PENDING and b.state == TaskState.PENDING:
                    return
                await asyncio.sleep(0.005)
        await asyncio.wait_for(_both_queued(), 2)

        release.set()
        for t in (holder1, holder2, t5, t10):
            await wait_for_state(svc, t.id, TaskState.SUCCEEDED)

        # 前两个完成顺序不定（都是 0），但排队的两个必须 10 先于 5
        queued = [p for p in order if p in (5, 10)]
        assert queued == [10, 5], order
    finally:
        await svc.stop()


async def test_timeout_releases_slot_and_goes_dead_letter(tmp_path, registry):
    db_path = os.path.join(str(tmp_path), "t.db")

    async def hangs(payload, ctx):
        await asyncio.sleep(10)

    after_done = asyncio.Event()

    async def after(payload, ctx):
        after_done.set()
        return "after"

    registry.register("hangs", hangs)
    registry.register("after", after)

    # max_attempts=1 => 超时即可重试失败，直接死信
    svc = _service(db_path, registry, max_concurrency=1, timeout=0.2,
                   max_attempts=1)
    await svc.start()
    try:
        h = await svc.submit("hangs", {})
        dead = await wait_for_state(svc, h.id,
                                    [TaskState.DEAD_LETTER, TaskState.FAILED])
        assert dead.state == TaskState.DEAD_LETTER
        assert dead.error_kind == "TIMEOUT"

        # 超时后并发槽必须被释放：后续任务可执行
        a = await svc.submit("after", {})
        at = await wait_for_state(svc, a.id, TaskState.SUCCEEDED)
        assert at.result == "after"
        assert after_done.is_set()
    finally:
        await svc.stop()
