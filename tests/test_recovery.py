"""进程重启恢复场景。

模拟“上一个进程”崩溃/被杀死后，数据库中遗留:
- RUNNING 孤儿任务（执行中断，绝不能静默成功）；
- RETRY_WAIT / PENDING 未完成任务（不得丢失）；
随后启动“新进程”（全新 Worker/owner/事件循环，但同一个 db 文件），
断言:
- 孤儿被重新调度并按结果成功或按重试策略处理；
- 执行中断的任务不被静默标记成功；
- 总执行次数不超过 max_attempts；
- 终态任务在再次重启后不被重复执行。
"""
from __future__ import annotations

import os
import time

from taskqueue.config import RetryConfig, TaskQueueConfig
from taskqueue.models import Task, TaskState
from taskqueue.retry import RetryPolicy
from taskqueue.service import TaskService
from taskqueue.store import TaskStore
from tests.helpers import CallRecorder, register_handlers, wait_for_state


def _make_service(db_path: str, registry, recorder, **overrides) -> TaskService:
    cfg = TaskQueueConfig(
        db_path=db_path,
        max_concurrency=overrides.get("max_concurrency", 4),
        poll_interval=0.005,
        default_task_timeout=5.0,
        retry=RetryConfig(
            max_attempts=overrides.get("max_attempts", 3),
            base_delay=0.01,
            multiplier=2.0,
        ),
    )
    import logging
    log = logging.getLogger("taskqueue.recovery")
    log.setLevel(logging.CRITICAL)
    log.propagate = False
    return TaskService(TaskStore(db_path), registry, RetryPolicy(cfg.retry), cfg, log)


def _seed_orphans(db_path: str) -> None:
    """以“旧进程”身份直接写入各种遗留状态。"""
    store = TaskStore(db_path)
    store.connect()
    now = time.time()
    # 执行到一半崩溃：已尝试 1 次，结果未落库
    store.insert_task(Task(
        id="orphan-ok", task_type="succeed", payload={}, idempotency_key=None,
        state=TaskState.RUNNING, priority=0, attempts=1, max_attempts=3,
        timeout=5.0, created_at=now, updated_at=now,
        lease_owner="dead-process", lease_expires_at=now + 999,
    ))
    # 等待退避中的任务，到期后应继续
    store.insert_task(Task(
        id="waiting", task_type="succeed", payload={"w": 1},
        idempotency_key=None, state=TaskState.RETRY_WAIT, priority=0,
        attempts=1, max_attempts=3, timeout=5.0, not_before=now + 0.03,
        created_at=now, updated_at=now, last_error="old",
        error_kind="RETRYABLE",
    ))
    # 崩溃时已用满次数（attempts=3）：恢复回滚为 2，重跑第 3 次仍失败 -> 死信
    store.insert_task(Task(
        id="orphan-dead", task_type="always_retry_fail", payload={},
        idempotency_key=None, state=TaskState.RUNNING, priority=0,
        attempts=3, max_attempts=3, timeout=5.0, created_at=now,
        updated_at=now, lease_owner="dead-process",
        lease_expires_at=now + 999,
    ))
    store.close()


async def test_restart_recovers_orphans_without_silent_success(
    tmp_path, registry
):
    db_path = os.path.join(str(tmp_path), "recovery.db")
    recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
    register_handlers(registry, recorder)
    _seed_orphans(db_path)

    svc = _make_service(db_path, registry, recorder)
    await svc.start()
    try:
        # 孤儿成功任务：崩溃的那次结果丢失，重启后重新执行一次；attempts=1
        ok = await wait_for_state(
            svc, "orphan-ok", [TaskState.SUCCEEDED, TaskState.DEAD_LETTER]
        )
        assert ok.state == TaskState.SUCCEEDED
        assert ok.attempts == 1
        assert ok.result == {"echo": {}}

        # 退避等待中的任务不丢失
        waiting = await wait_for_state(svc, "waiting", TaskState.SUCCEEDED)
        assert waiting.result == {"echo": {"w": 1}}

        # 用满次数的孤儿重新执行最后一次后进入死信，总次数恰为 3
        dead = await wait_for_state(svc, "orphan-dead", TaskState.DEAD_LETTER)
        assert dead.attempts == 3
        assert dead.error_kind == "RETRYABLE"
        assert dead.last_error
    finally:
        await svc.stop()

    # 真实执行计数：succeed = orphan-ok(1) + waiting(1)；dead 仅在新进程跑 1 次
    assert recorder.count("succeed") == 2
    assert recorder.count("always_retry_fail") == 1


async def test_restart_does_not_rerun_terminal_tasks(tmp_path, registry):
    db_path = os.path.join(str(tmp_path), "terminal.db")
    recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
    register_handlers(registry, recorder)
    _seed_orphans(db_path)

    # 第一个进程把所有任务推进到终态
    svc1 = _make_service(db_path, registry, recorder)
    await svc1.start()
    try:
        await wait_for_state(svc1, "orphan-ok", TaskState.SUCCEEDED)
        await wait_for_state(svc1, "waiting", TaskState.SUCCEEDED)
        await wait_for_state(svc1, "orphan-dead", TaskState.DEAD_LETTER)
    finally:
        await svc1.stop()

    succeed_after_first = recorder.count("succeed")
    fail_after_first = recorder.count("always_retry_fail")

    # 第二个进程：不应重复执行任何终态任务
    svc2 = _make_service(db_path, registry, recorder)
    await svc2.start()
    try:
        import asyncio
        await asyncio.sleep(0.2)
    finally:
        await svc2.stop()

    assert recorder.count("succeed") == succeed_after_first
    assert recorder.count("always_retry_fail") == fail_after_first
