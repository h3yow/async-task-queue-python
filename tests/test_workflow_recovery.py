"""流程在进程重启后的恢复。

模拟旧进程在流程执行中途崩溃，数据库中遗留:
- 已 SUCCEEDED 的上游（重启后绝不能再执行一遍）；
- 仍是 BLOCKED 但依赖其实已全部成功的任务（应被对账激活、继续推进）；
- 上游已 FAILED/DEAD_LETTER 但下游尚未及带停的任务（应被对账补成 SKIPPED）；
- 上次正在 RUNNING 的任务（沿用孤儿恢复回退 PENDING 重跑，不静默成功）。
"""
from __future__ import annotations

import os
import time

from taskqueue.models import Task, TaskState
from tests.helpers import CallRecorder, register_handlers
from tests.test_recovery import _make_service


def _wf_task(
    task_id, key, task_type, state, *, workflow_id="wf1", deps=(),
    attempts=0, not_before=0.0, error_kind=None, last_error=None,
    max_attempts=3, lease_owner=None, lease_expires_at=None,
):
    now = time.time()
    return Task(
        id=task_id, task_type=task_type, payload={}, idempotency_key=None,
        state=state, priority=0, attempts=attempts, max_attempts=max_attempts,
        timeout=5.0, not_before=not_before or now, created_at=now,
        updated_at=now, workflow_id=workflow_id, task_key=key,
        error_kind=error_kind, last_error=last_error,
        lease_owner=lease_owner, lease_expires_at=lease_expires_at,
    )


def _seed_partial_workflow(db_path: str) -> None:
    """直接写库模拟崩溃现场。"""
    from taskqueue.store import TaskStore

    store = TaskStore(db_path)
    store.connect()
    now = time.time()
    store.connection.execute(
        "INSERT INTO workflows (id, name, created_at) VALUES (?, ?, ?)",
        ("wf1", "partial", now),
    )
    tasks = [
        # 已成功的根：重启后不得重跑
        _wf_task("a", "a", "succeed", TaskState.SUCCEEDED, attempts=1),
        # 依赖只有 a，且 a 已成功，但崩溃导致它还停在 BLOCKED -> 应激活并成功
        _wf_task("b", "b", "succeed", TaskState.BLOCKED, deps=("a",)),
        # 依赖 a、b：b 恢复成功后才应被激活
        _wf_task("c", "c", "succeed", TaskState.BLOCKED, deps=("b",)),
    ]
    for t in tasks:
        store.insert_task(t)
    for child, parent in (("b", "a"), ("c", "b")):
        store.connection.execute(
            "INSERT INTO task_dependencies (workflow_id, task_key, depends_on_key)"
            " VALUES (?, ?, ?)",
            ("wf1", child, parent),
        )
    store.close()


def _seed_failed_workflow(db_path: str) -> None:
    """上游已 FAILED、下游还停在 BLOCKED（带停未完成的崩溃窗口）。"""
    from taskqueue.store import TaskStore

    store = TaskStore(db_path)
    store.connect()
    now = time.time()
    store.connection.execute(
        "INSERT INTO workflows (id, name, created_at) VALUES (?, ?, ?)",
        ("wf2", "failed", now),
    )
    store.insert_task(_wf_task(
        "f", "f", "non_retryable", TaskState.FAILED, workflow_id="wf2",
        attempts=1, error_kind="NON_RETRYABLE", last_error="boom",
    ))
    store.insert_task(_wf_task(
        "g", "g", "succeed", TaskState.BLOCKED, workflow_id="wf2",
    ))
    store.insert_task(_wf_task(
        "h", "h", "succeed", TaskState.BLOCKED, workflow_id="wf2",
    ))
    for child, parent in (("g", "f"), ("h", "g")):
        store.connection.execute(
            "INSERT INTO task_dependencies (workflow_id, task_key, depends_on_key)"
            " VALUES (?, ?, ?)",
            ("wf2", child, parent),
        )
    store.close()


def _seed_running_orphan_workflow(db_path: str) -> None:
    """根任务在 RUNNING 时崩溃：回退 PENDING 重跑，成功后带动下游。"""
    from taskqueue.store import TaskStore

    store = TaskStore(db_path)
    store.connect()
    now = time.time()
    store.connection.execute(
        "INSERT INTO workflows (id, name, created_at) VALUES (?, ?, ?)",
        ("wf3", "orphan", now),
    )
    store.insert_task(_wf_task(
        "r", "r", "succeed", TaskState.RUNNING, workflow_id="wf3",
        attempts=1, lease_owner="dead-process", lease_expires_at=now + 999,
    ))
    store.insert_task(_wf_task(
        "s", "s", "succeed", TaskState.BLOCKED, workflow_id="wf3",
    ))
    store.connection.execute(
        "INSERT INTO task_dependencies (workflow_id, task_key, depends_on_key)"
        " VALUES (?, ?, ?)",
        ("wf3", "s", "r"),
    )
    store.close()


async def test_restart_continues_workflow_without_rerunning_succeeded(tmp_path,
                                                                      registry):
    db_path = os.path.join(str(tmp_path), "recovery_wf.db")
    recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
    register_handlers(registry, recorder)
    _seed_partial_workflow(db_path)

    svc = _make_service(db_path, registry, recorder)
    await svc.start()
    try:
        view = None
        import asyncio
        for _ in range(300):
            await asyncio.sleep(0.01)
            view = await svc.get_workflow("wf1")
            if view.state != "RUNNING":
                break
        assert view is not None and view.state == "SUCCEEDED", \
            f"state={view.state if view else None}"
        nodes = {tv.task.task_key: tv.task for tv in view.tasks}
        # a 沿用旧的成功结果，attempts 仍为 1
        assert nodes["a"].state == TaskState.SUCCEEDED
        assert nodes["a"].attempts == 1
        assert nodes["b"].state == TaskState.SUCCEEDED
        assert nodes["c"].state == TaskState.SUCCEEDED
    finally:
        await svc.stop()

    # succeed 真实执行次数：a 不重跑，只有 b、c 各执行一次
    assert recorder.count("succeed") == 2


async def test_restart_sets_skipped_for_unpropagated_failure(tmp_path,
                                                             registry):
    db_path = os.path.join(str(tmp_path), "recovery_fail.db")
    recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
    register_handlers(registry, recorder)
    _seed_failed_workflow(db_path)

    svc = _make_service(db_path, registry, recorder)
    await svc.start()
    try:
        import asyncio
        view = None
        for _ in range(100):
            await asyncio.sleep(0.01)
            view = await svc.get_workflow("wf2")
            if view.state != "RUNNING":
                break
        assert view is not None and view.state == "FAILED"
        nodes = {tv.task.task_key: tv for tv in view.tasks}
        assert nodes["g"].task.state == TaskState.SKIPPED
        assert nodes["h"].task.state == TaskState.SKIPPED
        assert nodes["g"].skipped_by_key == "f"
        assert nodes["h"].skipped_by_key == "g"
        assert nodes["g"].task.error_kind == "UPSTREAM_FAILED"
        assert nodes["h"].task.error_kind == "UPSTREAM_FAILED"
        # 被带停任务从未执行
        assert recorder.count("succeed") == 0
    finally:
        await svc.stop()


async def test_restart_reruns_orphan_root_then_continues(tmp_path, registry):
    db_path = os.path.join(str(tmp_path), "recovery_orphan.db")
    recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
    register_handlers(registry, recorder)
    _seed_running_orphan_workflow(db_path)

    svc = _make_service(db_path, registry, recorder)
    await svc.start()
    try:
        import asyncio
        view = None
        for _ in range(300):
            await asyncio.sleep(0.01)
            view = await svc.get_workflow("wf3")
            if view.state != "RUNNING":
                break
        assert view is not None and view.state == "SUCCEEDED"
        nodes = {tv.task.task_key: tv.task for tv in view.tasks}
        # 崩溃那一次 attempts 回滚后重跑，最终 attempts=1
        assert nodes["r"].state == TaskState.SUCCEEDED
        assert nodes["r"].attempts == 1
        assert nodes["s"].state == TaskState.SUCCEEDED
        assert recorder.count("succeed") == 2
    finally:
        await svc.stop()
