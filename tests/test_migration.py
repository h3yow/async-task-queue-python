"""旧版本数据库升级（schema 迁移）场景测试。

覆盖需求:
- 用上一版格式（tasks 表无 flow_id/flow_key/blocked_by 列、
  无 task_dependencies 表）的数据库启动服务，不报错退出；
- 老任务的状态/结果/错误信息/幂等键原样保留，升级后照常查询；
- 老库中未完成的 PENDING/RUNNING 任务升级后继续被调度执行；
- 升级后的库上提交带依赖的流程能正常编排推进，
  重启后流程接着走、已成功上游不重跑、失败上游的下游被带停；
- 反复启动/重启幂等：不报错、不丢任务、不改坏状态；
- 全新数据库行为不变（建出完整新 schema）。
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time

import pytest

from taskqueue.models import FlowState, FlowTaskSpec, Task, TaskState
from taskqueue.retry import RetryPolicy
from taskqueue.service import TaskService
from taskqueue.store import TaskStore
from tests.helpers import CallRecorder, register_handlers, wait_for_state

#: 上一版（依赖/流程能力之前）的建库语句，原样保留用于构造“老库”
_V1_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    task_type        TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    idempotency_key  TEXT,
    state            TEXT NOT NULL,
    priority         INTEGER NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    timeout          REAL NOT NULL,
    not_before       REAL NOT NULL DEFAULT 0,
    result_json      TEXT,
    last_error       TEXT,
    error_kind       TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    started_at       REAL,
    finished_at      REAL,
    lease_owner      TEXT,
    lease_expires_at REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idem
    ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_due
    ON tasks(state, not_before, priority, created_at);
"""

_V1_COLUMNS = (
    "id, task_type, payload_json, idempotency_key, state, priority, attempts,"
    " max_attempts, timeout, not_before, result_json, last_error, error_kind,"
    " created_at, updated_at, started_at, finished_at, lease_owner,"
    " lease_expires_at"
)


def _v1_task(
    task_id: str,
    state: str,
    task_type: str = "succeed",
    payload: dict | None = None,
    idempotency_key: str | None = None,
    attempts: int = 0,
    result: object = None,
    last_error: str | None = None,
    error_kind: str | None = None,
    lease_owner: str | None = None,
    lease_expires_at: float | None = None,
) -> tuple:
    now = time.time()
    terminal = state in {"SUCCEEDED", "FAILED", "CANCELLED", "DEAD_LETTER"}
    return (
        task_id,
        task_type,
        json.dumps(payload if payload is not None else {"legacy": task_id}),
        idempotency_key,
        state,
        0,          # priority
        attempts,
        3,          # max_attempts
        5.0,        # timeout
        0.0,        # not_before
        json.dumps(result) if result is not None else None,
        last_error,
        error_kind,
        now,
        now,
        now if state in {"RUNNING"} or terminal else None,
        now if terminal else None,
        lease_owner,
        lease_expires_at,
    )


def create_legacy_db(db_path: str, tasks: list[tuple]) -> None:
    """按上一版格式建库并写入老任务（模拟升级前的真实数据库）。"""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_V1_SCHEMA)
        for row in tasks:
            conn.execute(
                f"INSERT INTO tasks ({_V1_COLUMNS}) "
                f"VALUES ({', '.join('?' for _ in row)})",
                row,
            )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def recorder(tmp_path):
    return CallRecorder(os.path.join(str(tmp_path), "calls.log"))


def make_service(config, registry, logger) -> TaskService:
    """每次调用生成一个“新进程”的服务实例（同一数据库文件）。"""
    return TaskService(
        TaskStore(config.db_path),
        registry,
        RetryPolicy(config.retry),
        config,
        logger,
    )


def spec(key: str, task_type: str = "succeed", **kw) -> FlowTaskSpec:
    return FlowTaskSpec(key=key, task_type=task_type, **kw)


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


# ---------- 旧库能打开，老数据原样保留 ----------


async def test_service_starts_on_legacy_db_and_preserves_tasks(
    service, config, registry, recorder
):
    """上一版格式的库：服务正常启动，老任务状态/结果/错误/幂等键原样可查。"""
    register_handlers(registry, recorder)
    create_legacy_db(config.db_path, [
        _v1_task("old-ok", "SUCCEEDED", result={"echo": {"n": 1}},
                 idempotency_key="idem-old-ok"),
        _v1_task("old-fail", "FAILED", last_error="boom",
                 error_kind="NON_RETRYABLE", attempts=3),
        _v1_task("old-dead", "DEAD_LETTER", last_error="retries exhausted",
                 error_kind="RETRYABLE", attempts=3),
    ])

    await service.start()
    try:
        ok = await service.get_task("old-ok")
        assert ok.state == TaskState.SUCCEEDED
        assert ok.result == {"echo": {"n": 1}}
        assert ok.finished_at is not None

        fail = await service.get_task("old-fail")
        assert fail.state == TaskState.FAILED
        assert fail.last_error == "boom"
        assert fail.error_kind == "NON_RETRYABLE"
        assert fail.attempts == 3

        dead = await service.get_task("old-dead")
        assert dead.state == TaskState.DEAD_LETTER

        # 幂等键查询照常可用
        by_key = await service.get_by_idempotency_key("idem-old-ok")
        assert by_key.id == "old-ok"

        # 终态任务绝不被重新调度
        assert recorder.count("succeed") == 0
    finally:
        await service.stop()


async def test_legacy_db_gains_new_schema_objects(service, config, registry):
    """升级后旧库补齐新列与新表，且不影响既有行。"""
    register_handlers(registry, CallRecorder(os.devnull))
    create_legacy_db(config.db_path, [_v1_task("old-1", "SUCCEEDED")])

    await service.start()
    try:
        cols = {
            row["name"]
            for row in service.store.connection.execute(
                "PRAGMA table_info(tasks)"
            )
        }
        assert {"flow_id", "flow_key", "blocked_by"} <= cols
        tables = {
            row["name"]
            for row in service.store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "task_dependencies" in tables
        # 老行的新列为 NULL（普通单任务语义）
        task = await service.get_task("old-1")
        assert task.flow_id is None
        assert task.flow_key is None
        assert task.blocked_by is None
    finally:
        await service.stop()


# ---------- 老库中未完成的任务升级后继续调度 ----------


async def test_legacy_pending_task_is_scheduled_after_upgrade(
    service, config, registry, recorder
):
    """老库里的 PENDING 任务升级后照常领取执行。"""
    register_handlers(registry, recorder)
    create_legacy_db(config.db_path, [
        _v1_task("old-pending", "PENDING", payload={"n": 42}),
    ])

    await service.start()
    try:
        done = await wait_for_state(service, "old-pending", TaskState.SUCCEEDED)
        assert done.result == {"echo": {"n": 42}}
        assert done.attempts == 1
    finally:
        await service.stop()
    assert recorder.count("succeed") == 1


async def test_legacy_running_task_is_recovered_after_upgrade(
    service, config, registry, recorder
):
    """老进程遗留的 RUNNING 孤儿任务升级后按既有语义恢复重跑。"""
    register_handlers(registry, recorder)
    create_legacy_db(config.db_path, [
        _v1_task("old-running", "RUNNING", attempts=1,
                 lease_owner="dead-process",
                 lease_expires_at=time.time() + 999),
    ])

    await service.start()
    try:
        done = await wait_for_state(service, "old-running", TaskState.SUCCEEDED)
        # 崩溃的那次尝试回滚计数，真实执行次数 == attempts
        assert done.attempts == 1
    finally:
        await service.stop()
    assert recorder.count("succeed") == 1


# ---------- 升级后的旧库上，流程编排能力可用 ----------


async def test_flow_runs_on_upgraded_legacy_db(
    service, config, registry, recorder
):
    """在升级后的旧库上提交带依赖的流程，能正常编排推进到成功。"""
    register_handlers(registry, recorder)
    create_legacy_db(config.db_path, [_v1_task("old-1", "SUCCEEDED")])

    await service.start()
    try:
        flow = await service.submit_flow([
            spec("a"), spec("b", depends_on=["a"]), spec("c", depends_on=["b"]),
        ])
        done = await wait_for_flow(service, flow.flow_id, FlowState.SUCCEEDED)
        assert all(t.state == TaskState.SUCCEEDED for t in done.tasks)
        # 老任务不受流程影响
        assert (await service.get_task("old-1")).state == TaskState.SUCCEEDED
    finally:
        await service.stop()


async def test_flow_on_legacy_db_skips_downstream_of_failed_upstream(
    service, config, registry, recorder
):
    """升级后的旧库上，上游失败时下游被带停（SKIPPED）且不执行。"""
    register_handlers(registry, recorder)
    create_legacy_db(config.db_path, [_v1_task("old-1", "SUCCEEDED")])

    await service.start()
    try:
        flow = await service.submit_flow([
            spec("bad", task_type="non_retryable"),
            spec("down", depends_on=["bad"]),
        ])
        done = await wait_for_flow(service, flow.flow_id, FlowState.FAILED)
        t = {x.flow_key: x for x in done.tasks}
        assert t["bad"].state == TaskState.FAILED
        assert t["down"].state == TaskState.SKIPPED
        assert t["down"].blocked_by == t["bad"].id
        assert t["down"].error_kind == "UPSTREAM_FAILED"
    finally:
        await service.stop()


async def test_restart_on_upgraded_db_continues_flow_without_rerun(
    service, config, registry, recorder, logger
):
    """旧库升级后：崩溃发生在“上游已成功、传播未完成”之间，
    重启后流程接着走，已成功的上游不重跑。"""

    async def tracked(payload, ctx):
        await recorder.bump(payload["name"])
        return {"done": payload["name"]}

    registry.register("tracked", tracked)
    create_legacy_db(config.db_path, [_v1_task("old-1", "SUCCEEDED")])

    # 第一轮：启动即完成迁移，随后停服（等价于进程退出）
    await service.start()
    await service.stop()

    # 在已升级的库上布置崩溃现场：上游已 SUCCEEDED，
    # 下游仍卡在 BLOCKED（崩溃发生在落终态与传播之间）
    now = time.time()
    flow_id = "flow-legacy"
    up = Task(
        id="t-up", task_type="tracked", payload={"name": "up"},
        idempotency_key=None, state=TaskState.SUCCEEDED, priority=0,
        attempts=1, max_attempts=3, timeout=5.0, not_before=now,
        result={"done": "up"}, created_at=now, updated_at=now,
        started_at=now, finished_at=now, flow_id=flow_id, flow_key="up",
    )
    down = Task(
        id="t-down", task_type="tracked", payload={"name": "down"},
        idempotency_key=None, state=TaskState.BLOCKED, priority=0,
        attempts=0, max_attempts=3, timeout=5.0, not_before=now,
        created_at=now, updated_at=now, flow_id=flow_id, flow_key="down",
    )
    store = TaskStore(config.db_path)
    store.connect()
    store.insert_flow([up, down], [("t-down", "t-up")])
    store.close()

    # 第二轮：全新服务实例（新进程）接着跑
    service2 = make_service(config, registry, logger)
    await service2.start()
    try:
        done = await wait_for_flow(service2, flow_id, FlowState.SUCCEEDED)
        t = {x.flow_key: x for x in done.tasks}
        assert t["up"].state == TaskState.SUCCEEDED
        assert t["up"].result == {"done": "up"}
        assert t["down"].state == TaskState.SUCCEEDED
        # 老任务不受流程恢复影响
        assert (await service2.get_task("old-1")).state == TaskState.SUCCEEDED
    finally:
        await service2.stop()

    # 已成功的 up 绝不重跑；卡在 BLOCKED 的 down 被放行后只执行一次
    assert recorder.count("up") == 0
    assert recorder.count("down") == 1


# ---------- 升级过程稳定、可重复 ----------


async def test_repeated_restarts_on_legacy_db_are_idempotent(
    service, config, registry, recorder, logger
):
    """同一旧库反复启动/停止：不报错、老任务不丢不坏。"""
    register_handlers(registry, recorder)
    create_legacy_db(config.db_path, [
        _v1_task("old-ok", "SUCCEEDED", result={"v": 1},
                 idempotency_key="idem-1"),
        _v1_task("old-fail", "FAILED", last_error="boom",
                 error_kind="NON_RETRYABLE", attempts=3),
        _v1_task("old-pending", "PENDING", payload={"n": 7}),
    ])

    # 第一轮：PENDING 任务被执行完毕
    await service.start()
    try:
        done = await wait_for_state(service, "old-pending", TaskState.SUCCEEDED)
        assert done.result == {"echo": {"n": 7}}
    finally:
        await service.stop()

    # 之后再反复重启两轮：每次都不报错，数据保持原样
    for _ in range(2):
        svc = make_service(config, registry, logger)
        await svc.start()
        try:
            ok = await svc.get_task("old-ok")
            assert ok.state == TaskState.SUCCEEDED
            assert ok.result == {"v": 1}
            fail = await svc.get_task("old-fail")
            assert fail.state == TaskState.FAILED
            assert fail.attempts == 3
            pend = await svc.get_task("old-pending")
            assert pend.state == TaskState.SUCCEEDED
            assert pend.attempts == 1
            assert (await svc.get_by_idempotency_key("idem-1")).id == "old-ok"
        finally:
            await svc.stop()

    # 终态任务全程没有被重复执行
    assert recorder.count("succeed") == 1


async def test_store_connect_is_idempotent_on_legacy_db(config):
    """底层存储层面：同一旧库重复 connect/close 不报错，行数不变。"""
    create_legacy_db(config.db_path, [
        _v1_task("old-1", "SUCCEEDED"),
        _v1_task("old-2", "PENDING"),
    ])
    for _ in range(3):
        store = TaskStore(config.db_path)
        store.connect()
        try:
            assert store.count_state(TaskState.SUCCEEDED) == 1
            assert store.count_state(TaskState.PENDING) == 1
            assert store.get_task("old-1") is not None
        finally:
            store.close()


# ---------- 全新数据库行为不变 ----------


async def test_fresh_db_has_full_schema(service, config, registry):
    """全新数据库：直接建出完整 schema（新列、新表、索引齐全）。"""
    register_handlers(registry, CallRecorder(os.devnull))
    await service.start()
    try:
        cols = {
            row["name"]
            for row in service.store.connection.execute(
                "PRAGMA table_info(tasks)"
            )
        }
        assert {"flow_id", "flow_key", "blocked_by"} <= cols
        tables = {
            row["name"]
            for row in service.store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "task_dependencies" in tables
        indexes = {
            row["name"]
            for row in service.store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            )
        }
        assert {"idx_tasks_idem", "idx_tasks_due", "idx_tasks_flow",
                "idx_deps_upstream"} <= indexes
    finally:
        await service.stop()
