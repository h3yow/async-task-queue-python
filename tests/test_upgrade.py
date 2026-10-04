"""旧版本数据库原地升级（沿用旧库继续运行）场景。

背景：上一版本（依赖/流程编排能力引入之前）创建的数据库里，
``tasks`` 表只有 19 个列，没有 flow_id/flow_key/blocked_by，
也没有 task_dependencies 表与 idx_tasks_flow 索引。升级后的服务
必须能直接打开这样的库而不是报错退出。

本组用例覆盖的业务：
1. 旧库启动：迁移补齐列/表/索引，服务正常 start，进程不退出；
2. 老任务保留：原有任务的状态、结果、attempts、错误信息原样不动，
   升级后仍可按 id / 幂等键查询；
3. 继续调度：旧库遗留的 PENDING / RETRY_WAIT 照常被领取执行，
   RUNNING 孤儿按重启恢复重跑，而所有终态老任务不会被重跑；
4. 稳定可重复：同一个库反复 start/stop 不报错、不丢数据、不改状态。

“旧库上新增的依赖/流程能力”见 TestFlowCapabilitiesOnUpgradedDatabase。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time

from taskqueue.config import RetryConfig, TaskQueueConfig
from taskqueue.models import FlowState, FlowTaskSpec, TaskState
from taskqueue.retry import RetryPolicy
from taskqueue.service import TaskService
from taskqueue.store import TaskStore
from tests.helpers import CallRecorder, register_handlers, wait_for_state

#: 上一版本（HEAD~1，引入流程能力之前）的建库 DDL，原样固化在此，
#: 以便在不依赖 git 历史的前提下复现“旧格式数据库”。
_LEGACY_DDL = """
CREATE TABLE tasks (
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
CREATE UNIQUE INDEX idx_tasks_idem
    ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_tasks_due
    ON tasks(state, not_before, priority, created_at);
"""

#: 旧版 tasks 表的 19 个列（INSERT 时按此顺序）
_LEGACY_COLUMNS = (
    "id, task_type, payload_json, idempotency_key, state, priority, "
    "attempts, max_attempts, timeout, not_before, result_json, last_error, "
    "error_kind, created_at, updated_at, started_at, finished_at, "
    "lease_owner, lease_expires_at"
)


def _make_service(db_path: str, registry, recorder) -> TaskService:
    cfg = TaskQueueConfig(
        db_path=db_path,
        max_concurrency=4,
        poll_interval=0.005,
        default_task_timeout=5.0,
        retry=RetryConfig(max_attempts=3, base_delay=0.01,
                          multiplier=2.0, max_delay=1.0),
    )
    log = logging.getLogger("taskqueue.upgrade")
    log.setLevel(logging.CRITICAL)
    log.propagate = False
    return TaskService(TaskStore(db_path), registry,
                       RetryPolicy(cfg.retry), cfg, log)


def _legacy_insert(conn, *, id, state, attempts, result_json=None,
                   last_error=None, error_kind=None, idempotency_key=None,
                   task_type="succeed", payload=None, not_before=0.0,
                   started=False, finished=False, lease=False):
    now = time.time()
    conn.execute(
        f"INSERT INTO tasks ({_LEGACY_COLUMNS}) "
        f"VALUES ({','.join('?' * 19)})",
        (
            id, task_type, json.dumps(payload or {}), idempotency_key,
            state, 0, attempts, 3, 60.0, not_before, result_json,
            last_error, error_kind, now, now,
            now if started else None,
            now if finished else None,
            "dead-process" if lease else None,
            now + 999 if lease else None,
        ),
    )


def create_legacy_database(db_path: str) -> None:
    """按上一版本的磁盘格式造一个含多种状态老任务的数据库。"""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_LEGACY_DDL)
        # 已成功：带结果，升级后绝不能重跑
        _legacy_insert(
            conn, id="legacy-succeeded", state="SUCCEEDED", attempts=1,
            result_json=json.dumps({"echo": {"n": 42}}),
            idempotency_key="legacy-key-succeeded",
            started=True, finished=True, payload={"n": 42},
        )
        # 已失败（不可重试）
        _legacy_insert(
            conn, id="legacy-failed", state="FAILED", attempts=1,
            task_type="non_retryable",
            last_error="bad request payload", error_kind="NON_RETRYABLE",
            started=True, finished=True,
        )
        # 已死信
        _legacy_insert(
            conn, id="legacy-dead", state="DEAD_LETTER", attempts=3,
            task_type="always_retry_fail",
            last_error="persistent failure #3", error_kind="RETRYABLE",
            started=True, finished=True,
        )
        # 已取消
        _legacy_insert(
            conn, id="legacy-cancelled", state="CANCELLED", attempts=0,
            last_error="cancelled before execution", error_kind="CANCELLED",
        )
        # 排队中：升级后应被继续调度并成功
        _legacy_insert(conn, id="legacy-pending", state="PENDING",
                       attempts=0, payload={"q": 1})
        # 退避等待中（not_before 已过）：升级后继续执行并成功
        _legacy_insert(conn, id="legacy-retry-wait", state="RETRY_WAIT",
                       attempts=1, payload={"q": 2}, not_before=time.time())
        # 上次进程执行到一半被杀的孤儿：重启恢复后重跑一次
        _legacy_insert(conn, id="legacy-orphan", state="RUNNING",
                       attempts=1, lease=True, payload={"q": 3})
        conn.commit()
    finally:
        conn.close()


class TestLegacyDatabaseStartup:
    """业务：旧版格式数据库原地启动、数据保留与继续调度。"""

    async def test_service_starts_on_legacy_database_without_error(
        self, tmp_path, registry
    ):
        db_path = os.path.join(str(tmp_path), "legacy.db")
        create_legacy_database(db_path)

        recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
        register_handlers(registry, recorder)
        svc = _make_service(db_path, registry, recorder)
        # 修复前这里会直接抛 OperationalError: no such column: flow_id
        await svc.start()
        try:
            # 迁移后结构齐全：新列、依赖表、流程索引都在
            store_conn = svc.store.connection
            cols = {
                r["name"]
                for r in store_conn.execute("PRAGMA table_info(tasks)")
            }
            assert {"flow_id", "flow_key", "blocked_by"} <= cols
            names = {
                r["name"]
                for r in store_conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            assert "task_dependencies" in names
            indexes = {
                r["name"]
                for r in store_conn.execute("PRAGMA index_list(tasks)")
            }
            assert "idx_tasks_flow" in indexes
        finally:
            await svc.stop()

    async def test_legacy_tasks_are_preserved_and_still_queryable(
        self, tmp_path, registry
    ):
        db_path = os.path.join(str(tmp_path), "legacy.db")
        create_legacy_database(db_path)

        recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
        register_handlers(registry, recorder)
        svc = _make_service(db_path, registry, recorder)
        await svc.start()
        try:
            succeeded = await svc.get_task("legacy-succeeded")
            assert succeeded.state is TaskState.SUCCEEDED
            assert succeeded.attempts == 1
            assert succeeded.result == {"echo": {"n": 42}}
            # 新列对老任务必须是 NULL，不能被胡乱填值
            assert succeeded.flow_id is None
            assert succeeded.blocked_by is None

            failed = await svc.get_task("legacy-failed")
            assert failed.state is TaskState.FAILED
            assert failed.error_kind == "NON_RETRYABLE"
            assert failed.last_error == "bad request payload"

            dead = await svc.get_task("legacy-dead")
            assert dead.state is TaskState.DEAD_LETTER
            assert dead.attempts == 3

            cancelled = await svc.get_task("legacy-cancelled")
            assert cancelled.state is TaskState.CANCELLED

            # 幂等键查询路径在迁移后的库上同样可用
            by_key = await svc.get_by_idempotency_key("legacy-key-succeeded")
            assert by_key.id == "legacy-succeeded"
        finally:
            await svc.stop()

    async def test_legacy_open_tasks_continue_scheduling(self, tmp_path, registry):
        db_path = os.path.join(str(tmp_path), "legacy.db")
        create_legacy_database(db_path)

        recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
        register_handlers(registry, recorder)
        svc = _make_service(db_path, registry, recorder)
        await svc.start()
        try:
            pending = await wait_for_state(
                svc, "legacy-pending", TaskState.SUCCEEDED
            )
            assert pending.result == {"echo": {"q": 1}}

            retry_wait = await wait_for_state(
                svc, "legacy-retry-wait", TaskState.SUCCEEDED
            )
            assert retry_wait.result == {"echo": {"q": 2}}

            orphan = await wait_for_state(
                svc, "legacy-orphan", TaskState.SUCCEEDED
            )
            # 崩溃的那次尝试在恢复时回滚，重新领取计数为 1
            assert orphan.attempts == 1
            assert orphan.result == {"echo": {"q": 3}}
        finally:
            await svc.stop()

        # succeed 处理器实际执行次数：
        # pending(1) + retry_wait(1) + orphan(1) = 3；
        # legacy-succeeded 是老终态，绝不能被重跑。
        assert recorder.count("succeed") == 3
        # 已失败/死信的老任务也不能被重新拉起
        assert recorder.count("non_retryable") == 0
        assert recorder.count("always_retry_fail") == 0

    async def test_repeated_restarts_on_same_legacy_db_are_stable(
        self, tmp_path, registry
    ):
        db_path = os.path.join(str(tmp_path), "legacy.db")
        create_legacy_database(db_path)
        recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
        register_handlers(registry, recorder)

        # 第 1 次启动：完成迁移并把可执行任务推进到终态
        svc = _make_service(db_path, registry, recorder)
        await svc.start()
        try:
            await wait_for_state(svc, "legacy-pending", TaskState.SUCCEEDED)
            await wait_for_state(svc, "legacy-retry-wait", TaskState.SUCCEEDED)
            await wait_for_state(svc, "legacy-orphan", TaskState.SUCCEEDED)
        finally:
            await svc.stop()

        succeed_after_migrate = recorder.count("succeed")
        assert succeed_after_migrate == 3

        # 第 2、3 次启动：迁移幂等，不报错；所有任务已终态，不重跑、不改状态
        states_before: dict[str, str] = {}
        for _ in range(2):
            svc = _make_service(db_path, registry, recorder)
            await svc.start()
            try:
                await asyncio.sleep(0.15)
                for tid in (
                    "legacy-succeeded", "legacy-failed", "legacy-dead",
                    "legacy-cancelled", "legacy-pending",
                    "legacy-retry-wait", "legacy-orphan",
                ):
                    t = await svc.get_task(tid)
                    snapshot = f"{t.state.value}:{t.attempts}"
                    if tid in states_before:
                        assert states_before[tid] == snapshot, (
                            f"{tid} changed across restart: "
                            f"{states_before[tid]} -> {snapshot}"
                        )
                    states_before[tid] = snapshot
            finally:
                await svc.stop()

        assert recorder.count("succeed") == succeed_after_migrate


class TestFlowCapabilitiesOnUpgradedDatabase:
    """业务：旧库升级后，新增的依赖/流程编排能力完整可用。"""

    async def test_new_flow_runs_on_upgraded_legacy_db(self, tmp_path, registry):
        """在旧库上提交带依赖的全新流程，能正常编排推进到全成功。"""
        db_path = os.path.join(str(tmp_path), "legacy.db")
        create_legacy_database(db_path)
        recorder = CallRecorder(os.path.join(str(tmp_path), "calls.log"))
        register_handlers(registry, recorder)

        svc = _make_service(db_path, registry, recorder)
        await svc.start()
        try:

            flow = await svc.submit_flow([
                FlowTaskSpec(key="a", task_type="succeed", payload={"step": "a"}),
                FlowTaskSpec(key="b", task_type="succeed", payload={"step": "b"},
                             depends_on=["a"]),
                FlowTaskSpec(key="c", task_type="succeed", payload={"step": "c"},
                             depends_on=["b"]),
            ])
            flow_id = flow.flow_id

            # 等链路跑完
            for _ in range(500):
                status = await svc.get_flow(flow_id)
                if status.state is not FlowState.RUNNING:
                    break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError("flow did not finish on upgraded legacy db")

            status = await svc.get_flow(flow_id)
            assert status.state is FlowState.SUCCEEDED
            by_key = {t.flow_key: t for t in status.tasks}
            assert by_key["a"].state is TaskState.SUCCEEDED
            assert by_key["b"].state is TaskState.SUCCEEDED
            assert by_key["c"].state is TaskState.SUCCEEDED
            assert by_key["a"].result == {"echo": {"step": "a"}}

            # 老任务依然完好，且老的成功任务没有被流程重跑
            legacy = await svc.get_task("legacy-succeeded")
            assert legacy.state is TaskState.SUCCEEDED
            assert legacy.result == {"echo": {"n": 42}}
        finally:
            await svc.stop()

    async def test_flow_continues_after_restart_without_rerunning_upstream(
        self, tmp_path, registry
    ):
        """旧库上：上游成功、下游尚在 BLOCKED 时重启，下游续跑而上游不重跑。"""
        db_path = os.path.join(str(tmp_path), "legacy.db")
        create_legacy_database(db_path)
        calls_path = os.path.join(str(tmp_path), "calls.log")
        recorder = CallRecorder(calls_path)
        register_handlers(registry, recorder)


        # 第一个进程：提交 a -> b 流程，只等 a 成功，然后立刻停进程。
        svc1 = _make_service(db_path, registry, recorder)
        await svc1.start()
        try:
            flow = await svc1.submit_flow([
                FlowTaskSpec(key="a", task_type="succeed", payload={"step": "a"}),
                FlowTaskSpec(key="b", task_type="succeed", payload={"step": "b"},
                             depends_on=["a"]),
            ])
            flow_id = flow.flow_id
            task_a = next(t for t in flow.tasks if t.flow_key == "a")
            task_b = next(t for t in flow.tasks if t.flow_key == "b")
            await wait_for_state(svc1, task_a.id, TaskState.SUCCEEDED)
            # b 此刻正常情况下也应已被传播放行；无论如何重启后都必须收敛
        finally:
            await svc1.stop()

        succeed_after_first = recorder.count("succeed")

        # 额外模拟“崩溃在传播之前”：直接把 b 退回 BLOCKED，
        # 验证启动恢复的 refresh_blocked 能在旧库上补做传播。
        raw = sqlite3.connect(db_path)
        try:
            raw.execute(
                "UPDATE tasks SET state = 'BLOCKED' WHERE id = ?",
                (task_b.id,),
            )
            raw.commit()
        finally:
            raw.close()

        svc2 = _make_service(db_path, registry, recorder)
        await svc2.start()
        try:
            b = await wait_for_state(svc2, task_b.id, TaskState.SUCCEEDED)
            assert b.result == {"echo": {"step": "b"}}
            status = await svc2.get_flow(flow_id)
            assert status.state is FlowState.SUCCEEDED
        finally:
            await svc2.stop()

        # b 恰好补跑一次；a 是已成功上游，重启后绝不能被重跑
        assert recorder.count("succeed") == succeed_after_first + 1

    async def test_downstream_is_skipped_when_upstream_fails_on_legacy_db(
        self, tmp_path, registry
    ):
        """旧库上：上游不可重试失败，下游在重启后也应被正确收尾为 SKIPPED。"""
        db_path = os.path.join(str(tmp_path), "legacy.db")
        create_legacy_database(db_path)
        calls_path = os.path.join(str(tmp_path), "calls.log")
        recorder = CallRecorder(calls_path)
        register_handlers(registry, recorder)

        svc1 = _make_service(db_path, registry, recorder)
        await svc1.start()
        try:
            # 先等旧库自带的未完成老任务全部跑完并记录执行基线，
            # 把“老任务被继续调度”与“流程下游被带停”区分开。
            for tid in ("legacy-pending", "legacy-retry-wait", "legacy-orphan"):
                await wait_for_state(svc1, tid, TaskState.SUCCEEDED)
            base_succeed = recorder.count("succeed")

            flow = await svc1.submit_flow([
                FlowTaskSpec(key="bad", task_type="non_retryable"),
                FlowTaskSpec(key="down", task_type="succeed",
                             depends_on=["bad"]),
                FlowTaskSpec(key="grand", task_type="succeed",
                             depends_on=["down"]),
            ])
            flow_id = flow.flow_id
            ids = {t.flow_key: t.id for t in flow.tasks}
            await wait_for_state(svc1, ids["bad"], TaskState.FAILED)
        finally:
            await svc1.stop()

        # 模拟崩溃发生在“上游失败 -> 级联传播到孙级”之间：
        # 把 grand 人为退回 BLOCKED，重启后必须级联带停，不能一直挂着。
        raw = sqlite3.connect(db_path)
        try:
            raw.execute(
                "UPDATE tasks SET state = 'BLOCKED' WHERE id = ?",
                (ids["grand"],),
            )
            raw.commit()
        finally:
            raw.close()

        svc2 = _make_service(db_path, registry, recorder)
        await svc2.start()
        try:
            down = await wait_for_state(
                svc2, ids["down"],
                (TaskState.SKIPPED, TaskState.FAILED),
            )
            grand = await wait_for_state(
                svc2, ids["grand"],
                (TaskState.SKIPPED, TaskState.FAILED),
            )
            assert down.state is TaskState.SKIPPED
            assert grand.state is TaskState.SKIPPED
            assert down.blocked_by == ids["bad"]
            # 孙级因父级被跳过而带停
            assert grand.blocked_by == ids["down"]

            for _ in range(100):
                status = await svc2.get_flow(flow_id)
                if status.state is not FlowState.RUNNING:
                    break
                await asyncio.sleep(0.02)
            status = await svc2.get_flow(flow_id)
            assert status.state is FlowState.FAILED
        finally:
            await svc2.stop()

        # 被带停的下游从未执行；succeed 次数相对老任务基线没有增加
        assert recorder.count("succeed") == base_succeed
        assert recorder.count("non_retryable") == 1
