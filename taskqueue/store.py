"""SQLite 持久化存储。

语义:
- 单进程单写者，开启 WAL；写入方法串行化（threading.Lock）。
- :meth:`claim_next_due` 使用单条 UPDATE ... ORDER BY 原子领取任务，
  保证同一条任务不会被并发领取/重复执行。
- RUNNING 任务带租约（lease_owner/lease_expires_at），
  进程重启后可识别上一个进程遗留的 RUNNING 任务并做恢复，绝不静默成功。
"""
from __future__ import annotations

import contextlib
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from taskqueue.errors import (
    ERROR_KIND_TIMEOUT,
    ERROR_KIND_UPSTREAM_CANCELLED,
    ERROR_KIND_UPSTREAM_DEAD_LETTER,
    ERROR_KIND_UPSTREAM_FAILED,
    ERROR_KIND_UPSTREAM_TIMEOUT,
)
from taskqueue.models import TERMINAL_STATES, Task, TaskState, Workflow

_SCHEMA_TABLES = """
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
    lease_expires_at REAL,
    workflow_id      TEXT,
    task_key         TEXT,
    skipped_by_task_id TEXT
);

CREATE TABLE IF NOT EXISTS workflows (
    id         TEXT PRIMARY KEY,
    name       TEXT,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    workflow_id    TEXT NOT NULL,
    task_key       TEXT NOT NULL,
    depends_on_key TEXT NOT NULL,
    PRIMARY KEY (workflow_id, task_key, depends_on_key)
);
"""

_SCHEMA_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idem
    ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_due
    ON tasks(state, not_before, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow
    ON tasks(workflow_id, created_at);
CREATE INDEX IF NOT EXISTS idx_deps_downstream
    ON task_dependencies(workflow_id, depends_on_key);
CREATE INDEX IF NOT EXISTS idx_deps_upstream
    ON task_dependencies(workflow_id, task_key);
"""

# 旧库升级：tasks 表补齐流程编排列（幂等，列已存在则跳过）
_TASK_ADDED_COLUMNS = {
    "workflow_id": "TEXT",
    "task_key": "TEXT",
    "skipped_by_task_id": "TEXT",
}

# 允许被调度器领取的状态
_CLAIMABLE_STATES = (TaskState.PENDING.value, TaskState.RETRY_WAIT.value)

# update_state 时不可由调用方直接覆写的内部字段
_RESERVED_COLUMNS = {"id", "state"}

# 非成功终态 -> 被带停下游的 error_kind（判定单一来源，整次传播用同一原因）
_UPSTREAM_SKIP_KIND = {
    TaskState.FAILED: ERROR_KIND_UPSTREAM_FAILED,
    TaskState.CANCELLED: ERROR_KIND_UPSTREAM_CANCELLED,
    TaskState.DEAD_LETTER: ERROR_KIND_UPSTREAM_DEAD_LETTER,
}


def _skip_kind_for(state: TaskState, error_kind: str | None) -> str:
    """根据上游终态与具体 error_kind 决定下游带停原因。

    超时会在用尽重试后进入 DEAD_LETTER（error_kind=TIMEOUT），这里将其细分
    为 UPSTREAM_TIMEOUT，与普通死信区分；SKIPPED 根沿用其既有带停原因。
    """
    if state == TaskState.DEAD_LETTER and error_kind == ERROR_KIND_TIMEOUT:
        return ERROR_KIND_UPSTREAM_TIMEOUT
    return _UPSTREAM_SKIP_KIND[state]


@dataclass
class TerminalSettlement:
    """终态结算结果。

    changed=False 表示状态守卫不匹配（任务已被并发改动），此时不会做任何传播。
    activated / skipped 仅在 changed=True 且任务属于某条流程时有意义。
    """

    changed: bool
    activated: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

_TASK_COLUMNS = [
    "id",
    "task_type",
    "payload_json",
    "idempotency_key",
    "state",
    "priority",
    "attempts",
    "max_attempts",
    "timeout",
    "not_before",
    "result_json",
    "last_error",
    "error_kind",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
    "lease_owner",
    "lease_expires_at",
    "workflow_id",
    "task_key",
    "skipped_by_task_id",
]


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    # ---------- 生命周期 ----------

    def connect(self) -> None:
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # autocommit；写事务显式 BEGIN IMMEDIATE
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.executescript(_SCHEMA_TABLES)
        # 旧库先补齐 tasks 新列，再创建引用这些列的索引，避免
        # “no such column: workflow_id”。
        self._migrate_tasks_columns(conn)
        conn.executescript(_SCHEMA_INDEXES)
        self._conn = conn

    @staticmethod
    def _migrate_tasks_columns(conn: sqlite3.Connection) -> None:
        existing = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)")}
        for column, decl in _TASK_ADDED_COLUMNS.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {column} {decl}")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("TaskStore is not connected")
        return self._conn

    # ---------- 序列化 ----------

    @staticmethod
    def _task_to_row(task: Task) -> dict[str, Any]:
        return {
            "id": task.id,
            "task_type": task.task_type,
            "payload_json": json.dumps(task.payload, ensure_ascii=False),
            "idempotency_key": task.idempotency_key,
            "state": task.state.value,
            "priority": task.priority,
            "attempts": task.attempts,
            "max_attempts": task.max_attempts,
            "timeout": task.timeout,
            "not_before": task.not_before,
            "result_json": (
                json.dumps(task.result, ensure_ascii=False)
                if task.result is not None
                else None
            ),
            "last_error": task.last_error,
            "error_kind": task.error_kind,
            "created_at": task.created_at,
            "updated_at": task.updated_at,
            "started_at": task.started_at,
            "finished_at": task.finished_at,
            "lease_owner": task.lease_owner,
            "lease_expires_at": task.lease_expires_at,
            "workflow_id": task.workflow_id,
            "task_key": task.task_key,
            "skipped_by_task_id": task.skipped_by_task_id,
        }

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            task_type=row["task_type"],
            payload=json.loads(row["payload_json"]),
            idempotency_key=row["idempotency_key"],
            state=TaskState(row["state"]),
            priority=row["priority"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            timeout=row["timeout"],
            not_before=row["not_before"],
            result=(
                json.loads(row["result_json"])
                if row["result_json"] is not None
                else None
            ),
            last_error=row["last_error"],
            error_kind=row["error_kind"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            workflow_id=row["workflow_id"],
            task_key=row["task_key"],
            skipped_by_task_id=row["skipped_by_task_id"],
        )

    # ---------- 读 ----------

    def get_task(self, task_id: str) -> Task | None:
        row = self.connection.execute(
            f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        return self._row_to_task(row) if row is not None else None

    def find_by_idempotency_key(self, key: str) -> Task | None:
        row = self.connection.execute(
            f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        return self._row_to_task(row) if row is not None else None

    def iter_open_tasks(self) -> list[Task]:
        """返回所有未进入终态的任务。"""
        placeholders = ",".join("?" for _ in TERMINAL_STATES)
        rows = self.connection.execute(
            f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
            f"WHERE state NOT IN ({placeholders})",
            tuple(s.value for s in TERMINAL_STATES),
        ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def count_state(self, state: TaskState) -> int:
        return self.connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE state = ?", (state.value,)
        ).fetchone()[0]

    # ---------- 流程读 ----------

    @staticmethod
    def _row_to_workflow(row: sqlite3.Row) -> Workflow:
        return Workflow(id=row["id"], name=row["name"], created_at=row["created_at"])

    def get_workflow(self, workflow_id: str) -> Workflow | None:
        row = self.connection.execute(
            "SELECT id, name, created_at FROM workflows WHERE id = ?",
            (workflow_id,),
        ).fetchone()
        return self._row_to_workflow(row) if row is not None else None

    def list_workflow_tasks(self, workflow_id: str) -> list[Task]:
        rows = self.connection.execute(
            f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
            "WHERE workflow_id = ? ORDER BY created_at ASC, id ASC",
            (workflow_id,),
        ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def list_workflow_dependencies(
        self, workflow_id: str
    ) -> list[tuple[str, str]]:
        """返回 (task_key, depends_on_key) 列表。"""
        rows = self.connection.execute(
            "SELECT task_key, depends_on_key FROM task_dependencies "
            "WHERE workflow_id = ? ORDER BY task_key, depends_on_key",
            (workflow_id,),
        ).fetchall()
        return [(r["task_key"], r["depends_on_key"]) for r in rows]

    def list_open_workflow_ids(self) -> list[str]:
        """返回仍含非终态任务的流程 id（去重）。"""
        placeholders = ",".join("?" for _ in TERMINAL_STATES)
        rows = self.connection.execute(
            f"SELECT DISTINCT workflow_id FROM tasks "
            f"WHERE workflow_id IS NOT NULL AND state NOT IN ({placeholders})",
            tuple(s.value for s in TERMINAL_STATES),
        ).fetchall()
        return [r["workflow_id"] for r in rows]

    # ---------- 写 ----------

    @contextlib.contextmanager
    def _transaction(self):
        """显式写事务；任何异常都回滚，绝不遗留脏事务。

        所有调用都在唯一 DB actor 线程上发生，天然串行，无需再加锁。
        """
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def insert_task(self, task: Task) -> Task:
        row = self._task_to_row(task)
        cols = ", ".join(row.keys())
        marks = ", ".join("?" for _ in row)
        try:
            with self._transaction():
                self.connection.execute(
                    f"INSERT INTO tasks ({cols}) VALUES ({marks})",
                    tuple(row.values()),
                )
        except sqlite3.IntegrityError as exc:
            raise IdempotencyKeyTaken(task.idempotency_key) from exc
        return task

    def claim_next_due(self, owner: str, now: float, lease_seconds: float) -> Task | None:
        """原子领取一条到期可执行任务（优先级高者优先，同优先级 FIFO）。

        仅在一个写事务内完成 SELECT + UPDATE，保证不会重复领取。
        """
        placeholders = ",".join("?" for _ in _CLAIMABLE_STATES)
        claimed_id: str | None = None
        with self._transaction():
            row = self.connection.execute(
                f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
                f"WHERE state IN ({placeholders}) AND not_before <= ? "
                f"ORDER BY priority DESC, created_at ASC LIMIT 1",
                (*_CLAIMABLE_STATES, now),
            ).fetchone()
            if row is not None:
                expires = now + lease_seconds
                updated = self.connection.execute(
                    "UPDATE tasks SET state = ?, attempts = attempts + 1, "
                    "started_at = ?, updated_at = ?, lease_owner = ?, lease_expires_at = ? "
                    "WHERE id = ? AND state IN (" + placeholders + ")",
                    (
                        TaskState.RUNNING.value,
                        now,
                        now,
                        owner,
                        expires,
                        row["id"],
                        *_CLAIMABLE_STATES,
                    ),
                )
                if updated.rowcount == 1:
                    claimed_id = row["id"]
                # rowcount != 1（极端并发竞争）：事务提交但未领取，等价本轮无任务

        if claimed_id is None:
            return None
        fresh = self.get_task(claimed_id)
        assert fresh is not None
        return fresh

    @staticmethod
    def _prepare_fields(
        new_state: TaskState, now: float | None, fields: dict[str, Any]
    ) -> dict[str, Any]:
        """把调用方字段转换为可直接写入的列映射（含 result_json/终态/租约处理）。"""
        for key in fields:
            if key in _RESERVED_COLUMNS:
                raise ValueError(f"reserved column cannot be overwritten: {key}")

        field_map = dict(fields)
        field_map["updated_at"] = now if now is not None else time.time()

        if "result" in field_map:
            field_map["result_json"] = (
                json.dumps(field_map.pop("result"), ensure_ascii=False)
                if field_map["result"] is not None
                else None
            )

        if new_state in TERMINAL_STATES:
            field_map.setdefault("finished_at", field_map["updated_at"])

        if new_state != TaskState.RUNNING:
            # 离开 RUNNING 后释放租约
            field_map["lease_owner"] = None
            field_map["lease_expires_at"] = None
        return field_map

    def update_state(
        self,
        task_id: str,
        expected_state: TaskState | tuple[TaskState, ...] | None,
        new_state: TaskState,
        now: float | None = None,
        **fields: Any,
    ) -> bool:
        """乐观状态更新。

        - ``expected_state`` 为 None 时不检查当前状态；
        - 可通过关键字字段更新 last_error/error_kind/not_before/result 等；
        - 成功返回 True，状态不匹配（被并发改动）返回 False。
        """
        field_map = self._prepare_fields(new_state, now, fields)

        # 参数顺序须与 SQL 占位符一致：state 在前，其后字段值，最后 id
        sets = ", ".join(f"{k} = ?" for k in field_map)
        sql = "UPDATE tasks SET state = ?"
        params: list[Any] = [new_state.value]
        if sets:
            sql += ", " + sets
            params.extend(field_map.values())
        sql += " WHERE id = ?"
        params.append(task_id)

        if expected_state is not None:
            expected = (
                (expected_state,)
                if isinstance(expected_state, TaskState)
                else tuple(expected_state)
            )
            marks = ",".join("?" for _ in expected)
            sql += f" AND state IN ({marks})"
            params.extend(s.value for s in expected)

        with self._transaction():
            cur = self.connection.execute(sql, params)
            changed = cur.rowcount == 1
        return changed

    def clear_lease(self, task_id: str) -> None:
        with self._transaction():
            self.connection.execute(
                "UPDATE tasks SET lease_owner = NULL, lease_expires_at = NULL "
                "WHERE id = ?",
                (task_id,),
            )

    def recover_orphans(self, now: float) -> list[str]:
        """启动恢复：把遗留的 RUNNING 任务退回 PENDING 重新调度。

        新进程使用全新的 owner id，旧租约必然不匹配；这些任务是上次进程
        被杀死/崩溃时正在执行的任务。领取时已预先 +1 的 attempts 在此回滚，
        使“真实执行次数 == attempts”，保证总执行次数恰为 max_attempts：
        崩溃的那次尝试会在重新领取时重新计数并重新执行，
        绝不会静默成功，也不会超出次数上限。
        """
        recovered: list[str] = []
        with self._transaction():
            rows = self.connection.execute(
                "SELECT id FROM tasks WHERE state = ?",
                (TaskState.RUNNING.value,),
            ).fetchall()
            for r in rows:
                self.connection.execute(
                    "UPDATE tasks SET state = ?, not_before = ?, "
                    "attempts = MAX(0, attempts - 1), "
                    "updated_at = ?, lease_owner = NULL, lease_expires_at = NULL "
                    "WHERE id = ? AND state = ?",
                    (
                        TaskState.PENDING.value,
                        now,
                        now,
                        r["id"],
                        TaskState.RUNNING.value,
                    ),
                )
                recovered.append(r["id"])
        return recovered

    # ---------- 流程写 ----------

    def insert_workflow(
        self,
        workflow: Workflow,
        tasks: list[Task],
        dependencies: list[tuple[str, str]],
    ) -> None:
        """在单事务内写入流程元数据、全部任务与依赖边。

        - 无依赖的任务以 PENDING 写入，立即可被调度；
        - 有依赖的任务以 BLOCKED 写入，不参与领取、不占并发名额。
        幂等键唯一冲突或其它完整性冲突时整体回滚并抛出 IntegrityError。
        """
        now = workflow.created_at
        with self._transaction():
            self.connection.execute(
                "INSERT INTO workflows (id, name, created_at) VALUES (?, ?, ?)",
                (workflow.id, workflow.name, now),
            )
            for task in tasks:
                row = self._task_to_row(task)
                cols = ", ".join(row.keys())
                marks = ", ".join("?" for _ in row)
                self.connection.execute(
                    f"INSERT INTO tasks ({cols}) VALUES ({marks})",
                    tuple(row.values()),
                )
            for task_key, depends_on_key in dependencies:
                self.connection.execute(
                    "INSERT INTO task_dependencies "
                    "(workflow_id, task_key, depends_on_key) VALUES (?, ?, ?)",
                    (workflow.id, task_key, depends_on_key),
                )

    # ---------- 流程依赖传播（全部在单个写事务内完成） ----------

    def settle_terminal(
        self,
        task_id: str,
        expected_state: TaskState | tuple[TaskState, ...] | None,
        new_state: TaskState,
        now: float | None = None,
        **fields: Any,
    ) -> TerminalSettlement:
        """以乐观守卫把任务落入终态，并在**同一事务**内传播依赖结果。

        - 成功（SUCCEEDED）：把“依赖已全部成功”的直接下游 BLOCKED 任务激活为
          PENDING，使其可被领取；
        - 失败/死信/取消：把所有尚未开始的传递下游带停为 SKIPPED，并记录
          ``skipped_by_task_id`` 与可区分的 ``error_kind``；
        - 状态守卫不匹配（changed=False）时不写入、不传播，保证上游结果只有
          一个权威判定，绝不会一边当成功激活、一边当失败带停。
        """
        field_map = self._prepare_fields(new_state, now, fields)
        sets = ", ".join(f"{k} = ?" for k in field_map)
        sql = "UPDATE tasks SET state = ?"
        params: list[Any] = [new_state.value]
        if sets:
            sql += ", " + sets
            params.extend(field_map.values())
        sql += " WHERE id = ?"
        params.append(task_id)

        if expected_state is not None:
            expected = (
                (expected_state,)
                if isinstance(expected_state, TaskState)
                else tuple(expected_state)
            )
            marks = ",".join("?" for _ in expected)
            sql += f" AND state IN ({marks})"
            params.extend(s.value for s in expected)

        result = TerminalSettlement(changed=False)
        with self._transaction():
            conn = self.connection
            cur = conn.execute(sql, params)
            if cur.rowcount != 1:
                return result  # 守卫失败：什么都不做
            result.changed = True

            root = conn.execute(
                f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            workflow_id = root["workflow_id"]
            if workflow_id is None:
                return result  # 普通任务：无依赖传播

            if new_state == TaskState.SUCCEEDED:
                result.activated = self._activate_ready_dependents_tx(
                    conn, workflow_id, root["task_key"], root["updated_at"]
                )
            else:
                skip_kind = _skip_kind_for(new_state, root["error_kind"])
                result.skipped = self._cascade_skip_tx(
                    conn,
                    workflow_id=workflow_id,
                    root_task_id=task_id,
                    root_task_key=root["task_key"],
                    root_state=new_state,
                    skip_kind=skip_kind,
                    now=root["updated_at"],
                )
        return result

    def _activate_ready_dependents_tx(
        self,
        conn: sqlite3.Connection,
        workflow_id: str,
        upstream_key: str,
        now: float,
    ) -> list[str]:
        """成功传播：激活直接下游中“全部依赖都已成功”的 BLOCKED 任务。"""
        activated: list[str] = []
        candidates = [
            r["task_key"]
            for r in conn.execute(
                "SELECT DISTINCT task_key FROM task_dependencies "
                "WHERE workflow_id = ? AND depends_on_key = ?",
                (workflow_id, upstream_key),
            )
        ]
        for key in candidates:
            row = conn.execute(
                f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
                "WHERE workflow_id = ? AND task_key = ?",
                (workflow_id, key),
            ).fetchone()
            if row is None or row["state"] != TaskState.BLOCKED.value:
                continue
            unmet = conn.execute(
                "SELECT 1 FROM task_dependencies d JOIN tasks t "
                "ON t.workflow_id = d.workflow_id AND t.task_key = d.depends_on_key "
                "WHERE d.workflow_id = ? AND d.task_key = ? "
                "AND t.state <> ? LIMIT 1",
                (workflow_id, key, TaskState.SUCCEEDED.value),
            ).fetchone()
            if unmet is not None:
                continue
            updated = conn.execute(
                "UPDATE tasks SET state = ?, not_before = ?, updated_at = ? "
                "WHERE id = ? AND state = ?",
                (TaskState.PENDING.value, now, now, row["id"],
                 TaskState.BLOCKED.value),
            )
            if updated.rowcount == 1:
                activated.append(row["id"])
        return activated

    def _cascade_skip_tx(
        self,
        conn: sqlite3.Connection,
        *,
        workflow_id: str,
        root_task_id: str,
        root_task_key: str,
        root_state: TaskState,
        skip_kind: str,
        now: float,
    ) -> list[str]:
        """失败传播：把尚未开始的传递下游带停为 SKIPPED。

        队列元素携带 (任务key, 直接上游id, 直接上游state)。被带停任务记录的是
        触发它的“直接上游”，便于查询具体被谁带停；error_kind 全链路统一取自
        根任务终态，保证同一次传播原因一致、可区分。
        """
        skipped: list[str] = []
        # (task_key, upstream_id, upstream_state)
        queue: list[tuple[str, str, TaskState]] = [
            (root_task_key, root_task_id, root_state)
        ]
        while queue:
            parent_key, parent_id, parent_state = queue.pop(0)
            children = conn.execute(
                "SELECT DISTINCT task_key FROM task_dependencies "
                "WHERE workflow_id = ? AND depends_on_key = ?",
                (workflow_id, parent_key),
            ).fetchall()
            for child in children:
                child_key = child["task_key"]
                row = conn.execute(
                    f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
                    "WHERE workflow_id = ? AND task_key = ?",
                    (workflow_id, child_key),
                ).fetchone()
                if row is None:
                    continue
                child_state = TaskState(row["state"])
                if child_state in TERMINAL_STATES:
                    # 已终结（例如早已成功）：不覆盖。沿其下游继续的传播由
                    # reconcile 或该节点自身的结算负责，此处跳过即可。
                    continue
                if child_state == TaskState.RUNNING:
                    # 不变量：上游未成功时下游不可能被领取；防御性跳过。
                    continue
                message = (
                    f"skipped: upstream task {parent_id} ended with "
                    f"{parent_state.value}"
                )
                updated = conn.execute(
                    "UPDATE tasks SET state = ?, error_kind = ?, last_error = ?, "
                    "skipped_by_task_id = ?, finished_at = ?, updated_at = ?, "
                    "lease_owner = NULL, lease_expires_at = NULL "
                    "WHERE id = ? AND state NOT IN "
                    "('SUCCEEDED','FAILED','CANCELLED','DEAD_LETTER','SKIPPED')",
                    (TaskState.SKIPPED.value, skip_kind, message, parent_id,
                     now, now, row["id"]),
                )
                if updated.rowcount == 1:
                    skipped.append(row["id"])
                    queue.append((child_key, row["id"], TaskState.SKIPPED))
        return skipped

    def reconcile_workflows(self, now: float) -> dict[str, TerminalSettlement]:
        """启动对账：修复崩溃窗口中“上游已结算、下游未传播”的流程。

        逐条流程处理（每条一个事务）：
        - 存在非成功终态上游的 BLOCKED/排队任务 -> 级联 SKIPPED；
        - 全部依赖已成功的 BLOCKED 任务 -> 激活 PENDING。
        已成功任务保持不动（绝不重跑）；RUNNING 已由 recover_orphans 先退回
        PENDING，故此处只需处理排队态。幂等，可重复执行。
        """
        results: dict[str, TerminalSettlement] = {}
        for workflow_id in self.list_open_workflow_ids():
            with self._transaction():
                conn = self.connection
                tasks = self.list_workflow_tasks(workflow_id)
                settlement = TerminalSettlement(changed=True)

                # 1) 失败传播：以每个非成功终态任务为根做级联带停
                for task in tasks:
                    if task.state not in (
                        TaskState.FAILED,
                        TaskState.CANCELLED,
                        TaskState.DEAD_LETTER,
                        TaskState.SKIPPED,
                    ):
                        continue
                    if task.state == TaskState.SKIPPED:
                        kind = task.error_kind or ERROR_KIND_UPSTREAM_FAILED
                    else:
                        kind = _skip_kind_for(task.state, task.error_kind)
                    settlement.skipped.extend(self._cascade_skip_tx(
                        conn,
                        workflow_id=workflow_id,
                        root_task_id=task.id,
                        root_task_key=task.task_key,
                        root_state=task.state,
                        skip_kind=kind,
                        now=now,
                    ))

                # 2) 成功传播：刷新视图后激活依赖全部成功的 BLOCKED 任务
                fresh_rows = conn.execute(
                    f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
                    "WHERE workflow_id = ? ORDER BY created_at ASC, id ASC",
                    (workflow_id,),
                ).fetchall()
                for row in fresh_rows:
                    if row["state"] != TaskState.BLOCKED.value:
                        continue
                    key = row["task_key"]
                    unmet = conn.execute(
                        "SELECT 1 FROM task_dependencies d JOIN tasks t "
                        "ON t.workflow_id = d.workflow_id "
                        "AND t.task_key = d.depends_on_key "
                        "WHERE d.workflow_id = ? AND d.task_key = ? "
                        "AND t.state <> ? LIMIT 1",
                        (workflow_id, key, TaskState.SUCCEEDED.value),
                    ).fetchone()
                    if unmet is None:
                        updated = conn.execute(
                            "UPDATE tasks SET state = ?, not_before = ?, "
                            "updated_at = ? WHERE id = ? AND state = ?",
                            (TaskState.PENDING.value, now, now, row["id"],
                             TaskState.BLOCKED.value),
                        )
                        if updated.rowcount == 1:
                            settlement.activated.append(row["id"])
            results[workflow_id] = settlement
        return results


class IdempotencyKeyTaken(Exception):
    """幂等键唯一约束冲突（底层存储异常）。"""

    def __init__(self, idempotency_key: str | None):
        super().__init__(f"idempotency key already exists: {idempotency_key}")
        self.idempotency_key = idempotency_key


def new_owner_id() -> str:
    return uuid.uuid4().hex
