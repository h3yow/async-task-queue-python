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
from typing import Any

from taskqueue.models import TERMINAL_STATES, Task, TaskState

_SCHEMA = """
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

# 允许被调度器领取的状态
_CLAIMABLE_STATES = (TaskState.PENDING.value, TaskState.RETRY_WAIT.value)

# update_state 时不可由调用方直接覆写的内部字段
_RESERVED_COLUMNS = {"id", "state"}

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
        conn.executescript(_SCHEMA)
        self._conn = conn

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


class IdempotencyKeyTaken(Exception):
    """幂等键唯一约束冲突（底层存储异常）。"""

    def __init__(self, idempotency_key: str | None):
        super().__init__(f"idempotency key already exists: {idempotency_key}")
        self.idempotency_key = idempotency_key


def new_owner_id() -> str:
    return uuid.uuid4().hex
