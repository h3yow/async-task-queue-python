# Async Task Queue（可持久化异步任务编排）

一个基于 FastAPI + SQLite 的可持久化异步任务队列，保证任务在**并发提交、失败重试、
进程重启**三类场景下状态可预期：

- 幂等提交：同一幂等键重复提交不会产生重复执行；
- 有界并发 + 优先级调度：慢任务不阻塞其他任务，取消/超时及时释放资源；
- 失败重试与死信：可重试错误按指数退避重试，超次数进入死信并可查询原因与尝试次数；
- 依赖与流程编排：一次提交一组任务及其先后依赖，上游成功后下游自动放行，
  上游失败/取消/死信时下游被带停到明确的终态并可追溯原因；
- 崩溃恢复：进程重启后未完成任务不丢失、不重复执行，执行中断的任务不会被静默标记成功，
  流程也不会卡住——已成功的上游不重跑，未完成的依赖关系继续推进。

## 目录结构

```
taskqueue/
  config.py         运行/重试配置
  models.py         Task 领域模型、任务状态枚举、流程视图（FlowStatus/FlowTaskSpec）
  errors.py         可区分的失败原因（未知任务/未知流程/非法状态/幂等冲突/可重试…）
  dbutil.py         单线程 DB actor：所有 SQLite 阻塞调用的唯一执行通道
  store.py          SQLite 持久化（原子领取、乐观状态更新、依赖边、下游传播、启动恢复）
  retry.py          指数退避重试策略
  registry.py       任务处理器注册表与执行上下文（取消信号）
  worker.py         调度与执行循环（并发上限、超时、取消、重试、死信、终态传播、日志）
  service.py        对外协调层：提交 / 流程提交 / 查询 / 幂等去重 / 取消
  api.py            FastAPI 路由与错误码映射（/tasks 与 /flows）
  app.py            应用组装与 lifespan 生命周期
  logging_setup.py  统一日志（task_id / attempts / decision）
main.py             ASGI 入口：app = create_app()
tests/              48 个测试，覆盖核心场景
```

## 任务状态机

```
                         ┌──────────────── 提交 ────────────────┐
                         ▼                                        │
                     ┌────────┐  原子领取(claim)   ┌─────────┐    │
        提交 ──────▶ │PENDING │ ─────────────────▶ │ RUNNING │    │
                     └────────┘                    └─────────┘    │
                         ▲                              │         │
                         │                              ├─成功──▶ SUCCEEDED (终态)
            重启恢复孤儿  │                              ├─不可重试▶ FAILED (终态)
                         │                              ├─主动取消▶ CANCELLED (终态)
                         │                              │
                     ┌────────────┐ 退避到期被重新领取   │
                     │ RETRY_WAIT │ ◀────可重试失败─────┤
                     └────────────┘                     │
                         │  重试次数耗尽                 │
                         └──────────────▶ DEAD_LETTER ◀──┘  (终态)
```

- **PENDING**：已落库等待调度（含重启后退回的孤儿）。
- **BLOCKED**：流程内等待上游依赖；**不可被调度领取，不占并发名额**。
- **RETRY_WAIT**：可重试失败后等待退避，`not_before` 到期前不会被领取。
- **RUNNING**：已被某个 worker 领取，带租约 `lease_owner / lease_expires_at`。
- **SUCCEEDED / FAILED / CANCELLED / DEAD_LETTER / SKIPPED**：五类终态，不再被调度。
  `SKIPPED` 表示因上游未成功而被带停，**从未执行，也不算成功**。

合法转换之外的操作一律拒绝：
- 查询/取消**未知任务** → `unknown_task`（404）；
- 取消**已终态**任务 → `invalid_state`（409），错误体带 `current_state`；
- 同一幂等键以**不同** task_type/payload 提交 → `idempotency_conflict`（409），
  错误体带已存在的 `existing_task_id`；同参重复提交视为重放，返回原任务、不重复执行。

## 重试与死信策略

- 处理器可抛出两类业务错误：
  - `RetryableTaskError`：可重试；
  - `NonRetryableTaskError`：立即 FAILED。
  - 未声明的普通异常保守按**可重试**处理；`asyncio.wait_for` 超时按 `TIMEOUT`
    （可重试）处理。
- 退避（第 n 次失败后）：
  `delay = min(max_delay, base_delay * multiplier ** (n - 1))`。
- 每条任务有独立 `max_attempts`（默认取 `RetryConfig.max_attempts`）。
  **总执行次数恰为 max_attempts**；最后一次仍失败则进入 **DEAD_LETTER**。
- 死信任务持久化 `attempts`、`error_kind`（RETRYABLE / NON_RETRYABLE / TIMEOUT /
  CANCELLED / UNKNOWN_TASK_TYPE）与 `last_error`，可通过查询接口区分失败原因。

## 并发与调度语义

- `max_concurrency` 由 `asyncio.Semaphore` 强制；调度循环本身不执行任务，
  慢任务/长任务运行在独立任务中，**不会阻塞调度循环或其他任务**。
- 领取顺序：`priority DESC, created_at ASC`，且 `not_before <= now`
  （高优先级先执行，同优先级 FIFO，退避未到期不执行）。
- 每条任务独立超时；超时或取消后信号量与运行计数都会在 `finally` 中释放，
  **不影响后续任务**。取消为协作式：处理器可通过
  `ExecutionContext.cancel_event` 感知并提前退出。

## 依赖与流程编排

一组有先后关系的任务可以作为一条**流程（flow）**一次提交：

```bash
curl -s -X POST localhost:8000/flows -H 'content-type: application/json' -d '{
  "tasks": [
    {"key": "extract",  "task_type": "etl_extract"},
    {"key": "transform","task_type": "etl_transform", "depends_on": ["extract"]},
    {"key": "load",     "task_type": "etl_load",      "depends_on": ["transform"]}
  ]
}'
```

- 每个任务用流程内唯一的 `key` 命名，`depends_on` 按 key 引用上游；
  支持扇入/扇出（菱形依赖）。提交时校验：空流程、重复 key、依赖未知 key、
  自依赖、**依赖环**、未注册的 `task_type` 一律以 `validation_error`（400）拒绝，
  且**不会产生任何数据**。
- 全部任务与依赖边在**一个事务**内落库：无依赖的任务直接 `PENDING`，
  有依赖的进入 `BLOCKED`。`BLOCKED` 不会被调度器领取，也**不占并发名额**，
  因此同一条流程里其他可运行的任务（以及无关的普通任务）不会被堵住。
- 上游进入终态后确定性地带动下游（同一事务内沿依赖边级联传播）：
  - 全部上游 `SUCCEEDED` → 下游放行（`BLOCKED → PENDING`），按正常调度执行；
  - 任一上游 `FAILED / CANCELLED / DEAD_LETTER / SKIPPED` → 下游落 `SKIPPED` 终态，
    并记录 `blocked_by`（带停它的上游任务 id）与可区分的 `error_kind`：
    `UPSTREAM_FAILED / UPSTREAM_CANCELLED / UPSTREAM_DEAD_LETTER / UPSTREAM_SKIPPED`。
    被跳过的任务会进一步级联带停它自己的下游。
- 判定只读取上游已落库的终态事实，且状态更新带 `state='BLOCKED'` 守卫：
  同一个上游的结果不会被一边当成功一边当失败，下游不会时跑时不跑。
- `BLOCKED` 中的任务也可以被直接取消（走 `POST /tasks/{id}/cancel`，
  与排队中任务相同的语义）。

### 流程进度查询

```bash
curl -s localhost:8000/flows/<flow_id>
```

返回整体状态与每个成员任务的明细（`state / attempts / error_kind /
last_error / blocked_by / finished_at` 等）。整体状态由成员任务推导：

| 整体状态 | 条件 |
| --- | --- |
| `RUNNING` | 还有任务未进入终态 |
| `SUCCEEDED` | 全部任务 `SUCCEEDED` |
| `FAILED` | 存在 `FAILED` 或 `DEAD_LETTER` 任务 |
| `CANCELLED` | 全部终结，无失败，但存在 `CANCELLED` 任务 |

### 重启后的流程恢复

- 流程不丢：任务与依赖边都持久化在 SQLite，重启后原样可见；
- 不卡住：worker 启动时除了恢复孤儿 `RUNNING` 任务，还会**全量重新评估所有
  `BLOCKED` 任务**——若上次进程崩溃在“上游落终态”与“传播到下游”之间，
  重启后补做传播（放行或带停），级联也能收敛；
- 不重复：已 `SUCCEEDED` 的上游永不重跑；执行中崩溃的上游按孤儿恢复重跑
  （崩溃那次不计入真实执行次数），成功后下游继续推进。

### 与单任务模式的兼容范围

- 不带依赖的普通任务：**行为完全不变**，`POST /tasks` 的入参、返回与状态机
  都不受影响（回归由既有测试套件保证）；
- 流程内任务与普通任务共享同一调度器、并发上限、重试/死信/取消语义，
  可以混跑互不影响；
- 流程内任务同样支持 `idempotency_key / priority / max_attempts / timeout`，
  语义与单任务一致；流程提交本身是整体校验、整体落库，
  目前**没有流程级幂等键**——调用方如需防重，可为成员任务指定幂等键；
- 跨流程依赖（`depends_on` 引用另一条流程的任务）暂不支持，
  依赖只能引用同一次提交内的 key。

## 持久化与崩溃恢复语义

- 存储为 SQLite（WAL），关键不变量：
  - **原子领取**：单条事务内 `SELECT … ORDER BY priority DESC, created_at ASC`
    + `UPDATE … SET state=RUNNING, attempts=attempts+1`，同一条任务不会被并发重复领取。
  - **幂等唯一约束**：`idempotency_key` 上有部分唯一索引，数据库层兜底防止并发重复插入。
  - **乐观状态更新**：所有结算都带 `expected_state=RUNNING` 守卫；
    若状态已被并发改动，结果会被丢弃并记录 `SUCCESS_REJECTED`，
    **绝不把执行中断的任务静默标记为成功**。
- **DB actor**：所有 SQLite 调用都在唯一的专用线程上 FIFO 串行执行。
  这既匹配“单连接需串行”的约束，也避免线程池 + 锁在高并发提交下因持锁等待
  文件锁而耗尽线程形成死锁。
- **启动恢复**：worker 启动时把上一进程遗留的 `RUNNING` 任务（owner 已不存在）
  退回 `PENDING`，并回滚其崩溃那一次预增的 `attempts`：
  - 崩溃的那次尝试会被重新执行并重新计数 → 不会静默成功，也不会少算；
  - 已用满次数的任务在最后一次重试失败后进入死信，总次数不超过 `max_attempts`；
  - `RETRY_WAIT/PENDING` 任务原样保留，到期继续；终态任务永不重复执行。

## 沿用旧版本数据库（schema 自动升级）

依赖/流程能力为 `tasks` 表新增了 `flow_id / flow_key / blocked_by` 三列，
并新增 `task_dependencies` 表与相关索引。**直接拿上一版创建的数据库文件
启动即可**，服务在连接时自动完成轻量迁移，不会报错退出：

- 迁移顺序固定为「建表（缺才建）→ 按名补齐缺失列 → 建索引」，
  索引永远排在补列之后，因此旧库缺列不会导致启动失败；
- 迁移**幂等可重复**：对同一个库反复启动/重启结果一致，
  不报错、不丢任务、不改坏状态；
- 老任务的所有字段（状态、结果、错误信息、attempts、幂等键）原样保留，
  升级后照常查询；老任务的新列为 `NULL`，语义等同普通单任务；
- 老库中未终结的任务升级后继续按既有语义处理：
  `PENDING/RETRY_WAIT` 到期继续调度，遗留 `RUNNING` 按孤儿恢复重跑；
- 升级后流程能力立即可用：可在该库上提交带依赖的流程，
  重启后流程接着推进，已成功的上游不重跑，
  因上游未成功而停下的下游会被收尾为 `SKIPPED` 而不是一直挂着；
- 全新空库的行为与之前完全一致（直接建出完整 schema）。

本地手动验证（用上一版格式的库启动）：

```bash
# 1) 用上一版建表语句准备一个“老库”并写入一条任务
python - <<'EOF'
import sqlite3
conn = sqlite3.connect("legacy.db")
conn.executescript("""
CREATE TABLE tasks (
    id TEXT PRIMARY KEY, task_type TEXT NOT NULL, payload_json TEXT NOT NULL,
    idempotency_key TEXT, state TEXT NOT NULL, priority INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
    timeout REAL NOT NULL, not_before REAL NOT NULL DEFAULT 0,
    result_json TEXT, last_error TEXT, error_kind TEXT,
    created_at REAL NOT NULL, updated_at REAL NOT NULL,
    started_at REAL, finished_at REAL, lease_owner TEXT, lease_expires_at REAL
);
""")
conn.execute(
    "INSERT INTO tasks VALUES ('old-1','flaky','{}',NULL,'PENDING',0,0,3,"
    "60,0,NULL,NULL,NULL,0,0,NULL,NULL,NULL,NULL)")
conn.commit(); conn.close()
EOF

# 2) 直接指向老库启动：正常起来，老任务被继续调度
TASKQUEUE_DB=legacy.db uv run uvicorn main:app
curl -s localhost:8000/tasks/old-1    # 状态/结果可查，PENDING 会被执行

# 3) 老库上提交流程、重启后续跑，行为与新库一致
curl -s -X POST localhost:8000/flows -H 'content-type: application/json' -d '{
  "tasks": [{"key":"a","task_type":"flaky"},
            {"key":"b","task_type":"flaky","depends_on":["a"]}]}'
```

自动化覆盖见 `tests/test_migration.py`（用上一版格式的真实库文件驱动）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务（201），body：`task_type, payload, idempotency_key?, priority?, max_attempts?, timeout?` |
| GET  | `/tasks/{id}` | 查询任务状态/结果/错误 |
| GET  | `/tasks/by-idempotency-key/{key}` | 按幂等键查询 |
| POST | `/tasks/{id}/cancel` | 取消排队中/阻塞中（直接 CANCELLED）或运行中（协作取消）任务 |
| POST | `/flows` | 提交流程（201），body：`tasks: [{key, task_type, payload?, depends_on?, ...}]` |
| GET  | `/flows/{flow_id}` | 查询流程整体状态与成员任务明细 |
| GET  | `/health` | 健康检查 |

错误响应体统一为 `{"detail": {"error": "<code>", "message": ..., ...}}`，
`error` 取值：`unknown_task` / `unknown_flow` / `invalid_state` /
`idempotency_conflict` / `validation_error`（请求体字段级校验为 422）。

### 处理器示例

```python
from taskqueue.app import create_app
from taskqueue.errors import RetryableTaskError
from taskqueue.registry import HandlerRegistry

attempts = 0

async def flaky(payload, ctx):
    global attempts
    attempts += 1
    if attempts < 3:
        raise RetryableTaskError("transient")   # 触发退避重试
    return {"ok": True, "echo": payload}

def configure(reg: HandlerRegistry) -> None:
    reg.register("flaky", flaky)                # 处理器必须是 async 函数

app = create_app(configure=configure)
```

长任务可响应取消：

```python
async def long_job(payload, ctx):
    while not ctx.cancelled:
        await do_one_chunk()
```

## 本地验证

依赖与环境由 [uv](https://docs.astral.sh/uv/) 管理（Python ≥ 3.14）。

```bash
# 安装依赖（含 pytest / httpx / ruff）
uv sync

# 运行全部测试（58 个，含并发幂等、重试成功、死信、流程编排、重启恢复、旧库升级等）
uv run pytest -q

# 静态检查
uv run ruff check taskqueue/ tests/ main.py

# 启动服务（默认 SQLite 文件 taskqueue.db，可用 TASKQUEUE_DB 覆盖）
uv run uvicorn main:app --reload

# 提交一个任务
curl -s -X POST localhost:8000/tasks \
  -H 'content-type: application/json' \
  -d '{"task_type":"flaky","payload":{"n":1},"idempotency_key":"demo-1"}'

# 查询
curl -s localhost:8000/tasks/<task_id>

# 提交一条三步流程并查询整体进度
curl -s -X POST localhost:8000/flows -H 'content-type: application/json' -d '{
  "tasks": [
    {"key": "a", "task_type": "flaky"},
    {"key": "b", "task_type": "flaky", "depends_on": ["a"]}
  ]}'
curl -s localhost:8000/flows/<flow_id>
```

关键测试与需求对应关系：

| 需求场景 | 测试 |
| --- | --- |
| 同一幂等键并发提交不重复执行 | `tests/test_service.py::test_concurrent_same_idempotency_key_runs_once`（20 并发） |
| 同参重放 / 异参冲突可区分 | `test_idempotent_serial_submit_runs_once` / `test_idempotent_conflicting_payload_rejected` |
| 重试后成功 | `test_retry_then_success` |
| 重试耗尽进入死信、可查询原因与次数 | `test_retries_exhausted_go_to_dead_letter` / `test_dead_letter_queryable_reason_and_attempts` |
| 不可重试立即失败 | `test_non_retryable_fails_immediately` |
| 重启恢复 / 不静默成功 / 不重复执行 | `tests/test_recovery.py` |
| 并发上限 / 优先级 / 超时释放 | `tests/test_scheduling.py` |
| 依赖满足才执行 / BLOCKED 不占并发名额 | `tests/test_flows.py::test_flow_executes_in_dependency_order` / `test_blocked_task_does_not_hold_concurrency_slot` |
| 上游失败/死信/取消带停下游且原因可区分 | `test_upstream_failure_skips_downstream_with_reason` / `test_upstream_dead_letter_skips_downstream` / `test_upstream_cancelled_while_running_skips_downstream` |
| 流程进度视图 | `test_flow_progress_view_mid_flight` |
| 重启后流程继续推进、已成功上游不重跑 | `test_restart_continues_flow_without_rerunning_succeeded` / `test_restart_skips_downstream_of_failed_upstream` / `test_restart_recovers_running_upstream_and_continues` |
| 旧版本数据库启动不报错、老任务原样保留 | `tests/test_migration.py::test_service_starts_on_legacy_db_and_preserves_tasks` / `test_legacy_db_gains_new_schema_objects` |
| 旧库未完成任务升级后继续调度 / 孤儿恢复 | `test_legacy_pending_task_is_scheduled_after_upgrade` / `test_legacy_running_task_is_recovered_after_upgrade` |
| 旧库上流程编排、带停、重启续跑不重跑 | `test_flow_runs_on_upgraded_legacy_db` / `test_flow_on_legacy_db_skips_downstream_of_failed_upstream` / `test_restart_on_upgraded_db_continues_flow_without_rerun` |
| 升级幂等可重复 / 新库行为不变 | `test_repeated_restarts_on_legacy_db_are_idempotent` / `test_store_connect_is_idempotent_on_legacy_db` / `test_fresh_db_has_full_schema` |
| 流程 API 端到端 | `tests/test_api.py::test_flow_submit_and_progress_via_api` 等 |

## 日志

所有关键判定均以结构化字段输出，便于审计：

```
... INFO  task accepted           task_id=... attempts=0 decision=ACCEPTED priority=0
... INFO  dispatching task        task_id=... attempts=1 decision=RUN
... WARNING task failed ...       task_id=... attempts=1 decision=RETRY in 0.010s
... WARNING task settled: DEAD_LETTER reason=... task_id=... attempts=3 decision=DEAD_LETTER
... WARNING recovered orphaned running task after restart task_id=... decision=RECOVER
```

`decision` 取值包括：`ACCEPTED / IDEMPOTENT_REPLAY / RUN / SUCCEEDED /
RETRY in <delay>s / DEAD_LETTER / FAILED_NON_RETRYABLE / CANCELLED /
CANCEL_REQUESTED / RECOVER / SUCCESS_REJECTED / SHUTDOWN /
FLOW_ACCEPTED / FLOW_UNBLOCKED / FLOW_SKIPPED by <upstream> (<state>) /
FLOW_RECOVER ...` 等。
