# Async Task Queue（可持久化异步任务编排）

一个基于 FastAPI + SQLite 的可持久化异步任务队列，保证任务在**并发提交、失败重试、
进程重启**三类场景下状态可预期：

- 幂等提交：同一幂等键重复提交不会产生重复执行；
- 有界并发 + 优先级调度：慢任务不阻塞其他任务，取消/超时及时释放资源；
- 失败重试与死信：可重试错误按指数退避重试，超次数进入死信并可查询原因与尝试次数；
- 崩溃恢复：进程重启后未完成任务不丢失、不重复执行，执行中断的任务不会被静默标记成功；
- **依赖与流程编排**：一次提交一组任务及它们之间的先后依赖，上游全部成功后下游才开始；
  上游失败/死信/取消/超时会把未开始的下游带停为明确的终态并记录来源与原因；
  可按整条流程查看进度，重启后流程不丢、不卡、已成功上游不重跑。

## 目录结构

```
taskqueue/
  config.py         运行/重试配置
  models.py         Task / Workflow 领域模型、任务与流程状态
  errors.py         可区分的失败原因（未知任务/流程/非法状态/幂等冲突/带停原因…）
  dbutil.py         单线程 DB actor：所有 SQLite 阻塞调用的唯一执行通道
  store.py          SQLite 持久化（原子领取、乐观状态更新、启动恢复、依赖传播、流程对账）
  retry.py          指数退避重试策略
  registry.py       任务处理器注册表与执行上下文（取消信号）
  workflow.py       流程提交规格与 DAG 校验（重复 key / 未知依赖 / 环）
  worker.py         调度与执行循环（并发上限、超时、取消、重试、死信、重启对账、日志）
  service.py        对外协调层：单任务 / 流程提交、查询、幂等去重、取消
  api.py            FastAPI 路由与错误码映射（/tasks、/workflows）
  app.py            应用组装与 lifespan 生命周期
  logging_setup.py  统一日志（task_id / attempts / decision）
main.py             ASGI 入口：app = create_app()
tests/              49 个测试，覆盖单任务与流程编排场景
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

 流程专属：
   ┌─────────┐ 依赖全部成功（上游成功事务内激活）  ┌────────┐
   │ BLOCKED │ ───────────────────────────────▶ │PENDING │
   └─────────┘                                  └────────┘
       │ 任一上游以 FAILED / DEAD_LETTER / CANCELLED 结束
       ▼
   ┌─────────┐
   │ SKIPPED │  (终态，从未执行，记录 skipped_by_task_id + UPSTREAM_* 原因)
   └─────────┘
```

- **PENDING**：已落库等待调度（含重启后退回的孤儿、被激活的流程任务）。
- **BLOCKED**：流程任务的依赖尚未全部成功。**不会被领取、不占并发名额**，
  因此同一条流程里其他满足条件的任务不会被它堵住。
- **RETRY_WAIT**：可重试失败后等待退避，`not_before` 到期前不会被领取。
- **RUNNING**：已被某个 worker 领取，带租约 `lease_owner / lease_expires_at`。
- **SUCCEEDED / FAILED / CANCELLED / DEAD_LETTER**：四类终态，不再被调度。
- **SKIPPED**：流程任务因上游以非成功终态结束而被带停，自身**从未执行**
  （`attempts=0`），记录触发它的直接上游 `skipped_by_task_id` 与可区分原因
  `error_kind`（`UPSTREAM_FAILED / UPSTREAM_DEAD_LETTER / UPSTREAM_CANCELLED /
  UPSTREAM_TIMEOUT`）。这是终态，绝不会被误算为成功。

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

### 提交一组带依赖的任务

通过 `TaskService.submit_workflow(WorkflowSpec)` 或 `POST /workflows` 一次提交
一组任务与依赖。每个任务用流程内唯一的 `key` 标识，用 `depends_on`（一组 key）
声明“必须等这些上游都成功后我才能开始”：

```python
from taskqueue.workflow import WorkflowSpec, WorkflowTaskSpec

spec = WorkflowSpec(name="order-pipeline", tasks=[
    WorkflowTaskSpec(key="reserve", task_type="reserve_stock", payload={"sku": 1}),
    WorkflowTaskSpec(key="pay",     task_type="charge_card", depends_on=["reserve"]),
    WorkflowTaskSpec(key="ship",    task_type="arrange_shipment", depends_on=["pay"]),
    WorkflowTaskSpec(key="notify",  task_type="send_receipt", depends_on=["pay"]),
])
view = await service.submit_workflow(spec)
workflow_id = view.workflow.id
```

提交前做结构校验（任一不满足直接 400 `validation_error`，整批不落库）：

- 每个任务必须有非空且流程内唯一的 `key`；
- `depends_on` 只能引用流程内已有的 key，不能依赖自己；
- 依赖图必须是**有向无环图（DAG）**，检测到环会被拒绝；
- 每个 `task_type` 都必须已注册处理器；批内幂等键不得重复，
  与库中既有任务幂等键冲突时整批回滚（409 `idempotency_conflict`）。

### 调度与带停语义

- **无依赖**任务以 `PENDING` 落库，立即可被调度；**有依赖**任务以 `BLOCKED`
  落库。`BLOCKED` 不进入领取查询，因此**不占并发名额**、不堵同流程其他任务。
- 某个上游**成功**后，在它落终态的**同一个数据库事务**里检查其直接下游：
  只有当某下游的**全部**依赖都已 `SUCCEEDED` 时才把它从 `BLOCKED` 激活为
  `PENDING`。激活与上游结算原子提交，不会出现“激活了但上游其实失败”。
- 某个上游以 `FAILED / DEAD_LETTER / CANCELLED`（含超时用尽进死信）结束时，
  在同一事务内把所有**尚未开始的传递下游**带停为 `SKIPPED`：
  - `SKIPPED` 是终态、`attempts=0`，不会再被调度，也不会被算作成功；
  - 每个被带停任务记录直接触发者 `skipped_by_task_id`（视图里给出
    `skipped_by_key`），并按上游终态区分 `error_kind`：
    `UPSTREAM_FAILED / UPSTREAM_DEAD_LETTER / UPSTREAM_CANCELLED /
    UPSTREAM_TIMEOUT`（超时在用尽重试后落死信，带停原因单独细分）。
- **判定稳定性**：上游结算是“单条带状态守卫的 UPDATE + 同事务传播”。
  守卫不命中（状态已被并发改动）则既不写终态也不传播，因此同一条上游的结果
  只有一个权威版本，绝不会一边被当成功激活下游、一边被当失败带停下游。

### 流程进度视图

`GET /workflows/{id}`（或 `service.get_workflow`）返回：

- 流程整体 `state`：`RUNNING`（仍有未终态任务）/ `SUCCEEDED`（全部成功）/
  `FAILED`（已结束但含非成功任务）；
- `state_counts`：各任务状态计数，用于进度展示；
- `tasks`：每个任务的 `key`、`depends_on`、`skipped_by_key` 以及完整任务信息
  （状态、`attempts`、`result`、`error_kind`、`last_error`、起止时间等）。

### 与单任务模式的兼容性

- 不带依赖的普通提交流程（`submit` / `POST /tasks`）**完全不变**：
  普通任务没有流程归属（`workflow_id/task_key` 均为 `None`），不写入流程表，
  终态结算时不做任何依赖传播。
- 流程任务复用同一套重试、死信、超时、取消与优先级机制；
  重试等待期间下游仍为 `BLOCKED`，只有上游**最终成功**才会放行。
- 取消一个尚未开始的流程任务（`PENDING/RETRY_WAIT/BLOCKED`）同样会级联带停
  其未开始的下游；取消运行中任务为协作式取消，落 `CANCELLED` 后再传播。
- 旧版本数据库文件会在启动时自动补齐新增列（`workflow_id / task_key /
  skipped_by_task_id`）与新表，无需手工迁移。

## 流程的崩溃恢复语义

- 上次进程遗留的 `RUNNING` 流程任务沿用既有孤儿恢复：退回 `PENDING`、回滚崩溃
  那次预增的 `attempts` 后重新执行，绝不静默成功；成功后再由同事务传播放行下游。
- worker 启动时额外执行一次**流程对账**（`reconcile_workflows`），修复崩溃窗口
  里“上游已结算、下游尚未传播”的流程：
  - 上游已非成功终态而下游还停在排队态的，补成 `SKIPPED` 并带正确原因；
  - 依赖已全部成功但仍是 `BLOCKED` 的，补激活为 `PENDING` 继续推进。
- 已 `SUCCEEDED` 的上游保持不动，**重启后不会再执行一遍**；对账幂等，可重复执行。

## 持久化与崩溃恢复语义

- 存储为 SQLite（WAL），关键不变量：
  - **原子领取**：单条事务内 `SELECT … ORDER BY priority DESC, created_at ASC`
    + `UPDATE … SET state=RUNNING, attempts=attempts+1`，同一条任务不会被并发重复领取。
  - **幂等唯一约束**：`idempotency_key` 上有部分唯一索引，数据库层兜底防止并发重复插入。
  - **乐观状态更新**：所有结算都带 `expected_state=RUNNING` 守卫；
    若状态已被并发改动，结果会被丢弃并记录 `SUCCESS_REJECTED`，
    **绝不把执行中断的任务静默标记为成功**。
  - **依赖传播同事务原子化**：上游落终态与“激活下游 / 级联带停下游”在同一个
    写事务内完成；守卫不命中则整次传播不执行，保证上游判定唯一、不会半激活半带停。
- **DB actor**：所有 SQLite 调用都在唯一的专用线程上 FIFO 串行执行。
  这既匹配“单连接需串行”的约束，也避免线程池 + 锁在高并发提交下因持锁等待
  文件锁而耗尽线程形成死锁。
- **启动恢复**：worker 启动时把上一进程遗留的 `RUNNING` 任务（owner 已不存在）
  退回 `PENDING`，并回滚其崩溃那一次预增的 `attempts`：
  - 崩溃的那次尝试会被重新执行并重新计数 → 不会静默成功，也不会少算；
  - 已用满次数的任务在最后一次重试失败后进入死信，总次数不超过 `max_attempts`；
  - `RETRY_WAIT/PENDING` 任务原样保留，到期继续；终态任务永不重复执行。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交单个任务（201），body：`task_type, payload, idempotency_key?, priority?, max_attempts?, timeout?` |
| GET  | `/tasks/{id}` | 查询任务状态/结果/错误 |
| GET  | `/tasks/by-idempotency-key/{key}` | 按幂等键查询 |
| POST | `/tasks/{id}/cancel` | 取消排队中/阻塞中（直接 CANCELLED 并级联带停下游）或运行中（协作取消）任务 |
| POST | `/workflows` | 一次提交一组任务与依赖（201），body：`name?, tasks[{key, task_type, payload?, depends_on?, ...}]` |
| GET  | `/workflows/{id}` | 查询流程整体进度、状态计数与逐任务详情（含 `skipped_by_key`） |
| GET  | `/health` | 健康检查 |

错误响应体统一为 `{"detail": {"error": "<code>", "message": ..., ...}}`，
`error` 取值：`unknown_task` / `unknown_workflow` / `invalid_state` /
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

# 运行全部测试（49 个，含并发幂等、重试成功、死信、重启恢复、依赖编排等）
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

# 提交一条带依赖的流程：reserve -> pay -> ship / notify
curl -s -X POST localhost:8000/workflows \
  -H 'content-type: application/json' \
  -d '{
    "name": "demo-flow",
    "tasks": [
      {"key": "reserve", "task_type": "flaky", "payload": {"n": 1}},
      {"key": "pay",     "task_type": "flaky", "depends_on": ["reserve"]},
      {"key": "ship",    "task_type": "flaky", "depends_on": ["pay"]},
      {"key": "notify",  "task_type": "flaky", "depends_on": ["pay"]}
    ]
  }'

# 查询整条流程进度（state / state_counts / 每个任务详情）
curl -s localhost:8000/workflows/<workflow_id>
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
| 依赖满足才执行（菱形/多上游）/ BLOCKED 不占并发名额 | `tests/test_workflow.py::test_diamond_workflow_runs_in_dependency_order`、`test_downstream_not_runnable_until_all_upstreams_succeed`、`test_blocked_tasks_do_not_consume_concurrency` |
| 上游失败/死信/超时带停下游且原因可区分 | `test_upstream_failure_skips_downstream_with_reason`、`test_upstream_dead_letter_skips_with_distinct_kind`、`test_upstream_timeout_skips_with_distinct_kind` |
| 取消上游带停下游（运行中/未开始） | `test_cancel_blocked_or_queued_task_cascades_skip`、`test_cancel_upstream_before_it_starts_cascades` |
| 流程进度视图 / 结构校验 / 幂等冲突 | `test_workflow_progress_view_and_unknown_404`、`test_workflow_validation_errors`、`test_workflow_idempotency_conflict_with_existing` |
| 重启后流程继续推进、已成功上游不重跑、失败补带停、孤儿重跑 | `tests/test_workflow_recovery.py` |
| 流程 HTTP 接口 | `tests/test_api.py`（`test_workflow_*`） |

## 日志

所有关键判定均以结构化字段输出，便于审计：

```
... INFO  task accepted           task_id=... attempts=0 decision=ACCEPTED priority=0
... INFO  dispatching task        task_id=... attempts=1 decision=RUN
... WARNING task failed ...       task_id=... attempts=1 decision=RETRY in 0.010s
... WARNING task settled: DEAD_LETTER reason=... task_id=... attempts=3 decision=DEAD_LETTER
... WARNING recovered orphaned running task after restart task_id=... decision=RECOVER
```

`decision` 取值包括：`ACCEPTED / IDEMPOTENT_REPLAY / WORKFLOW_ACCEPTED /
RUN / SUCCEEDED(activated=N) / RETRY in <delay>s / DEAD_LETTER /
FAILED_NON_RETRYABLE / CANCELLED(skipped=N) / DOWNSTREAM_SKIPPED count=N /
CANCEL_REQUESTED / RECOVER / WORKFLOW_RECONCILE activated=N skipped=N /
SUCCESS_REJECTED / SHUTDOWN` 等。
