# Async Task Queue（可持久化异步任务编排）

一个基于 FastAPI + SQLite 的可持久化异步任务队列，保证任务在**并发提交、失败重试、
进程重启**三类场景下状态可预期：

- 幂等提交：同一幂等键重复提交不会产生重复执行；
- 有界并发 + 优先级调度：慢任务不阻塞其他任务，取消/超时及时释放资源；
- 失败重试与死信：可重试错误按指数退避重试，超次数进入死信并可查询原因与尝试次数；
- 崩溃恢复：进程重启后未完成任务不丢失、不重复执行，执行中断的任务不会被静默标记成功。

## 目录结构

```
taskqueue/
  config.py         运行/重试配置
  models.py         Task 领域模型与任务状态枚举
  errors.py         可区分的失败原因（未知任务/非法状态/幂等冲突/可重试/不可重试…）
  dbutil.py         单线程 DB actor：所有 SQLite 阻塞调用的唯一执行通道
  store.py          SQLite 持久化（原子领取、乐观状态更新、启动恢复）
  retry.py          指数退避重试策略
  registry.py       任务处理器注册表与执行上下文（取消信号）
  worker.py         调度与执行循环（并发上限、超时、取消、重试、死信、日志）
  service.py        对外协调层：提交 / 查询 / 幂等去重 / 取消
  api.py            FastAPI 路由与错误码映射
  app.py            应用组装与 lifespan 生命周期
  logging_setup.py  统一日志（task_id / attempts / decision）
main.py             ASGI 入口：app = create_app()
tests/              30 个测试，覆盖核心场景
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
- **RETRY_WAIT**：可重试失败后等待退避，`not_before` 到期前不会被领取。
- **RUNNING**：已被某个 worker 领取，带租约 `lease_owner / lease_expires_at`。
- **SUCCEEDED / FAILED / CANCELLED / DEAD_LETTER**：四类终态，不再被调度。

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

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务（201），body：`task_type, payload, idempotency_key?, priority?, max_attempts?, timeout?` |
| GET  | `/tasks/{id}` | 查询任务状态/结果/错误 |
| GET  | `/tasks/by-idempotency-key/{key}` | 按幂等键查询 |
| POST | `/tasks/{id}/cancel` | 取消排队中（直接 CANCELLED）或运行中（协作取消）任务 |
| GET  | `/health` | 健康检查 |

错误响应体统一为 `{"detail": {"error": "<code>", "message": ..., ...}}`，
`error` 取值：`unknown_task` / `invalid_state` / `idempotency_conflict` /
`validation_error`（请求体字段级校验为 422）。

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

# 运行全部测试（30 个，含并发幂等、重试成功、死信、重启恢复等）
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
CANCEL_REQUESTED / RECOVER / SUCCESS_REJECTED / SHUTDOWN` 等。
