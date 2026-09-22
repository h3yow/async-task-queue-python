# 持久化异步任务编排服务

基于 FastAPI + SQLite(WAL) 的可持久化异步任务队列，支持：

- **提交 / 查询 / 取消**：重复提交同一幂等键不会产生重复执行；未知任务与非法状态转换被拒绝，
  且返回可区分的错误码。
- **可配置并发上限与优先级调度**：慢任务只占用自己的槽位，不阻塞其他任务；超时 / 取消后
  执行协程立即释放槽位。
- **失败重试与死信**：按指数退避重试，超过 `max_attempts` 进入死信，可查询失败原因与尝试次数。
- **重启恢复**：未完成任务不丢失、不重复执行；执行中断的任务绝不会被静默标记为成功。

## 1. 任务状态机

```
PENDING ──► RUNNING ──► SUCCEEDED
  │            │
  │            ├─► SCHEDULED ──► RUNNING        (可重试失败，等待退避)
  │            └─► DEAD_LETTER                  (尝试次数耗尽 / 中断后无剩余次数)
  │
  └─► CANCELLED                                 (排队中取消，或运行中响应取消标志)

进程重启时遗留的 RUNNING ──► SCHEDULED（仍有重试次数）
                        └─► DEAD_LETTER（次数已耗尽，原因记为 "task interrupted by process restart before completion"）
```

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `PENDING` | 已提交，等待空闲槽位（仅按优先级排序） | 否 |
| `SCHEDULED` | 重试退避中，等待 `run_after` 到期 | 否 |
| `RUNNING` | 正被某执行槽处理，带有领取令牌 `run_token` 与已递增的 `attempts` | 否 |
| `SUCCEEDED` | 成功，`result` 存 JSON 结果 | 是 |
| `CANCELLED` | 已取消，`error` 记录取消原因 | 是 |
| `DEAD_LETTER` | 死信，`error` 记录最终原因，`attempts` 记录实际尝试次数 | 是 |
| `FAILED` | 保留枚举（实际失败统一进 `DEAD_LETTER`） | 是 |

所有状态转换都在存储层单一收口（`TaskStore._update` / `claim_next` 事务），非法转换抛出
`invalid_state_transition`（HTTP 409，响应体带 `from_state` / `to_state`）。

**关键不变量**

1. 任务只能由 `claim_next` 的 `BEGIN IMMEDIATE` 事务从 `PENDING/SCHEDULED` 变为 `RUNNING`，
   且 `attempts` 在此事务内 +1——因此每次物理尝试恰好计数一次，崩溃发生在领取之后也不会丢计数。
2. 只有 `RUNNING` 能被标记为 `SUCCEEDED`；进程被杀时没有任何路径能把中断任务写成成功。
3. 终态不可逆。

## 2. 重试与死信策略

- 每个任务提交时可指定 `max_attempts`（含首次执行；默认 1，即不重试）。
- 退避形状由服务级 `RetryPolicy` 决定：
  `delay(n) = min(max_delay, base_delay * multiplier ** (n - 1))`（n 为已失败的尝试次数）。
- 判定依据（每次都会写日志，含任务标识、尝试次数与 basis）：
  - `basis=retryable_failure` → `SCHEDULED`，日志含 `retry_in=...`
  - `basis=attempts_exhausted` → `DEAD_LETTER`，`error` 形如
    `RuntimeError: ... (attempts exhausted: 3/3)`
  - `basis=handler_returned` → `SUCCEEDED`
  - `basis=cancellation` / `cancel observed ...` → `CANCELLED`
- 未注册任务类型在提交时即被拒绝（`unknown_task_type`，422）。
- 查询死信：`GET /tasks/dead-letter/list` 或按 `state=DEAD_LETTER` 过滤。

> 注意：重试成功要求**处理器自身幂等**——队列保证"至少一次"执行且尝试次数不超上限，
> 处理器的副作用去重应由业务侧依据幂等键完成（测试中的 `persistent_flaky` 演示了
> 用外部存储记录尝试进度的做法）。

## 3. 并发与调度语义

- `TASKQUEUE_CONCURRENCY`（默认 4）限制同时执行的处理器数量；领取顺序为
  **优先级 DESC → 到期时间 ASC → 创建时间 ASC**。
- 新任务（`PENDING`）不按提交时间排队，只按优先级；退避重试任务（`SCHEDULED`）在
  `run_after` 到期后才参与竞争。
- 每个处理器运行在 `asyncio.wait_for(timeout=default_timeout)` 内，超时抛 `TimeoutError`
  并走正常失败 / 重试 / 死信判定，槽位立即释放。
- 取消是"持久化意图"：`POST /tasks/{id}/cancel` 写入 `cancel_requested=1`；
  排队中的任务立即转 `CANCELLED`，运行中的任务由取消监听器轮询到标志后取消其协程。
  即使取消标志写入后进程立即崩溃，重启恢复也会把任务置为 `CANCELLED`。
- 处理器中的阻塞 I/O 应使用 `asyncio.to_thread` 等方式让出事件循环。

## 4. 持久化语义与重启恢复

- 存储：单文件 SQLite，`journal_mode=WAL`、`synchronous=NORMAL`；进程内单连接 + asyncio 锁
  串行化所有写操作，阻塞的 sqlite 调用放入线程，不卡事件循环。所有线程态 sqlite 调用都由
  在途计数器（`threading.Condition`）跟踪，`close()` 会等待真正在跑的语句结束后再关闭连接——
  即使等待方协程被取消（取消无法中止已在线程中的 sqlite 语句），也不会发生连接被提前释放的
  use-after-free。
- 幂等键在表上有 `UNIQUE` 约束；并发提交同一键时由数据库决出唯一胜者，其余提交返回
  同一 `task_id`（响应体 `created=false`）。
- 启动时 `recover_interrupted()` 在一个 `BEGIN IMMEDIATE` 事务内对账：
  - 上一进程遗留的 `RUNNING` 任务：**不视为成功**；`attempts < max_attempts` 则回到
    `SCHEDULED`（立即重试），否则进 `DEAD_LETTER`，原因固定为
    `task interrupted by process restart before completion`。
  - `PENDING/SCHEDULED` 且已请求取消的任务 → `CANCELLED`。
  - 终态任务（`SUCCEEDED/DEAD_LETTER/CANCELLED`）保持不变，不会重新执行。
- 多进程 / 多实例共享同一数据库时，`claim_next` 的原子事务保证同一任务不会被两个执行器
  同时领取（见 `tests/test_concurrent_workers.py`）。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务（body：`task_type`、`payload`、`idempotency_key?`、`priority?`、`max_attempts?`），202 返回任务记录，含 `created` 标记 |
| GET | `/tasks/{id}` | 查询任务；未知 id → 404 `task_not_found` |
| GET | `/tasks/by-idempotency-key/{key}` | 按幂等键查询 |
| POST | `/tasks/{id}/cancel` | 请求取消；终态 → 409 `invalid_state_transition` |
| GET | `/tasks?state=DEAD_LETTER` | 按状态列出任务 |
| GET | `/tasks/dead-letter/list` | 列出死信（含 `error` 与 `attempts`） |
| GET | `/stats` | 各状态任务计数与当前活动执行槽数 |

错误响应体形如：

```json
{"code": "invalid_state_transition",
 "message": "cannot cancel task ...: state SUCCEEDED is terminal",
 "task_id": "...", "from_state": "SUCCEEDED", "to_state": "CANCELLED"}
```

内置示例处理器：`echo`（回显）、`flaky`（前 N 次失败后成功，演示重试）、`slow`（睡眠）。

## 6. 本地验证方法

```bash
# 安装依赖（uv）
uv sync --dev

# 运行全部测试（29 个：幂等并发、错误区分、重试成功、死信、调度/超时/取消、重启恢复、HTTP、多执行器互斥）
uv run python -m pytest -q

# 启动服务（可用环境变量调参）
TASKQUEUE_CONCURRENCY=4 \
TASKQUEUE_TIMEOUT=30 \
TASKQUEUE_BASE_DELAY=0.5 \
uv run fastapi dev main.py
# 交互式文档：http://127.0.0.1:8000/docs
```

环境变量：`TASKQUEUE_DB`（默认 `./tasks.db`）、`TASKQUEUE_CONCURRENCY`、
`TASKQUEUE_TIMEOUT`、`TASKQUEUE_MAX_ATTEMPTS`、`TASKQUEUE_BASE_DELAY`、
`TASKQUEUE_MAX_DELAY`、`TASKQUEUE_MULTIPLIER`、`TASKQUEUE_POLL_INTERVAL`、
`TASKQUEUE_LOG_LEVEL`。

手动体验：

```bash
# 提交一个失败 2 次、允许 3 次尝试的任务
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"task_type":"flaky","payload":{"key":"demo","fail_times":2},"idempotency_key":"demo-1","max_attempts":3}'
# 用返回的 id 轮询，观察 RUNNING -> SCHEDULED -> ... -> SUCCEEDED 与 attempts 增长
curl -s localhost:8000/tasks/<id>
# 重复提交同一幂等键：返回同一 id，created=false，不会重复执行
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"task_type":"flaky","payload":{"key":"demo","fail_times":2},"idempotency_key":"demo-1","max_attempts":3}'
# 死信列表
curl -s localhost:8000/tasks/dead-letter/list
```

日志每行都带任务标识、`attempt=n/max`、目标状态与判定依据（`basis=...`），
启动时还会打印恢复汇总，例如：

```
... INFO taskqueue.app task service started: {'rescheduled': 1, 'dead_letter': 0, 'cancelled': 1}
... WARNING taskqueue.worker task=<id> attempt=1/3 state=SCHEDULED retry_in=0.500s basis=retryable_failure reason=RuntimeError: flaky failure #1 for demo
... ERROR taskqueue.worker task=<id> attempt=3/3 state=DEAD_LETTER basis=attempts_exhausted reason=RuntimeError: ...
```

## 7. 代码结构

```
taskqueue/
  models.py    # 状态枚举、状态机合法转换、RetryPolicy 退避、TaskRecord
  errors.py    # 可区分错误码与 HTTP 状态映射
  store.py     # SQLite/WAL：原子领取、状态守卫、重启恢复
  registry.py  # task_type -> 异步处理器
  worker.py    # 调度循环、执行槽、超时/取消、重试/死信判定与日志
  service.py   # 对外门面：submit/get/cancel/list/stats
  api.py       # FastAPI 路由
main.py        # 应用装配、生命周期、示例处理器、环境变量配置
tests/         # 29 个测试与场景夹具
```
