# 晶圆厚度批次放行 API / Wafer Thickness Batch Release Service

厚度明细入湖前的漂移批次隔离服务。FastAPI + PostgreSQL，双实例后台处理，
同设备严格 FIFO、崩溃可接续、判定/参考历史/供数原子生效。

**已放行批次自动投递到独立分析库**（第二套 PostgreSQL + 独立接收 API），
供漂移复核；覆盖既有与后来放行批，隔离批不投递，参考批未交付前不发布依赖批，
同批同内容重投复用原回执、异内容拒绝，断连/源端崩溃/双实例接续均不漏批、
不重复入库、不误报已交付。

## 判定规则

对每个设备（`device_id`）独立、按持久化受理顺序（`accept_seq`）判定：

1. 该设备前 **3** 批直接 **放行 RELEASED**（基线批，无参考集）；
2. 第 4 批起，取该设备**最近 3 个已放行批**的中位数求均值 `ref_mean`；
3. 当前批中位数 `median` 与 `ref_mean` 的偏差 **> 5nm** → **隔离 QUARANTINED**；
   偏差恰好 5nm 仍放行（规则是“超过”）；
4. **隔离批不进入参考窗口**，不参与后续均值，也不会出现在供数接口；
5. 中位数：奇数片取中间值，偶数片取中间两片的均值。

## 架构

```
                    ┌─ api1 (FastAPI + decision worker + delivery worker) ─┐
nginx :8080 ────────┤                                                      ├─ db (源端 PostgreSQL)
        └ /analysis┤─ api2 (FastAPI + decision worker + delivery worker) ─┘
                    │
                    └─ receiver :8081 (FastAPI) ── analysis-db (独立分析库 PostgreSQL)
```

* **源端**照旧受理、判定、查询；新增 `delivery_outbox` 与后台投递 worker。
* **接收端**是独立进程 + 独立 PostgreSQL（`analysis-db`），只保存已放行批：
  整批晶圆明细、判定字段、参考批号、回执，可按批查询。
* 两个源实例各自运行投递 worker；租约 + `SKIP LOCKED` 保证同一时刻只有一个
  实例发送同一批。接收端按 `batch_no` + 内容 SHA-256 幂等。

## 投递可靠性设计

| 要求 | 实现 |
| --- | --- |
| 覆盖既有与后来放行批 | 新放行批在**判定事务内**写 outbox；reconciler 定期/启动回填所有 `RELEASED` 但无 outbox 的批（含功能上线前已放行批），`POST /deliveries/reconcile` 可手动触发 |
| 隔离批不投递 | outbox 行只为 `status='RELEASED'` 创建；接收端也会拒绝非 RELEASED 负载（422） |
| 接收明细与回执一起生效 | 接收端在**单事务**内插入批次 + 全部晶圆 + 回执 + 发布判定 |
| 同批同内容重投复用原回执 | `batch_no` 主键 + `content_sha256`；相同内容返回原 `receipt`（`reused=true`），`received_count++`，不新增明细 |
| 异内容拒绝 | 同 `batch_no`、不同 `content_sha256` → `409 Conflict`；源端标记该批投递 `REJECTED`，不影响本地判定 |
| 参考批未确认交付前不发布依赖批 | 接收端 `published=false` 直到所有 `reference_batch_nos` 都已接收且已发布；同事务内做链式释放（可处理乱序与依赖链） |
| 收件后断连 | 源端未收到 2xx/回执前绝不置 `DELIVERED`；网络错误按指数退避重试 |
| 源端记回执前退出 | 租约到期后任一实例可重新领取；接收端返回原回执，不重复入库 |
| 双实例接续 | `FOR UPDATE SKIP LOCKED` 原子领取 + 每行租约；未完成行保留 `PENDING/IN_FLIGHT` 由存活实例继续 |
| 不漏批、不重复、不误报 | `DELIVERED` 仅在 2xx 且回执 `batch_no`/内容摘要校验通过后写入；reconciler 兜底补建；接收端幂等 |
| 故障解除后自动重试 | 指数退避（默认 1s→60s，可配），接收端恢复后自动成功 |
| 投递失败不改原判定 | outbox 与 `batches` 分表，投递代码从不写判定字段 |
| 慢投递不挡上传/判定/查询 | 投递是独立后台任务：HTTP 调用期间不占数据库连接；慢投递仅阻塞其自身任务 |
| 源端可查逐批状态与回执 | `GET /deliveries/{batch_no}`、`GET /deliveries`（可按状态/设备过滤；隔离批返回 `NOT_APPLICABLE`） |

## 目录结构

```
app/
  config.py            环境变量配置、判定常量、投递/接收端参数
  schema.py            源端 PostgreSQL 表结构（含 delivery_outbox）
  db.py                asyncpg 连接池 + jsonb codec
  csvutil.py           严格 CSV 解析
  storage.py           原件内容寻址持久化
  service.py           受理（幂等/冲突）、查询、供数读模型
  worker.py            后台判定 worker（判定事务内写 outbox）
  delivery.py          投递 outbox、租约领取、HTTP 发送、重试与状态查询
  delivery_protocol.py 源端/接收端共享的规范化负载与内容摘要
  main.py              FastAPI 路由、生命周期（判定 + 投递 worker）
receiver/
  config.py            独立分析库连接与可选 Bearer token
  schema.py            analysis_batches / analysis_wafers
  service.py           接收、幂等回执、参考依赖发布、查询
  main.py              接收端 FastAPI
scripts/
  worker_harness.py    单实例判定 worker 驱动（提交/慢批/崩溃/空闲退出）
  delivery_harness.py  单实例投递 worker 驱动（含“收件后崩溃”演示）
  demo_local.py        本地双源 + 接收端端到端演示（含断连/重启/崩溃）
  demo_compose.sh      Docker Compose 栈演示
  start_test_db.py     便携 PostgreSQL
tests/                 pytest（47 个）：判定规则、并发、崩溃恢复、投递/接收端
deploy/nginx/          双实例轮询 + /analysis 反代接收端
docker-compose.yml     db + analysis-db + receiver + api1 + api2 + nginx
```

## 用 Docker Compose 启动

```bash
cp .env.example .env        # 可选：改口令 / 设置 DELIVERY_TOKEN
docker compose up --build -d
# 源端 API 文档（提交、判定、投递状态）
open http://localhost:8080/docs
# 接收端 API 文档（按批查询、已发布数据、阻塞依赖、回执）
open http://localhost:8081/docs
# 端到端演示（判定 / 并发 / 崩溃 / 投递 / 接收端断连与重启 / 收件后源端崩溃）
bash scripts/demo_compose.sh
```

两个源实例（`api1`、`api2`）各自服务 HTTP 并运行一个判定 worker 和一个投递
worker，前置 nginx 在 `:8080` 轮询，并把 `/analysis/*` 反代到接收端；接收端
另在 `:8081` 直接暴露。PostgreSQL（源端/分析端）与原件目录均为命名卷。

## 本地运行（无 Docker 时）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest pytest-asyncio httpx pgserver
# 启动一个便携 PostgreSQL 并导出 DSN，然后：
export DATABASE_DSN=...          # 见 scripts/start_test_db.py
uvicorn app.main:app --port 8000 # 源端，可启动第二个实例换端口
# 接收端使用另一个数据库（同服务器上的独立 database 即可）：
createdb -h ... analysis
ANALYSIS_DATABASE_DSN=<analysis dsn> uvicorn receiver.main:app --port 8001
DELIVERY_RECEIVER_URL=http://127.0.0.1:8001 <启动源端>
python -m scripts.demo_local     # 完整演示（含投递断连/重启/收件后崩溃）
```

## API 摘要

### 源端

| 方法/路径 | 说明 |
| --- | --- |
| `POST /batches` | multipart：`batch_no`、`device_id`、`file`(CSV)；成功 `202`，校验失败 `400`，冲突 `409` |
| `GET /batches/{batch_no}` | 判定结果、中位数、偏差、参考批号、原件路径、尝试次数、决定实例 |
| `GET /devices/{device_id}/batches` | 按设备列受理顺序与状态 |
| `GET /data/released` | 供数接口，**仅放行**批的晶圆明细，支持 device/batch 过滤与分页 |
| `GET /deliveries/{batch_no}` | 逐批投递状态、尝试次数、最后错误、租约、**回执**；隔离批 `NOT_APPLICABLE` |
| `GET /deliveries` | 投递任务列表，`status`（PENDING/IN_FLIGHT/DELIVERED/REJECTED）与 `device_id` 过滤 |
| `POST /deliveries/reconcile` | 立即回填漏建/上线前已放行的投递任务 |
| `GET /health` | 存活与数据库连通性 |
| `POST /internal/shutdown?token=` | 仅演示用；未设 `SHUTDOWN_TOKEN` 时 404 |

### 接收端

| 方法/路径 | 说明 |
| --- | --- |
| `POST /analysis/batches` | 接收一个已放行批；`201`（含 `receipt`），同内容重投 `reused=true`，异内容 `409`，结构非法 `422` |
| `GET /analysis/batches` | 按批列表；默认仅已发布，`published_only=false` 可看全部 |
| `GET /analysis/batches/{batch_no}` | 单批完整明细（晶圆 + 判定 + 参考批号 + 回执 + `published`） |
| `GET /analysis/data` | 分析读模型：仅已发布的批次与晶圆明细 |
| `GET /analysis/blocked` | 已收但因参考批未发布而阻塞的批次 + 缺失参考批号 |
| `GET /analysis/receipts/{batch_no}` | 投递回执 |
| `GET /health` | 存活与分析库连通性 |

可选 `DELIVERY_TOKEN`：源端以 `Authorization: Bearer` 发送，接收端一致时放行。

### 投递负载（规范化 JSON）

```json
{
  "batch_no": "B42", "device_id": "EQ-A", "accept_seq": 42,
  "status": "RELEASED", "median_nm": 504.0,
  "reference_mean_nm": 500.0, "deviation_nm": 4.0,
  "reference_batch_nos": ["B39", "B40", "B41"],
  "sha256": "<原始 CSV 的 SHA-256>",
  "decided_at": "2026-01-01T00:00:00+00:00",
  "wafers": [{"wafer_no": "W001", "thickness_nm": 504}]
}
```

内容身份是对上述规范化 JSON（字段排序、紧凑分隔）的 SHA-256，不包含投递
元数据（源实例、时间、尝试次数），因此同一批的重投摘要恒定。

### CSV 格式

```
wafer_no,thickness_nm
W001,512
W002,508
```

表头可选（也接受中文表头 `晶圆号,厚度nm`）；厚度必须是正整数；片号批内唯一；
每批 1–1000 片。

## 测试

```bash
DATABASE_DSN=<postgres dsn> python -m pytest -q
```

47 个用例覆盖：CSV 边界与非法输入、前三批基线、>5nm 隔离与临界 5nm 放行、
隔离批不进参考窗、滚动窗口滑动、幂等重试与 409 冲突、供数不可见隔离批、
双 worker 同设备 FIFO 与跨设备并行、小连接池下高并发受理无死锁、真实子进程
硬退出后的接续与无半批；以及投递端到端：隔离批不投递、既有批回填、逐批状态
与回执、同内容重投复用回执、异内容 409、参考批未交付前不发布依赖批（含乱序
到达的链式释放）、接收端停机后重试且判定不变、2xx 后响应丢失的安全重投、
双投递 worker 恰好一次、源端收件后硬退出由存活实例用原回执续投。

## 演示中的故障注入钩子（仅测试/演示用途）

与既有判定 worker 一致，均为“文件不存在即完全关闭”：

* `SLOW_MARKER_FILE`：`batch_id` 或 `batch_id:秒数`，判定时持设备锁睡眠；
* 同目录 `crash_batches.txt`：`batch_id`，判定提交前 `os._exit(1)`；
* 同目录 `delivery_slow_batches.txt`：`batch_no[:秒数]`，投递调用前睡眠
  （用于证明慢投递不影响上传/判定/查询）；
* 同目录 `delivery_crash_batches.txt`：`batch_no`，接收端已接受、源端尚未记录
  回执时 `os._exit(1)`（用于证明租约重试与原回执复用）。

生产环境不挂载/不写这些文件即可。
