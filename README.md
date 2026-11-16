# 晶圆厚度批次放行 API / Wafer Thickness Batch Release Service

厚度明细入湖前的漂移批次隔离服务。FastAPI + PostgreSQL，双实例后台处理，
同设备严格 FIFO、崩溃可接续、判定/参考历史/供数原子生效。

## 判定规则

对每个设备（`device_id`）独立、按持久化受理顺序（`accept_seq`）判定：

1. 该设备前 **3** 批直接 **放行 RELEASED**（基线批，无参考集）；
2. 第 4 批起，取该设备**最近 3 个已放行批**的中位数求均值 `ref_mean`；
3. 当前批中位数 `median` 与 `ref_mean` 的偏差 **> 5nm** → **隔离 QUARANTINED**；
   偏差恰好 5nm 仍放行（规则是“超过”）；
4. **隔离批不进入参考窗口**，不参与后续均值，也不会出现在供数接口；
5. 中位数：奇数片取中间值，偶数片取中间两片的均值。

## 可靠性设计（如何满足要求）

| 要求 | 实现 |
| --- | --- |
| 原件持久保存后才受理 | `POST /batches` 先把原始字节以内容寻址（SHA-256）落盘并 `fsync`，再写库；见 `app/storage.py` |
| 同批号同设备同原件重试复用 | 落盘路径由 SHA-256 决定；库内按 `batch_no` 唯一，同 sha256 直接返回既有受理（`reused_retry=true`） |
| 异内容 / 异设备拒绝 | sha256 不同或 device 不同 → `409 Conflict` |
| 同设备慢批不被后批越过 | worker 每设备先取 `accept_seq` 最小的 PENDING 批；设备级会话咨询锁 `pg_try_advisory_lock`（非阻塞，拿不到就跳过该设备）串行化同设备 |
| 高并发受理不发生连接池饥饿 | 受理序号分配**不使用阻塞式咨询锁**，而靠 `(device_id, accept_seq)` 唯一约束 + 冲突重试；每个受理事务都很短，没有连接会在等锁时被占住，因此同设备再多并发提交也不会自死锁 |
| 不阻塞其他设备/上传/查询 | 锁是设备级而非全局；HTTP 请求与 worker 用同一连接池，互不阻塞 |
| 实例退出后存活实例接续 | 判定在**单事务**内完成；进程崩溃则事务回滚，批次保持 PENDING、`device_state` 不变，咨询锁随会话自动释放，存活实例下轮 poll（0.25s）接续 |
| 不重复推进参考、无半批 | 事务内 `SELECT ... FOR UPDATE` 锁 `device_state` 与批次行，决策字段与 `ref_batch_ids` 同事务提交；FIFO 序号断言 `accept_seq = last_decided_seq + 1` |
| 供数只给放行明细 | `/data/released` SQL 固定 `status='RELEASED'`，隔离批任何过滤条件都查不到 |

## 目录结构

```
app/
  config.py     环境变量配置与判定常量（前3批/参考3批/阈值5nm）
  schema.py     PostgreSQL 表结构
  db.py         asyncpg 连接池 + jsonb codec
  csvutil.py    严格 CSV 解析（1~1000片、片号批内唯一、厚度正整数）
  storage.py    原件内容寻址持久化（tmp+fsync+原子rename）
  service.py    受理（幂等/冲突）、查询、供数读模型
  worker.py     后台判定 worker（设备咨询锁 + 单事务原子判定）
  main.py       FastAPI 路由、生命周期（每实例启动一个 worker）
scripts/
  worker_harness.py  单实例 worker 驱动（提交/慢批/崩溃标记/空闲退出）
  demo_local.py      本地双实例端到端演示（便携 Postgres）
  demo_compose.sh    Docker Compose 栈演示
tests/          pytest：CSV、API 规则、双 worker 并发、崩溃恢复（23 个）
deploy/nginx/   双实例轮询负载均衡
docker-compose.yml  db + api1 + api2 + nginx
```

## 用 Docker Compose 启动

```bash
docker compose up --build -d
# API 文档（Swagger UI）
open http://localhost:8080/docs
# 端到端演示：受理/幂等/漂移/慢批FIFO/容器崩溃接续/供数隔离
bash scripts/demo_compose.sh
```

两个 API 实例（`api1`、`api2`）各自服务 HTTP 并运行一个后台 worker，
前置 nginx 在 `:8080` 轮询；PostgreSQL 与原件目录均为命名卷，实例重启不丢数据。

## 本地运行（无 Docker 时）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pgserver pytest pytest-asyncio httpx python-multipart
# 启动一个便携 PostgreSQL 并导出 DSN，然后：
export DATABASE_DSN=...          # 见 scripts/start_test_db.py
uvicorn app.main:app --port 8000 # 可启动第二个实例换端口即可
python -m scripts.demo_local     # 慢批 + 崩溃接续完整演示
```

## API 摘要

| 方法/路径 | 说明 |
| --- | --- |
| `POST /batches` | multipart：`batch_no`、`device_id`、`file`(CSV)；成功 `202`，校验失败 `400`，冲突 `409` |
| `GET /batches/{batch_no}` | 判定结果、中位数、偏差、参考批号、原件路径、尝试次数、决定实例 |
| `GET /devices/{device_id}/batches` | 按设备列受理顺序与状态 |
| `GET /data/released` | 供数接口，**仅放行**批的晶圆明细，支持 device/batch 过滤与分页 |
| `GET /health` | 存活与数据库连通性 |
| `POST /internal/shutdown?token=` | 仅演示用；未设 `SHUTDOWN_TOKEN` 时返回 404，不出现在 OpenAPI |

### CSV 格式

```
wafer_no,thickness_nm
W001,512
W002,508
```
表头可选（也接受中文表头 `晶圆号,厚度nm`）；厚度必须是正整数（拒绝 0、负数、
小数、空值、非数字）；片号批内唯一；每批 1–1000 片。

## 测试

```bash
DATABASE_DSN=<postgres dsn> python -m pytest -q
```
28 个用例覆盖：CSV 边界与非法输入、前三批基线、>5nm 隔离与临界 5nm 放行、
隔离批不进参考窗、滚动窗口滑动、幂等重试与 409 冲突、供数不可见隔离批、
双 worker 同设备 FIFO 与跨设备并行、小连接池下高并发同设备受理无死锁（回归）、
真实子进程硬退出后的接续与无半批。

## 演示中的慢批/崩溃钩子（仅测试用途）

* `SLOW_MARKER_FILE`：每行 `batch_id` 或 `batch_id:秒数`，命中的批在持有设备锁
  期间睡眠，用于制造慢批；
* 同目录 `crash_batches.txt`：每行 `batch_id`，worker 在该批提交判定前调用
  `os._exit(1)` 硬退出（等价 `kill -9`）。生产环境不挂载/不写这些文件即可，
  文件缺失时功能完全关闭。
