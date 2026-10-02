# HPC 作业抢占与检查点恢复服务

实现调度器回收节点场景下的**抢占纪元签发、内容寻址检查点、全量校验释放、
同代际恢复与输出租约栅栏**。纯 Python 标准库，无外部依赖。

## 核心不变量

| # | 不变量 | 实现位置 |
|---|--------|----------|
| 1 | 回收前签发**抢占纪元**（单调 epoch/generation + HMAC 签名令牌）；重复抢占通知幂等，不轮换令牌 | `epochs.py`、`jobs.py::preempt` |
| 2 | 检查点分片为**内容寻址**（`sha256(payload)`），相同分片跨代际天然去重 | `storage.py::ContentStore` |
| 3 | 期望分片集合在开检查点时由「进程集合 × 每进程分片数」固定；提交时**逐个从 CAS 重读重算哈希**，缺片/损坏一律拒绝，检查点停留在 `PENDING`，绝不提前可用 | `checkpoints.py::commit` |
| 4 | **只有全部校验完成**（`COMPLETE`）才允许确认释放；不完整只能 `abandon`（`INCOMPLETE`/`ABORTED`） | `jobs.py::confirm_release/abort_epoch` |
| 5 | **恢复必须使用同一检查点代际**：恢复时再次全量重校验、核对进程集合、回传每个分片输入位点，校验通过才签发**新的输出租约** | `jobs.py::restore`、`leases.py` |
| 6 | 抢占瞬间吊销旧租约；旧进程迟到写入（旧 generation/已 REVOKED 令牌）返回 `412 FenceViolation`；输出按 `(generation, output_id)` 幂等去重 —— 无双重输出 | `leases.py::authorize`、`storage.py::OutputLog` |
| 7 | 节点失联：节点与 rank 标记 `LOST`，其分片不被接受、提交被阻塞；心跳恢复后 incarnation+1，补齐方可整体提交 | `jobs.py::mark_node_lost/heartbeat` |
| 8 | 新一轮抢占使旧的未完成检查点 `SUPERSEDED`；旧纪元令牌不能写新代际 | `checkpoints.py`、`jobs.py` |

## 状态机

```
作业:   RUNNING ──preempt──▶ PREEMPTING ──release(仅COMPLETE)──▶ RELEASED
                                  │                                   │
                                  └──abandon──▶ ABORTED              │
                                      ▲                              │
        RELEASED ──restore(同代际+重校验+新租约)──▶ RUNNING ◀─────────┘

检查点: PENDING ──全部分片重校验通过──▶ COMPLETE      (可重复 commit: 幂等)
            ├── 缺片/损坏: 提交拒绝, 保持 PENDING
            ├── 新纪元接管: SUPERSEDED
            └── 宽限期耗尽放弃: INCOMPLETE
```

## 运行

```bash
# 测试
python3 -m unittest discover -s tests -v        # 22 个测试, 含并发竞争

# 端到端演示 (打印恢复验证报告 Markdown)
python3 demo.py
# 演示后继续提供 HTTP 服务, 浏览器打开节点进度页
python3 demo.py --serve --port 8080
#   http://127.0.0.1:8080/ui/jobs/climate-7

# 空服务
python3 -m hpc_cr            # 默认 127.0.0.1:8080
```

## 作业 API

所有写操作为 `POST /api/jobs/{id}`，JSON body 用 `action` 区分；
凭证走请求头：`X-Epoch-Token`（抢占纪元令牌）、`X-Lease-Token`（输出租约令牌）。
二进制数据以 `data_b64`（标准 base64）传递。

| 方法/路径 | 说明 |
|---|---|
| `POST /api/jobs` | 创建作业（world_size、shards_per_rank、rank→node 放置） |
| `GET  /api/jobs` / `GET /api/jobs/{id}` | 作业列表 / 快照（纪元、宽限剩余、当前检查点、租约） |
| `action=preempt` | 签发抢占纪元；重复通知幂等，`force_new_epoch:true` 表示新一轮抢占 |
| `action=heartbeat` | 节点心跳（LOST→UP，进程 incarnation+1） |
| `action=mark_node_lost` | 标记节点失联，阻塞其分片 |
| `action=progress` | 上报 rank 输入位点 |
| `action=upload_shard` + `X-Epoch-Token` | 上传分片（rank/index/node/input_pos/data_b64），重复同内容幂等 |
| `action=commit_checkpoint` + `X-Epoch-Token` | 全量重校验提交；缺片/损坏返回 409 |
| `action=confirm_release` + `X-Epoch-Token` | 确认释放；非 COMPLETE 返回 409 |
| `action=abandon` | 宽限耗尽放弃，检查点 INCOMPLETE |
| `action=append_output` + `X-Lease-Token` | 提交输出；旧代际/吊销租约 412；同 output_id 幂等 |
| `action=restore` `{generation}` | 同代际恢复验证，返回恢复验证报告 + 新租约 |
| `GET  /api/jobs/{id}/progress.json` | 进度页聚合数据（节点/进程/分片矩阵/输出/审计） |
| `GET  /api/jobs/{id}/checkpoints` | 全部代际检查点 |
| `GET  /api/jobs/{id}/outputs` | 输出条目（按代际隔离） |
| `GET  /api/jobs/{id}/restore/report` | 最近一次恢复验证报告（JSON） |
| `GET  /ui/jobs/{id}` | **节点进度页面**（2s 自动刷新） |

错误映射：`400` 请求格式、`401` 令牌无效/缺失、`404` 对象不存在、
`409` 状态冲突（缺片提交/重复分片/状态机）、`412` 栅栏拒绝（旧纪元/旧租约）。

## 恢复验证报告

`restore()` 与 `/restore/report` 返回结构化报告（`RestoreReport`），并可用
`report.to_markdown()` 渲染，包含：清单摘要、分片重算哈希结果（逐个列出失败
分片）、每个分片的**输入位点 seek 表**、旧/新进程放置与迁移 rank、新输出租约、
警告；任一检查项失败时 `success=false` 且作业保持 `RELEASED`，不会带着坏
检查点运行。

## 并发正确性

- 每作业一把 `RLock`，加锁顺序固定 Job → 子管理器；CAS 存储独立细锁。
- 测试 `tests/test_concurrency_api.py` 覆盖：
  - 旧进程迟到写入与恢复后新进程并发提交相同 `output_id` → 旧写入 412、恰好一次落盘；
  - 8 线程并发重复 output / shard → 只有一次 delivered/stored；
  - 未补齐期间并发提交 6 次全部失败，补齐后才 COMPLETE；
  - 失联恢复与提交循环并发 → COMPLETE 最多出现一次且在补齐之后。

## 代码结构

```
hpc_cr/
  security.py     # HMAC 签名令牌、规范化 JSON、sha256 CAS 原语
  epochs.py       # 调度器、抢占纪元 (epoch/generation/deadline)
  storage.py      # 内容寻址 blob 存储 + 输出去重日志
  checkpoints.py  # 分片、检查点状态机、全量重校验提交
  leases.py       # 输出租约与代际栅栏
  jobs.py         # Job/JobManager 聚合根、恢复验证报告
  api.py          # stdlib ThreadingHTTPServer JSON API
  ui.py           # 节点进度页面 HTML 渲染
tests/            # 22 个单元/并发/HTTP 测试
demo.py           # 端到端场景脚本
```
