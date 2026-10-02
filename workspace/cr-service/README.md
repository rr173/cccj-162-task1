# 高性能计算作业的抢占与检查点恢复服务（CR Service）

一个**零第三方依赖**（仅 Python 3.10+ 标准库）的抢占式调度与检查点/恢复服务。
它在一个真实可运行的系统上落实以下分布式正确性约束：

- 调度器回收节点前先签发**抢占纪元（epoch，单调递增）**；
- 作业把**进程集合、数据分片、输入位点**写入**内容寻址（CAS）检查点**；
- 只有**全部分片与进程记录校验（重新拉取并重新哈希）完成**，检查点代际才
  原子地 `OPEN → SEALED`，并且**只有 SEALED 后才允许确认释放节点**；
- 恢复必须使用**同一检查点代际**，并**重新取得绑定该代际的输出租约**；
- 面对**节点失联、重复检查点、恢复与旧进程迟到写入并发**，绝不产生
  **双重输出**，也绝不把**不完整/损坏的检查点标记为可用**。

## 目录结构

```
cr/
  errors.py       稳定错误码（epoch_stale / shard_hash_mismatch /
                  generation_not_sealed / lease_invalid / duplicate_output …）
  storage.py      原子落盘 + 内容寻址存储（sha256 去重、损坏识别与修复）
  events.py       集群事件时间线（节点页面实时渲染、报告引用）
  coordinator.py  抢占纪元 + 检查点代际状态机（OPEN/SEALED/ABORTED）
  leasing.py      输出租约（单调 token、吊销、epoch/gen 栅栏）+ 输出汇（键级写一次）
  scheduler.py    调度器（PENDING/RELEASED/ABORTED，释放闸门）+ 节点监控
  cluster.py      作业运行时与编排（rank、分片、位点、故障注入、恢复重放）
  report.py       恢复验证报告（7 项不变式核验，JSON + Markdown）
  demo.py         端到端故事线（正常/损坏/重复/失联/慢节点/位腐烂/并发僵尸）
  web.py          HTTP API + 节点进度页面（单文件前端，1s 轮询）
  cli.py          命令行：serve / demo
tests/            24 个单元/端到端/HTTP 冒烟测试
```

## 快速开始

```bash
# 终端演示：跑完整个故事线并打印恢复验证报告
python3 -m cr demo --data ./data-demo --reset

# 启动服务：作业 API（http://127.0.0.1:8080/api）+ 节点进度页面（http://127.0.0.1:8080/）
python3 -m cr serve --data ./data --port 8080
#   加 --reset 清空历史；加 --demo 启动时自动演示一遍
```

运行测试：

```bash
python3 -m unittest discover -s tests -v
```

## 节点进度页面

浏览器打开 `http://127.0.0.1:8080/`，可以：

- 查看每个 rank 的节点、pid、计算步/输入位点、绑定的租约与代际；
- 实时观察每个检查点代际的**分片校验进度条**、进程记录进度、状态与 manifest；
- 一键触发：正常抢占、**损坏分片+重复检查点**、**节点失联**、**慢节点超时**、
  恢复、三种**旧进程迟到写入**；
- 查看每个输出键的接受/幂等去重/栅栏拦截/内容冲突统计；
- 打开**恢复验证报告**（Markdown 渲染）。
- 右侧事件流对每一次拒绝都给出明确原因（旧纪元、租约已吊销、哈希不符、
  检查点不完整、内容冲突）。

## 作业 API

| 方法 & 路径 | 说明 |
|---|---|
| `POST /api/jobs` | 创建作业 `{job_id,ranks,shards_per_rank,shard_size}` |
| `GET  /api/jobs/{id}` | 作业/进程/位点/租约/输出统计 |
| `POST /api/jobs/{id}/run-steps` | 推进计算 `{steps}` |
| `POST /api/jobs/{id}/preempt` | 签发抢占纪元并写检查点（见下方故障注入参数） |
| `POST /api/jobs/{id}/recover` | 从 SEALED 代际恢复 `{gen_id?}` |
| `POST /api/jobs/{id}/zombie` | 旧进程迟到写 `{rank,seq,divergent?}` |
| `POST /api/jobs/{id}/zombie-newkey` | 旧进程写一个全新键 |
| `GET  /api/jobs/{id}/generations` | 全部代际及进度 |
| `GET  /api/generations/{gid}` | 代际详情（承诺摘要、校验状态、缺失项） |
| `GET  /api/jobs/{id}/report` | 恢复验证报告（JSON） |
| `GET  /api/jobs/{id}/report.md` | 恢复验证报告（Markdown） |
| `GET  /api/events?since=N` | 增量事件流 |
| `GET  /api/status` | 集群总览 |
| `POST /api/demo` | 一键完整演示 |

抢占故障注入参数：

```json
{
  "lost_nodes":     ["sim-001-node-01"],   // 节点失联 => 分片缺失 => 封印被拒、纪元中止
  "corrupt_upload": ["r01-s0"],            // 上传损坏字节 => 哈希校验拒绝 => 重传修复
  "flip_after":     ["r00-s0"],            // 已校验 blob 在封印前位腐烂 => 复验拒绝 => 修复
  "duplicate":      true,                  // 每个分片/记录重复提交一遍（幂等去重）
  "slow_ranks":     [3], "slow_delay": 1.5,// 慢 rank
  "deadline":       0.4                    // 抢占期限；超时先中止纪元，迟到上传再被拒
}
```

## 核心协议与不变式

### 1. 抢占纪元（epoch fence）

`Scheduler.issue_preemption` 先让协调器单调推进 epoch 并开出绑定该 epoch 的
`OPEN` 代际。所有上传都必须携带该 epoch：

- epoch 偏小（旧纪元迟到）→ `409 epoch_stale`；
- 纪元已 `ABORTED`（抢占失败）→ `409 generation_aborted`，迟到上传一律拒绝。

### 2. 内容寻址检查点 + 全量校验封印

- 分片在 OPEN 时先登记**承诺（大小 + sha256）**；上传内容必须与承诺一致，
  否则 `422 shard_hash_mismatch`，分片保持 `REJECTED`，检查点不完整；
- 每个 rank 提交**进程记录**（pid、节点、分片清单、`input_positions`、
  `last_output_seq`），记录本身也是 CAS blob；
- `seal()` 封印前**不信任任何内存状态**：从 CAS 重新 `get` 每个分片/记录/
  manifest 并重新哈希；任一缺失或不符即拒绝封印（位腐烂也会被抓到）；
- 封印 = 写入 manifest（其内容覆盖全部进程集合与分片摘要）并原子翻牌为
  `SEALED`。封印与中止都是幂等的。

### 3. 释放闸门

`Scheduler.confirm_release` 强制要求代际为 `SEALED`；失联/超时导致检查点
不完整时走 `abort_preemption`，代际 `ABORTED`、**节点保留继续运行**，
调度器不会回收任何节点。

### 4. 同代际恢复 + 新输出租约

`Cluster.recover` 依次执行：

1. 目标代际必须 `SEALED`，且 `verify_generation` 全量复验通过；
2. 按检查点记录在**替代节点**上重建进程（**新 pid**）、恢复输入位点；
3. `LeaseManager.acquire` 重新签发输出租约：**token 单调递增、绑定同一个
   gen_id/epoch，同时吊销该作业全部旧租约**；
4. 以新租约重放检查点之后的输出（at-least-once），相同内容在输出汇幂等去重。

### 5. 防双重输出的两道防线

| 防线 | 机制 | 拦截对象 |
|---|---|---|
| 租约栅栏 | 旧 token 已吊销 / epoch 不符 / gen 不符 → `lease_*` | 恢复后旧进程迟到写（同内容/分歧/新键） |
| 键级写一次 | `out_key` 写一次：同内容 `dedup`，异内容 `duplicate_output` | 吊销前极窄窗口内的并发写、重放 |

测试 `test_concurrent_recovery_and_zombie_writes_no_double_output` 用
`threading.Barrier` 把恢复与多个旧进程写压在同一时刻，验证无论谁先拿到锁，
唯一输出键集合都不增加、分歧内容绝不覆盖。

## 恢复验证报告

报告（`cr/report.py`）独立复核 7 项门并给出证据：封印门、同代际门、
不完整门、单输出门、租约门、重建门（pid 全换）、释放门。
总体结论 `PASS/FAIL`；Markdown 落盘在 `<data>/reports/recovery-report.md`。

## 持久化与崩溃恢复

- 协调器、调度器、租约、输出索引、节点名册均以「临时文件 + `fsync` +
  `os.replace`」原子写入 JSON；blob 存于 CAS；
- 进程重启后 `Cluster._bootstrap_runtimes` 只依据已落盘状态重建内存视图，
  **不重新签发租约**：恢复租约仍有效则 rank 继续绑定，否则标记 `PAUSED`。
