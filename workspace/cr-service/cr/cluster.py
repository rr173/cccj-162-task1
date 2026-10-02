"""作业运行时与集群编排。

把协调器/调度器/租约/输出汇组装成一个可驱动的集群，并模拟 HPC
作业的进程集合（rank）、数据分片、输入位点与故障：

* :meth:`Cluster.do_preempt` —— 签发抢占纪元并并发写检查点，支持故障注入：
  失联节点、损坏分片（上传时被拒后修复）、封印后位腐烂（复验被拒后修复）、
  重复检查点（幂等去重）、慢 rank 超时（纪元中止后迟到上传被拒）；
* :meth:`Cluster.recover` —— 必须从 SEALED 代际恢复，先全量验证，重新
  分配节点与新 pid，重新取得绑定同代际的输出租约，再以新租约重放输出
  （相同内容在输出端幂等去重，不产生双重输出）；
* :meth:`Cluster.zombie_write` —— 模拟被抢占的旧进程迟到写入。
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import errors
from .coordinator import (
    OPEN,
    SEALED,
    CheckpointCoordinator,
)
from .events import EventLog
from .leasing import LeaseManager, OutputSink
from .scheduler import NodeMonitor, Scheduler
from .storage import CASStore, canonical_json, sha256_hex

INPUT_BLOCK = 4096

RUNNING = "RUNNING"
CHECKPOINTING = "CHECKPOINTING"
PAUSED = "PAUSED"          # 检查点封印、节点已回收，等待恢复
LOST = "LOST"              # 节点失联


def shard_payload(job_id: str, rank: int, epoch: int, shard_idx: int,
                  size: int) -> bytes:
    """确定性地生成分片内容（相同进程位点 + 纪元 => 相同字节/相同摘要）。"""

    seed = canonical_json({
        "job": job_id, "rank": rank, "epoch": epoch, "shard": shard_idx,
    })
    buf = bytearray()
    block = seed
    while len(buf) < size:
        block = sha256_hex(block.encode() if isinstance(block, str) else block).encode()
        buf.extend(block)
    return bytes(buf[:size])


def output_payload(job_id: str, rank: int, seq: int) -> bytes:
    head = json.dumps(
        {"job": job_id, "rank": rank, "seq": seq}, sort_keys=True
    ).encode()
    return head + b"\n" + (head + b"-payload\n") * 128


def out_key(job_id: str, rank: int, seq: int) -> str:
    return f"{job_id}/out/r{rank:02d}/s{seq:06d}"


@dataclass
class RankState:
    rank: int
    node: str
    pid: int
    status: str = RUNNING
    step: int = 0
    last_output_seq: int = -1
    gen_id: str = "origin"
    epoch: int = 0
    token: Optional[int] = None
    last_sealed_gen: Optional[str] = None
    last_sealed_epoch: Optional[int] = None

    def to_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class JobRuntime:
    job_id: str
    ranks: int
    shards_per_rank: int
    shard_size: int
    rank_state: Dict[int, RankState] = field(default_factory=dict)
    recovery_count: int = 0
    # 被新进程取代的旧进程快照（用于模拟迟到的僵尸写入）
    superseded: List[dict] = field(default_factory=list)

    @property
    def lock(self) -> threading.RLock:
        if not hasattr(self, "_lock"):
            self._lock = threading.RLock()
        return self._lock

    def shard_ids(self, rank: int) -> List[str]:
        return [f"r{rank:02d}-s{j}" for j in range(self.shards_per_rank)]

    def live_ranks(self) -> List[RankState]:
        return [
            r for r in self.rank_state.values()
            if r.status in (RUNNING, CHECKPOINTING)
        ]

    def running_ranks(self) -> List[RankState]:
        return [r for r in self.rank_state.values() if r.status == RUNNING]


class Cluster:
    """进程内集群：组装全部子系统并提供作业编排动作。"""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.events = EventLog()
        self.cas = CASStore(f"{data_dir}/cas")
        self.coordinator = CheckpointCoordinator(data_dir, self.cas, self.events)
        self.nodes = NodeMonitor(data_dir, self.events)
        self.scheduler = Scheduler(
            data_dir, self.coordinator, self.nodes, self.events
        )
        self.leases = LeaseManager(data_dir, self.events)
        self.sink = OutputSink(
            f"{data_dir}/cas", data_dir, self.leases, self.events
        )
        self._lock = threading.RLock()
        self.jobs: Dict[str, JobRuntime] = {}
        self._bootstrap_runtimes()

    def _bootstrap_runtimes(self) -> None:
        """进程重启后自举：只依据已落盘状态重建内存中的作业/进程视图。

        不签发任何新租约：若恢复租约仍有效，rank 继续绑定该租约与其代际；
        若作业没有活租约（例如节点已回收、等待恢复），rank 标记为 PAUSED。
        分片数等作业结构信息从最近一个代际的清单推断。
        """

        for job_id, job_meta in list(self.coordinator.jobs.items()):
            gens = self.coordinator.list_generations(job_id)
            latest_gen = gens[-1] if gens else None
            shards_per_rank = 1
            shard_size = 0
            if latest_gen and latest_gen["shards"]:
                sample = next(iter(latest_gen["shards"].values()))
                shard_ids = [
                    p["shards"] for p in latest_gen["processes"].values()
                ]
                shards_per_rank = max(
                    (len(s) for s in shard_ids), default=1
                )
                shard_size = sample["size"]
            job = JobRuntime(
                job_id=job_id, ranks=int(job_meta["ranks"]),
                shards_per_rank=shards_per_rank, shard_size=shard_size,
            )
            lease = self.leases.current_lease(job_id)
            source_gen_id = lease.gen_id if lease and lease.gen_id != "origin" else (
                self.coordinator.latest_sealed_generation(job_id)
            )
            for rank in range(job.ranks):
                node, pid, step, last_seq = "unassigned", 0, 0, -1
                gen_id = "origin"
                epoch = 0
                status = PAUSED
                if source_gen_id:
                    sg = self.coordinator.get_generation(source_gen_id)
                    pd = sg["processes"].get(str(rank))
                    if pd is not None:
                        node = pd["node"]
                        pid = pd["pid"]
                        step = pd["input_positions"].get("input-0", 0) // INPUT_BLOCK
                        last_seq = pd.get("last_output_seq", -1)
                        gen_id = source_gen_id
                        epoch = sg["epoch"]
                        status = RUNNING if lease is not None else PAUSED
                job.rank_state[rank] = RankState(
                    rank=rank, node=node, pid=pid, status=status,
                    step=step, last_output_seq=last_seq,
                    gen_id=gen_id, epoch=epoch,
                    token=lease.token if lease else None,
                    last_sealed_gen=self.coordinator.latest_sealed_generation(job_id),
                )
            self.jobs[job_id] = job

    # ============================ 作业生命周期 ========================

    def create_job(
        self,
        job_id: str = "sim-001",
        ranks: int = 4,
        shards_per_rank: int = 2,
        shard_size: int = 64 * 1024,
        nodes_per: int = 2,
    ) -> dict:
        with self._lock:
            if job_id in self.jobs:
                return self.job_view(job_id)
            self.coordinator.create_job(job_id, ranks=ranks, name=f"HPC作业 {job_id}")

            job = JobRuntime(
                job_id=job_id, ranks=ranks,
                shards_per_rank=shards_per_rank, shard_size=shard_size,
            )
            assignment: Dict[str, List[int]] = {}
            for rank in range(ranks):
                node_id = f"{job_id}-node-{rank % nodes_per:02d}"
                self.nodes.register(node_id)
                assignment.setdefault(node_id, []).append(rank)
                job.rank_state[rank] = RankState(
                    rank=rank, node=node_id, pid=20000 + rank
                )
            for node_id, rks in assignment.items():
                self.nodes.register(node_id)
                self.nodes.allocate(node_id, job_id, rks)

            lease = self.leases.acquire(job_id, epoch=0, gen_id="origin",
                                        reason="job_start")
            for r in job.rank_state.values():
                r.token = lease.token
            self.jobs[job_id] = job
            self.events.emit(
                "job", "ok",
                f"作业 {job_id} 启动：{ranks} 个进程，每进程 "
                f"{shards_per_rank} 个 {shard_size}B 分片",
                job_id=job_id, ranks=ranks,
            )
            return self.job_view(job_id)

    def _job(self, job_id: str) -> JobRuntime:
        job = self.jobs.get(job_id)
        if job is None:
            raise errors.JobNotFound(job_id=job_id)
        return job

    # ============================ 正常运行 ============================

    def run_steps(self, job_id: str, steps: int = 1) -> dict:
        """推进计算，每个 rank 每步产出一个输出。"""

        job = self._job(job_id)
        produced = 0
        with job.lock:
            runners = job.running_ranks()
            for _ in range(steps):
                for r in runners:
                    seq = r.last_output_seq + 1
                    res = self.sink.write(
                        job_id, r.token, r.epoch, r.gen_id,
                        out_key(job_id, r.rank, seq),
                        output_payload(job_id, r.rank, seq),
                        rank=r.rank, seq=seq,
                    )
                    if res["result"] == OutputSink.ACCEPTED:
                        produced += 1
                    r.last_output_seq = seq
                    r.step += 1
        return {"advanced_steps": steps, "outputs_produced": produced,
                "ranks": len(runners)}

    # ============================ 检查点规格 ==========================

    def _snapshot_live(self, job: JobRuntime, next_epoch: int) -> tuple[list[dict], list[dict]]:
        """生成检查点承诺：覆盖*完整进程集合*（含失联 rank）。

        失联 rank 没有上传线程，它们的分片会永远停留在 EXPECTED，
        封印时因此被判不完整——这正是「部分节点失联不得释放」的关键。
        """

        process_specs: List[dict] = []
        shard_specs: List[dict] = []
        for r in job.rank_state.values():
            if r.status == PAUSED:
                continue
            ids = job.shard_ids(r.rank)
            process_specs.append({
                "rank": r.rank,
                "node": r.node,
                "pid": r.pid,
                "shards": ids,
                "input_positions": {"input-0": r.step * INPUT_BLOCK},
                "last_output_seq": r.last_output_seq,
                "reachable": r.status != LOST,
            })
            for j, sid in enumerate(ids):
                payload = shard_payload(
                    job.job_id, r.rank, next_epoch, j, job.shard_size
                )
                shard_specs.append({
                    "shard_id": sid,
                    "size": job.shard_size,
                    "expected_digest": sha256_hex(payload),
                })
        return process_specs, shard_specs

    @staticmethod
    def _rank_of_shard(shard_id: str) -> int:
        return int(shard_id.split("-", 1)[0][1:])

    @staticmethod
    def _index_of_shard(shard_id: str) -> int:
        return int(shard_id.rsplit("-s", 1)[1])

    # ============================ 抢占 ================================

    def do_preempt(
        self,
        job_id: str,
        *,
        lost_nodes: Optional[List[str]] = None,
        corrupt_upload: Optional[List[str]] = None,
        flip_after: Optional[List[str]] = None,
        duplicate: bool = False,
        slow_ranks: Optional[List[int]] = None,
        slow_delay: float = 2.0,
        deadline: Optional[float] = None,
    ) -> dict:
        """执行一次抢占并写检查点（含各种故障注入）。"""

        job = self._job(job_id)
        lost_nodes = lost_nodes or []
        corrupt_upload = set(corrupt_upload or [])
        flip_after = set(flip_after or [])
        slow_ranks = set(slow_ranks or [])

        counters = {
            "hash_rejected": 0, "repaired": 0, "duplicate_shards": 0,
            "duplicate_records": 0, "late_rejected": 0,
            "seal_rejected": 0,
        }

        # --- 故障注入：失联节点上的 rank 直接死亡，分片永远到不齐 ---------
        lost_ranks: set[int] = set()
        for node_id in lost_nodes:
            self.nodes.mark_lost(node_id, "心跳超时，节点不可达")
        with job.lock:
            for r in list(job.rank_state.values()):
                if r.node in lost_nodes and r.status != PAUSED:
                    r.status = LOST
                    lost_ranks.add(r.rank)
            live = list(job.live_ranks())

        if not live:
            raise errors.PreemptionConflict("没有可写检查点的存活进程")

        next_epoch = self.coordinator.peek_next_epoch(job_id)
        process_specs, shard_specs = self._snapshot_live(job, next_epoch)
        rec = self.scheduler.issue_preemption(job_id, process_specs, shard_specs)
        epoch, gen_id = rec["epoch"], rec["gen_id"]

        with job.lock:
            for r in live:
                r.status = CHECKPOINTING

        barrier: Dict[str, object] = {}
        blobs_ok: List[str] = []
        counters_lock = threading.Lock()

        def count(name: str, n: int = 1) -> None:
            with counters_lock:
                counters[name] += n

        def rank_record(r: RankState) -> dict:
            return {
                "rank": r.rank,
                "node": r.node,
                "pid": r.pid,
                "shards": job.shard_ids(r.rank),
                "input_positions": {"input-0": r.step * INPUT_BLOCK},
                "last_output_seq": r.last_output_seq,
            }

        def upload_rank(r: RankState, repeat: int) -> None:
            if r.rank in slow_ranks:
                time.sleep(slow_delay)
            try:
                for j, sid in enumerate(job.shard_ids(r.rank)):
                    good = shard_payload(job_id, r.rank, epoch, j,
                                         job.shard_size)
                    for attempt in range(repeat):
                        data = b"CORRUPT-UPLOAD\n" if (
                            attempt == 0 and sid in corrupt_upload
                        ) else good
                        try:
                            self.coordinator.upload_shard(
                                gen_id, epoch, sid, data
                            )
                            if attempt == 0:
                                blobs_ok.append(sid)
                            else:
                                count("duplicate_shards")
                        except errors.ShardHashMismatch:
                            # 校验失败 => 不标记可用；用正确字节重传修复。
                            count("hash_rejected")
                            self.coordinator.upload_shard(
                                gen_id, epoch, sid, good
                            )
                            count("repaired")
                            blobs_ok.append(sid)
                for attempt in range(repeat):
                    self.coordinator.submit_rank_record(
                        gen_id, epoch, r.rank, rank_record(r)
                    )
                    if attempt == 1:
                        count("duplicate_records")
            except errors.GenerationAborted:
                count("late_rejected")
                self.events.emit(
                    "job", "reject",
                    f"rank {r.rank} 的迟到分片/记录在纪元中止后到达，已拒绝",
                    gen_id=gen_id, rank=r.rank,
                )
            except errors.EpochStale:
                count("late_rejected")

        threads: List[threading.Thread] = []
        reps = 2 if duplicate else 1
        for r in live:
            t = threading.Thread(
                target=upload_rank, args=(r, reps),
                name=f"cp-r{r.rank}-ep{epoch}",
            )
            threads.append(t)
        start = time.time()
        for t in threads:
            t.start()

        timed_out = False
        if deadline is not None:
            view = self.coordinator.wait_for_completion(gen_id, timeout=deadline)
            timed_out = view["status"] == OPEN

        # 超时即中止纪元；仍在睡眠的慢 rank 醒来后，其迟到上传必须撞上
        # 「代际已中止」栅栏。注意：先 abort 再 join 慢线程。
        if timed_out:
            abort = self.scheduler.abort_preemption(
                job_id, f"检查点在 {deadline}s 期限内未完成（慢节点）"
            )
            for t in threads:
                t.join(timeout=slow_delay + 2)
            with job.lock:
                for r in live:
                    if r.status == CHECKPOINTING:
                        r.status = RUNNING
            return {
                "outcome": "aborted",
                "gen_id": gen_id, "epoch": epoch,
                "reason": abort["reason"], "counters": counters,
                "lost_ranks": sorted(lost_ranks),
            }

        for t in threads:
            t.join(timeout=slow_delay + 2)

        # --- 故障注入：已校验的 blob 在封印前位腐烂 ----------------------
        flipped = [sid for sid in flip_after if sid in blobs_ok]
        for sid in flipped:
            entry = self.coordinator.get_generation(gen_id)["shards"][sid]
            self.cas.corrupt_for_test(entry["digest"])

        # 失联 => 分片必然不齐 => 封印必须失败、纪元中止
        if lost_ranks:
            try:
                self.coordinator.seal(gen_id, epoch)
            except errors.CheckpointIncomplete as exc:
                count("seal_rejected")
                abort = self.scheduler.abort_preemption(
                    job_id,
                    f"{len(lost_nodes)} 个节点失联、{len(lost_ranks)} 个进程的"
                    "检查点不完整，禁止回收",
                )
                with job.lock:
                    for r in live:
                        if r.status == CHECKPOINTING:
                            r.status = RUNNING
                return {
                    "outcome": "aborted",
                    "gen_id": gen_id, "epoch": epoch,
                    "reason": abort["reason"],
                    "missing": exc.details,
                    "counters": counters,
                    "lost_ranks": sorted(lost_ranks),
                }

        # 正常路径：封印（全量复验）。位腐烂会在这一步被抓出并修复后重试。
        try:
            sealed = self.coordinator.seal(gen_id, epoch)
        except errors.ShardHashMismatch as exc:
            count("seal_rejected")
            self.events.emit(
                "job", "reject",
                "封印前全量复验发现损坏 blob，修复后重新封印",
                gen_id=gen_id, problems=exc.details.get("problems"),
            )
            for sid in flipped:
                j = self._index_of_shard(sid)
                rank = self._rank_of_shard(sid)
                good = shard_payload(job_id, rank, epoch, j, job.shard_size)
                self.coordinator.upload_shard(gen_id, epoch, sid, good)
                count("repaired")
            sealed = self.coordinator.seal(gen_id, epoch)

        released = self.scheduler.confirm_release(job_id)
        with job.lock:
            for r in live:
                r.status = PAUSED
                r.last_sealed_gen = gen_id
                r.last_sealed_epoch = epoch
        return {
            "outcome": "released",
            "gen_id": gen_id,
            "epoch": epoch,
            "sealed": sealed["status"] == SEALED,
            "manifest": sealed["manifest_digest"],
            "freed_nodes": released["freed_nodes"],
            "counters": counters,
            "elapsed": time.time() - start,
        }

    # ============================ 恢复 ================================

    def recover(
        self,
        job_id: str,
        gen_id: Optional[str] = None,
        *,
        replay: bool = True,
        extra_node_prefix: str = "replacement",
    ) -> dict:
        """从同一 SEALED 代际恢复：验证 → 新节点/新 pid → 新租约 → 重放。"""

        job = self._job(job_id)
        gen_id = gen_id or self.coordinator.latest_sealed_generation(job_id)
        if not gen_id:
            raise errors.GenerationNotFound("作业没有可恢复的已封印检查点")

        gen = self.coordinator.get_generation(gen_id)
        if gen["status"] != SEALED:
            raise errors.GenerationNotSealed(
                f"代际 {gen_id} 状态为 {gen['status']}，恢复被拒绝",
                gen_id=gen_id,
            )
        verification = self.coordinator.verify_generation(gen_id)
        if not verification["ok"]:
            raise errors.ShardHashMismatch(
                "恢复验证失败：检查点内容与封印时不一致",
                gen_id=gen_id, problems=verification["problems"],
            )

        with job.lock:
            # 记录被取代的旧进程（它们持有的旧租约即将失效）。
            for r in job.rank_state.values():
                job.superseded.append({
                    "rank": r.rank, "node": r.node, "pid": r.pid,
                    "token": r.token, "epoch": r.epoch, "gen_id": r.gen_id,
                })

            # 在替代节点上重建进程集合，身份与位点来自检查点记录。
            job.recovery_count += 1
            rebuilt: List[dict] = []
            node_for_rank: Dict[str, List[int]] = {}
            for rank_s, pd in sorted(gen["processes"].items(),
                                     key=lambda kv: int(kv[0])):
                rank = int(rank_s)
                record = json.loads(
                    self.cas.get(pd["record_digest"]).decode("utf-8")
                )
                node_id = (
                    f"{job_id}-{extra_node_prefix}-"
                    f"{job.recovery_count}-{rank % 2:02d}"
                )
                self.nodes.register(node_id)
                node_for_rank.setdefault(node_id, []).append(rank)
                new_pid = 30000 + rank + job.recovery_count * 1000
                step = record["input_positions"]["input-0"] // INPUT_BLOCK
                rebuilt.append({
                    "rank": rank,
                    "old_pid": job.rank_state.get(rank,
                                                  RankState(rank, node_id, 0)).pid,
                    "new_pid": new_pid,
                    "node": node_id,
                    "step": step,
                    "last_output_seq": record["last_output_seq"],
                    "record_digest": pd["record_digest"][:12],
                })
                job.rank_state[rank] = RankState(
                    rank=rank, node=node_id, pid=new_pid, status=RUNNING,
                    step=step,
                    last_output_seq=record["last_output_seq"],
                    last_sealed_gen=gen_id,
                    last_sealed_epoch=gen["epoch"],
                )
            for node_id, rks in node_for_rank.items():
                self.nodes.allocate(node_id, job_id, rks)

            # 恢复时重新取得输出租约：绑定同一代际，新 token 吊销全部旧租约。
            lease = self.leases.acquire(
                job_id, epoch=gen["epoch"], gen_id=gen_id, reason="recover"
            )
            for r in job.rank_state.values():
                r.token = lease.token
                r.epoch = gen["epoch"]
                r.gen_id = gen_id

        # 以新租约重放检查点之后的输出（at-least-once 重放）：
        # 相同内容命中键级幂等，不产生第二份输出。
        replay_stats = {"replayed": 0, "dedup": 0, "accepted": 0}
        if replay:
            for item in rebuilt:
                rank = item["rank"]
                for seq in range(0, item["last_output_seq"] + 1):
                    res = self.sink.write(
                        job_id, lease.token, gen["epoch"], gen_id,
                        out_key(job_id, rank, seq),
                        output_payload(job_id, rank, seq),
                        rank=rank, seq=seq,
                    )
                    replay_stats["replayed"] += 1
                    if res["result"] == OutputSink.DEDUP:
                        replay_stats["dedup"] += 1
                    else:
                        replay_stats["accepted"] += 1

        self.events.emit(
            "job", "ok",
            f"作业 {job_id} 从代际 {gen_id} 恢复：{len(rebuilt)} 个进程在 "
            f"{len(node_for_rank)} 个替代节点上重建",
            job_id=job_id, gen_id=gen_id, new_token=lease.token,
            replay=replay_stats,
        )
        return {
            "outcome": "recovered",
            "gen_id": gen_id,
            "epoch": gen["epoch"],
            "token": lease.token,
            "verification": verification,
            "rebuilt": rebuilt,
            "replay": replay_stats,
        }

    # ============================ 旧进程迟到写入 ======================

    def zombie_write(
        self,
        job_id: str,
        *,
        rank: int = 0,
        seq: Optional[int] = None,
        divergent: bool = False,
    ) -> dict:
        """模拟被抢占的旧进程在恢复之后带着旧租约迟到写入。

        * 相同内容：现实中可能是重传——但旧租约已吊销，仍在栅栏处拒绝；
        * 不同内容（divergent）：双重输出企图，栅栏 + 键级双重防线；
        * 全新键：旧纪元/旧代际令牌同样被拒。
        """

        job = self._job(job_id)
        if not job.superseded:
            raise errors.PreemptionConflict("尚无被取代的旧进程可模拟")
        victim = next(
            (s for s in reversed(job.superseded) if s["rank"] == rank),
            job.superseded[-1],
        )
        with job.lock:
            cur = job.rank_state[rank]
            seq = cur.last_output_seq if seq is None else seq
        key = out_key(job_id, rank, seq)
        payload = (
            b"ZOMBIE-DIVERGENT-CONTENT\n"
            if divergent else output_payload(job_id, rank, seq)
        )
        try:
            res = self.sink.write(
                job_id, victim["token"], victim["epoch"], victim["gen_id"],
                key, payload, rank=rank, seq=seq,
            )
            return {"blocked": False, "result": res["result"], "out_key": key,
                    "victim_token": victim["token"]}
        except errors.CRError as exc:
            self.events.emit(
                "output", "reject",
                f"旧进程 pid={victim['pid']} 携带旧租约 token="
                f"{victim['token']} 的迟到写入被拒（{exc.code}）",
                job_id=job_id, out_key=key, code=exc.code,
            )
            return {"blocked": True, "code": exc.code, "message": str(exc),
                    "out_key": key, "victim_token": victim["token"]}

    def zombie_write_new_key(self, job_id: str, rank: int = 0) -> dict:
        """旧进程试图写一个从未存在过的新键（迟到的「新」输出）。"""

        job = self._job(job_id)
        victim = next(
            (s for s in reversed(job.superseded) if s["rank"] == rank),
            job.superseded[-1],
        )
        with job.lock:
            seq = 10_000 + job.recovery_count  # 选一个恢复后不会撞的序号
        key = out_key(job_id, rank, seq)
        try:
            self.sink.write(
                job_id, victim["token"], victim["epoch"], victim["gen_id"],
                key, b"late new output from old epoch\n",
                rank=rank, seq=seq,
            )
            return {"blocked": False, "out_key": key}
        except errors.CRError as exc:
            return {"blocked": True, "code": exc.code, "message": str(exc),
                    "out_key": key, "victim_token": victim["token"]}

    # ============================ 故障注入/修复 =======================

    def corrupt_sealed_blob(self, gen_id: str) -> str:
        """破坏一个已封印检查点的底层 blob（恢复验证必须能发现）。"""

        gen = self.coordinator.get_generation(gen_id)
        sid = next(iter(gen["shards"]))
        digest = gen["shards"][sid]["digest"]
        self.cas.corrupt_for_test(digest)
        self.events.emit(
            "chaos", "warn",
            f"故障注入：代际 {gen_id} 的分片 {sid} 底层 blob 被静默损坏",
            gen_id=gen_id, shard=sid,
        )
        return sid

    def repair_sealed_blob(self, job_id: str, gen_id: str, shard_id: str) -> None:
        """用确定性内容重建损坏 blob（CAS 发现现存对象哈希不符会覆盖修复）。"""

        gen = self.coordinator.get_generation(gen_id)
        rank = self._rank_of_shard(shard_id)
        idx = self._index_of_shard(shard_id)
        size = gen["shards"][shard_id]["size"]
        good = shard_payload(job_id, rank, gen["epoch"], idx, size)
        self.cas.put(good, expected_digest=sha256_hex(good))
        self.events.emit(
            "chaos", "ok", f"分片 {shard_id} 已从权威内容重建",
            gen_id=gen_id, shard=shard_id,
        )

    # ============================ 视图 ================================

    def job_view(self, job_id: str) -> dict:
        job = self._job(job_id)
        with job.lock:
            return {
                "job_id": job_id,
                "ranks": job.ranks,
                "shards_per_rank": job.shards_per_rank,
                "shard_size": job.shard_size,
                "recovery_count": job.recovery_count,
                "ranks_state": {
                    str(rank): r.to_dict()
                    for rank, r in sorted(job.rank_state.items())
                },
                "active_preemption": self.scheduler.active_preemption(job_id),
                "latest_sealed": self.coordinator.latest_sealed_generation(job_id),
                "lease": (
                    self.leases.current_lease(job_id).to_dict()
                    if self.leases.current_lease(job_id) else None
                ),
                "outputs": self.sink.stats_for(job_id),
            }

    def status(self) -> dict:
        return {
            "jobs": [self.job_view(j) for j in sorted(self.jobs)],
            "coordinator": self.coordinator.status(),
            "scheduler": {
                "active": {
                    j: r for j, r in self.scheduler.preemptions.items()
                },
                "history": self.scheduler.history,
            },
            "leases": self.leases.view(),
            "nodes": self.nodes.view(),
            "events": self.events.tail(60),
        }
