"""作业聚合根: 协调抢占纪元、检查点、输出租约与恢复全流程。

线程模型: 每个 Job 一把 RLock; 子管理器 (CheckpointManager/LeaseManager)
各有内部锁, 加锁顺序固定为 Job -> 子管理器, 避免死锁。
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field

from .checkpoints import (
    COMPLETE,
    PENDING,
    CheckpointManager,
    shard_id,
)
from .epochs import Scheduler
from .errors import Conflict, FenceViolation, NotFound, ValidationError
from .leases import LeaseManager
from .storage import ContentStore, OutputLog

# 作业状态
RUNNING = "RUNNING"
PREEMPTING = "PREEMPTING"
RELEASED = "RELEASED"
ABORTED = "ABORTED"

# 进程 / 节点状态
ALIVE = "ALIVE"
LOST = "LOST"
UP = "UP"

AUDIT_LIMIT = 500


@dataclass
class Process:
    rank: int
    node: str
    incarnation: int = 1
    state: str = ALIVE
    last_heartbeat: float = field(default_factory=time.time)
    last_input_pos: int = 0
    last_progress_ts: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "node": self.node,
            "incarnation": self.incarnation,
            "state": self.state,
            "last_heartbeat": self.last_heartbeat,
            "last_input_pos": self.last_input_pos,
            "last_progress_ts": self.last_progress_ts,
        }


@dataclass
class Node:
    node_id: str
    state: str = UP
    last_heartbeat: float = field(default_factory=time.time)
    ranks: list[int] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state,
            "last_heartbeat": self.last_heartbeat,
            "ranks": list(self.ranks),
        }


@dataclass
class RestoreReport:
    """恢复验证报告: 只有全部检查项通过 success 才为 True。"""

    job_id: str
    requested_generation: int
    success: bool
    ts: float
    checkpoint_state: str
    manifest_digest: str | None = None
    shards_total: int = 0
    shards_verified: int = 0
    shards_failed: list[str] = field(default_factory=list)
    input_positions: dict[str, int] = field(default_factory=dict)
    old_processes: list[dict] = field(default_factory=list)
    new_processes: list[dict] = field(default_factory=list)
    process_set_match: bool = False
    relocated_ranks: list[int] = field(default_factory=list)
    lease: dict | None = None
    warnings: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "requested_generation": self.requested_generation,
            "success": self.success,
            "ts": self.ts,
            "checkpoint_state": self.checkpoint_state,
            "manifest_digest": self.manifest_digest,
            "shards_total": self.shards_total,
            "shards_verified": self.shards_verified,
            "shards_failed": list(self.shards_failed),
            "input_positions": dict(self.input_positions),
            "old_processes": list(self.old_processes),
            "new_processes": list(self.new_processes),
            "process_set_match": self.process_set_match,
            "relocated_ranks": list(self.relocated_ranks),
            "lease": self.lease,
            "warnings": list(self.warnings),
            "error": self.error,
        }

    def to_markdown(self) -> str:
        return render_report_md(self)


def render_report_md(r: RestoreReport) -> str:
    lines = [
        f"# 恢复验证报告 — 作业 {r.job_id}",
        "",
        f"- 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.ts))}",
        f"- 请求恢复代际: gen{r.requested_generation}",
        f"- 检查点状态: {r.checkpoint_state}",
        f"- 结论: **{'✅ 验证通过, 允许恢复' if r.success else '❌ 验证失败, 拒绝恢复'}**",
    ]
    if r.error:
        lines += [f"- 失败原因: `{r.error}`"]
    lines += [
        "",
        "## 检查点完整性",
        "",
        f"- 清单摘要 (manifest digest): `{r.manifest_digest or '-'}`",
        f"- 分片总数: {r.shards_total}",
        f"- 重算哈希通过: {r.shards_verified}",
        f"- 校验失败: {r.shards_failed or '无'}",
        f"- 进程集合一致 (rank 0..N-1): {'是' if r.process_set_match else '否'}",
        "",
        "## 输入位点 (恢复后 seek 位置)",
        "",
    ]
    if r.input_positions:
        lines += [
            "| 分片 | rank | 序号 | 输入位点 |",
            "|---|---|---|---|",
        ]
        for sid, pos in sorted(
            r.input_positions.items(),
            key=lambda kv: (int(kv[0].split(":")[0][1:]), kv[1]),
        ):
            rank, idx = sid.split(":")
            lines.append(f"| {sid} | {rank[1:]} | {idx[1:]} | {pos} |")
    else:
        lines.append("(无)")
    lines += ["", "## 进程放置", "", "| rank | 旧节点 | 新节点 | 备注 |", "|---|---|---|---|"]
    old = {p["rank"]: p["node"] for p in r.old_processes}
    relocated = set(r.relocated_ranks)
    for p in r.new_processes:
        rk = p["rank"]
        note = "迁移重调度" if rk in relocated else "同节点"
        lines.append(f"| {rk} | {old.get(rk, '-')} | {p['node']} | {note} |")
    if r.lease:
        lines += [
            "",
            "## 输出租约",
            "",
            f"- 租约 ID: `{r.lease['lease_id']}`",
            f"- 绑定代际: gen{r.lease['generation']}",
            f"- 状态: {r.lease['state']}",
            f"- 有效期至: {time.strftime('%H:%M:%S', time.localtime(r.lease['expires_ts']))}",
        ]
    if r.warnings:
        lines += ["", "## 警告", ""]
        lines += [f"- {w}" for w in r.warnings]
    lines.append("")
    return "\n".join(lines)


class Job:
    def __init__(
        self,
        job_id: str,
        name: str,
        world_size: int,
        shards_per_rank: int,
        rank_nodes: dict[int, str],
        store: ContentStore,
        scheduler: Scheduler,
        now: float | None = None,
        lease_ttl: float = 300.0,
    ):
        self.job_id = job_id
        self.name = name
        self.world_size = world_size
        self.shards_per_rank = shards_per_rank
        self.created_ts = now or time.time()
        self.state = RUNNING
        self.store = store
        self.scheduler = scheduler
        self.lock = threading.RLock()

        self.nodes: dict[str, Node] = {}
        self.processes: dict[int, Process] = {}
        for rank in range(world_size):
            node = rank_nodes[rank]
            self.processes[rank] = Process(rank=rank, node=node)
            n = self.nodes.setdefault(node, Node(node_id=node))
            if rank not in n.ranks:
                n.ranks.append(rank)

        self.cpm = CheckpointManager(job_id, store)
        self.leases = LeaseManager(job_id, scheduler)
        self.outputs = OutputLog()
        self.audit: list[dict] = []

        self.current_epoch = 0
        self.current_generation = 0          # 抢占纪元代际 (0 = 初始)
        self.output_generation = 0           # 当前允许输出的代际
        self.epoch_token: str | None = None
        self.released_generation: int | None = None
        self.restored_generation: int | None = None
        self.released_ts: float | None = None
        self.aborted_ts: float | None = None
        self._last_restore: RestoreReport | None = None
        self._lease_ttl = lease_ttl

        # 初始代际 gen0 的输出租约
        lease = self.leases.grant(0, lease_ttl)
        self._log("lease_granted", {"generation": 0, "lease_id": lease.lease_id})

    # ------------------------------------------------------------- utility
    def _log(self, event: str, detail: dict | None = None, ts: float | None = None):
        self.audit.append({"ts": ts or time.time(), "event": event, "detail": detail or {}})
        if len(self.audit) > AUDIT_LIMIT:
            del self.audit[:-AUDIT_LIMIT]

    def epoch_info(self):
        return self.scheduler.current_epoch(self.job_id)

    def snapshot(self) -> dict:
        with self.lock:
            ep = self.epoch_info()
            cur_cp = None
            if self.current_generation:
                cps = {c.generation: c for c in self.cpm.list_all()}
                if self.current_generation in cps:
                    cur_cp = cps[self.current_generation].to_dict()
            return {
                "job_id": self.job_id,
                "name": self.name,
                "state": self.state,
                "world_size": self.world_size,
                "shards_per_rank": self.shards_per_rank,
                "created_ts": self.created_ts,
                "current_epoch": self.current_epoch,
                "current_generation": self.current_generation,
                "output_generation": self.output_generation,
                "released_generation": self.released_generation,
                "restored_generation": self.restored_generation,
                "epoch": ep.to_dict() if ep else None,
                "epoch_grace_remaining": (
                    max(0.0, ep.deadline_ts - time.time()) if ep else None
                ),
                "released_ts": self.released_ts,
                "aborted_ts": self.aborted_ts,
                "current_checkpoint": cur_cp,
                "active_lease": self.leases.current().to_dict()
                if self.leases.current() else None,
                "outputs_total": len(self.outputs.list()),
            }


class JobManager:
    """作业注册表 + 全部编排用例。"""

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self.store = ContentStore()

    # ------------------------------------------------------------ create
    def create_job(
        self,
        *,
        name: str,
        world_size: int,
        shards_per_rank: int = 1,
        rank_nodes: dict[int, str] | None = None,
        job_id: str | None = None,
        lease_ttl: float = 300.0,
    ) -> Job:
        if world_size <= 0 or shards_per_rank <= 0:
            raise ValidationError("world_size/shards_per_rank must be positive")
        rank_nodes = rank_nodes or {
            r: f"node-{r // 8}" for r in range(world_size)
        }
        if set(rank_nodes) != set(range(world_size)):
            raise ValidationError("rank_nodes must map every rank 0..N-1")
        job_id = job_id or f"job-{uuid.uuid4().hex[:10]}"
        with self._lock:
            if job_id in self._jobs:
                raise Conflict(f"job {job_id} already exists")
            job = Job(
                job_id=job_id,
                name=name,
                world_size=world_size,
                shards_per_rank=shards_per_rank,
                rank_nodes=rank_nodes,
                store=self.store,
                scheduler=self.scheduler,
                lease_ttl=lease_ttl,
            )
            self._jobs[job_id] = job
            job._log("job_created", {"name": name, "world_size": world_size})
            return job

    def get(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise NotFound(f"unknown job {job_id}")
        return job

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    # ---------------------------------------------------------- preempt
    def preempt(
        self,
        job_id: str,
        *,
        reason: str = "scheduler_reclaim",
        kind: str = "all",
        nodes: list[str] | None = None,
        grace_seconds: float = 30.0,
        force_new_epoch: bool = False,
    ) -> dict:
        """调度器回收节点: 签发抢占纪元。

        抢占通知可能因重试重复投递: 当作业已处于 PREEMPTING 且未显式声明
        新抢占 (``force_new_epoch=True``) 时, 原样回传当前纪元, 不递增、
        不轮换令牌。调度器确定要再次回收 (新的一轮抢占) 时传
        force_new_epoch=True, 旧的未完成检查点被 SUPERSEDED。
        """
        job = self.get(job_id)
        with job.lock:
            ep = self.scheduler.current_epoch(job_id)
            if (
                not force_new_epoch
                and ep is not None
                and job.state == PREEMPTING
                and job.current_epoch == ep.epoch
            ):
                job._log("preempt_duplicate", {"epoch": ep.epoch})
                return {
                    "preempted": False,
                    "idempotent_replay": True,
                    "epoch": ep.to_dict(),
                    "checkpoint": job.cpm.get(ep.generation).to_dict(),
                    "epoch_token": ep.token,
                }
            if job.state in (RELEASED, ABORTED):
                raise Conflict(f"job is {job.state}, cannot preempt")

            ep = self.scheduler.issue_preemption(
                job_id,
                reason=reason,
                kind=kind,
                nodes=nodes,
                grace_seconds=grace_seconds,
            )
            job.current_epoch = ep.epoch
            job.current_generation = ep.generation
            job.state = PREEMPTING
            job.epoch_token = ep.token

            job.cpm.supersede_pending(ep.generation)
            job.cpm.open(
                epoch=ep.epoch,
                generation=ep.generation,
                world_size=job.world_size,
                shards_per_rank=job.shards_per_rank,
                processes=[job.processes[r].to_dict() for r in
                           sorted(job.processes)],
            )
            # 抢占瞬间吊销当前输出租约: 旧进程的迟到写入立即失去资格
            revoked = job.leases.revoke_generation(job.output_generation)
            job._log(
                "preempt_issued",
                {
                    "epoch": ep.epoch,
                    "generation": ep.generation,
                    "kind": kind,
                    "nodes": list(ep.nodes),
                    "grace_seconds": grace_seconds,
                    "revoked_lease": revoked.lease_id if revoked else None,
                },
                ts=ep.issued_ts,
            )
            return {
                "preempted": True,
                "idempotent_replay": False,
                "epoch": ep.to_dict(),
                "checkpoint": job.cpm.get(ep.generation).to_dict(),
                "epoch_token": ep.token,
            }

    # ----------------------------------------------------- node progress
    def heartbeat(self, job_id: str, node_id: str, now: float | None = None):
        job = self.get(job_id)
        ts = now or time.time()
        with job.lock:
            node = job.nodes.get(node_id)
            if node is None:
                raise NotFound(f"unknown node {node_id}")
            node.last_heartbeat = ts
            if node.state == LOST:
                node.state = UP
                job._log("node_recovered", {"node": node_id})
            for rank in node.ranks:
                proc = job.processes[rank]
                proc.last_heartbeat = ts
                if proc.state == LOST:
                    proc.state = ALIVE
                    proc.incarnation += 1
            return node.to_dict()

    def report_progress(self, job_id: str, rank: int, input_pos: int):
        job = self.get(job_id)
        with job.lock:
            proc = job.processes.get(rank)
            if proc is None:
                raise NotFound(f"unknown rank {rank}")
            proc.last_input_pos = input_pos
            proc.last_progress_ts = time.time()
            return proc.to_dict()

    def mark_node_lost(self, job_id: str, node_id: str):
        """节点失联: 节点与其上全部 rank 标记 LOST, 阻塞其分片。"""
        job = self.get(job_id)
        with job.lock:
            node = job.nodes.get(node_id)
            if node is None:
                raise NotFound(f"unknown node {node_id}")
            node.state = LOST
            lost_ranks = []
            for rank in node.ranks:
                proc = job.processes[rank]
                proc.state = LOST
                lost_ranks.append(rank)
            job._log("node_lost", {"node": node_id, "ranks": lost_ranks})
            return {"node": node.to_dict(), "lost_ranks": lost_ranks}

    # ------------------------------------------------------- shard write
    def _check_epoch_token(self, job: Job, token: str):
        payload = self.scheduler.verify_epoch_token(
            token, job_id=job.job_id, epoch=job.current_epoch
        )
        if payload["generation"] != job.current_generation:
            raise FenceViolation(
                f"token generation {payload['generation']} does not match "
                f"current generation {job.current_generation}"
            )
        return payload

    def upload_shard(
        self,
        job_id: str,
        *,
        epoch_token: str,
        rank: int,
        index: int,
        node: str,
        input_pos: int,
        data: bytes,
    ) -> dict:
        job = self.get(job_id)
        with job.lock:
            self._check_epoch_token(job, epoch_token)
            if job.state != PREEMPTING:
                raise Conflict(f"job is {job.state}, not accepting shards")
            proc = job.processes.get(rank)
            if proc is None:
                raise NotFound(f"unknown rank {rank}")
            if proc.state == LOST:
                raise Conflict(
                    f"rank {rank} on {node} is marked LOST; shard cannot be "
                    "accepted until the node recovers/heartbeats"
                )
            shard, is_new = job.cpm.put_shard(
                generation=job.current_generation,
                rank=rank,
                index=index,
                data=data,
                node=node,
                input_pos=input_pos,
            )
            cp = job.cpm.get(job.current_generation)
            job._log(
                "shard_uploaded" if is_new else "shard_upload_duplicate",
                {
                    "shard": shard_id(rank, index),
                    "generation": job.current_generation,
                    "blob": shard.blob[:12],
                    "input_pos": input_pos,
                },
            )
            return {"shard": shard.ref(), "stored": is_new,
                    "checkpoint": cp.to_dict()}

    # ------------------------------------------------------------- commit
    def commit_checkpoint(self, job_id: str, *, epoch_token: str) -> dict:
        """提交检查点: 全部分片重校验通过才可 COMPLETE。"""
        job = self.get(job_id)
        with job.lock:
            self._check_epoch_token(job, epoch_token)
            gen = job.current_generation
            cp_before = job.cpm.get(gen)
            if cp_before.state == COMPLETE:
                job._log("commit_duplicate", {"generation": gen})
                return {"checkpoint": cp_before.to_dict(), "already_complete": True}
            if cp_before.state != PENDING:
                raise Conflict(
                    f"checkpoint gen{gen} is {cp_before.state}, cannot commit"
                )
            cp = job.cpm.commit(gen)
            job._log("checkpoint_complete", {
                "generation": gen, "shards": len(cp.shards)})
            return {"checkpoint": cp.to_dict(), "already_complete": False}

    # ------------------------------------------------------------- release
    def confirm_release(self, job_id: str, *, epoch_token: str) -> dict:
        """确认释放资源 — 只有当前纪元检查点 COMPLETE 才允许。"""
        job = self.get(job_id)
        with job.lock:
            self._check_epoch_token(job, epoch_token)
            gen = job.current_generation
            cp = job.cpm.get(gen)
            if cp.state != COMPLETE:
                raise Conflict(
                    "resource release refused: checkpoint gen"
                    f"{gen} is {cp.state}, not COMPLETE; missing/verified "
                    f"shards: {cp.missing()}"
                )
            job.state = RELEASED
            job.released_generation = gen
            job.released_ts = time.time()
            job.leases.revoke_generation(job.output_generation)
            job._log("resources_released", {"generation": gen})
            return {"released": True, "generation": gen,
                    "released_ts": job.released_ts, "job": job.snapshot()}

    def abort_epoch(self, job_id: str, *, reason: str = "grace_expired") -> dict:
        """调度器在宽限期用尽后放弃: 检查点 INCOMPLETE, 作业 ABORTED。"""
        job = self.get(job_id)
        with job.lock:
            gen = job.current_generation
            cp = job.cpm.get(gen)
            if cp.state == COMPLETE:
                raise Conflict("checkpoint already complete; release instead")
            job.cpm.mark_incomplete(gen)
            job.state = ABORTED
            job.aborted_ts = time.time()
            job.leases.revoke_generation(job.output_generation)
            job._log("epoch_abandoned", {"generation": gen, "reason": reason})
            return {"aborted": True, "generation": gen,
                    "checkpoint": cp.to_dict()}

    # -------------------------------------------------------------- output
    def append_output(
        self,
        job_id: str,
        *,
        lease_token: str,
        output_id: str,
        data: bytes,
        rank: int | None = None,
    ) -> dict:
        """提交最终输出。租约栅栏 + (代际, output_id) 幂等双重防护。"""
        job = self.get(job_id)
        with job.lock:
            gen = job.output_generation
            try:
                lease = job.leases.authorize(
                    lease_token, expected_generation=gen
                )
            except FenceViolation as exc:
                digest = hashlib.sha256(data).hexdigest()[:12]
                job._log("output_fenced", {
                    "output_id": output_id, "attempted_blob": digest,
                    "reason": str(exc)})
                raise
            digest, size, stored_blob = job.store.put(data)
            rec, is_new = job.outputs.append(
                generation=gen,
                output_id=output_id,
                blob=digest,
                size=size,
                rank=rank,
                ts=time.time(),
            )
            job._log(
                "output_committed" if is_new else "output_duplicate",
                {"output_id": output_id, "generation": gen,
                 "blob": digest[:12], "rank": rank},
            )
            return {"record": rec.to_dict(), "delivered": is_new,
                    "generation": gen, "lease_id": lease.lease_id}

    def list_outputs(self, job_id: str, generation: int | None = None):
        job = self.get(job_id)
        return [r.to_dict() for r in job.outputs.list(generation)]

    # ------------------------------------------------------------- restore
    def restore(
        self,
        job_id: str,
        *,
        generation: int,
        new_rank_nodes: dict[int, str] | None = None,
        lease_ttl: float | None = None,
    ) -> RestoreReport:
        """从指定代际检查点恢复: 同代际 + 全量重校验 + 新输出租约。

        对同一代际重复调用幂等, 直接返回已生成的报告 (不重发租约)。
        """
        job = self.get(job_id)
        ttl = lease_ttl or job._lease_ttl
        with job.lock:
            # 仅当请求代际同时是 *当前已释放代际* 且已恢复过, 才幂等短路;
            # 再次抢占释放后回指旧代际必须继续走向栅栏检查
            if (
                job.restored_generation == generation
                and job.released_generation == generation
            ):
                return job._last_restore  # type: ignore[return-value]
            if job.state not in (RELEASED, RUNNING):
                raise Conflict(
                    f"restore requires RELEASED job (current {job.state})"
                )
            existing = {c.generation for c in job.cpm.list_all()}
            if generation not in existing:
                report = self._failed_report(
                    job, generation, checkpoint_state="MISSING",
                    error=f"no checkpoint exists for generation {generation}")
                job._last_restore = report
                return report
            if job.released_generation != generation:
                raise FenceViolation(
                    "restore generation mismatch: checkpoint was released "
                    f"at gen{job.released_generation}, requested gen"
                    f"{generation}. Must restore the exact same checkpoint "
                    "generation.")
            cp = job.cpm.get(generation)

            if cp.state != COMPLETE:
                report = self._failed_report(
                    job, generation, checkpoint_state=cp.state,
                    error=f"checkpoint is {cp.state}, only COMPLETE "
                          "checkpoints can be restored")
                job._last_restore = report
                return report

            # 恢复时再次对每一个分片做内容重校验 (不信任提交时刻的结论)
            failed, positions = [], {}
            for r in range(cp.world_size):
                for i in range(cp.shards_per_rank):
                    sid = shard_id(r, i)
                    sh = cp.shards[sid]
                    if not job.store.verify(sh.blob):
                        failed.append(sid)
                    else:
                        positions[sid] = sh.input_pos
            if failed:
                report = self._failed_report(
                    job, generation, checkpoint_state=cp.state,
                    error="post-hoc shard verification failed",
                    failed=failed, cp=cp)
                job._last_restore = report
                return report

            new_rank_nodes = new_rank_nodes or {
                r: n for r, n in cp.processes
            }
            if set(new_rank_nodes) != set(range(cp.world_size)):
                raise ValidationError("new placement must cover all ranks")

            old_procs = [{"rank": r, "node": n} for r, n in cp.processes]
            new_procs = [
                {"rank": r, "node": new_rank_nodes[r]}
                for r in sorted(new_rank_nodes)
            ]
            relocated = [
                r for r, n in cp.processes if new_rank_nodes[r] != n
            ]
            warnings = []
            if relocated:
                warnings.append(
                    f"{len(relocated)} 个 rank 被重新放置到不同节点: "
                    f"ranks {relocated}"
                )

            # 同代际恢复: 输出代际沿用 checkpoint 代际, 但签发全新租约;
            # 旧 lease 已在抢占/释放时吊销, 旧进程令牌不可能通过新栅栏
            lease = job.leases.grant(generation, ttl)
            job.output_generation = generation
            job.state = RUNNING
            job.restored_generation = generation

            # 重建节点/进程视图 (新的 incarnation)
            for r, n in new_rank_nodes.items():
                old_node = job.processes[r].node
                proc = job.processes[r]
                proc.node = n
                proc.state = ALIVE
                proc.incarnation += 1
                proc.last_heartbeat = time.time()
                node = job.nodes.setdefault(n, Node(node_id=n))
                if r not in node.ranks:
                    node.ranks.append(r)
                node.state = UP
                node.last_heartbeat = time.time()
                if old_node != n and old_node in job.nodes:
                    old = job.nodes[old_node]
                    old.ranks = [x for x in old.ranks if x != r]

            report = RestoreReport(
                job_id=job.job_id,
                requested_generation=generation,
                success=True,
                ts=time.time(),
                checkpoint_state=cp.state,
                manifest_digest=(cp.manifest or {}).get("manifest_digest"),
                shards_total=cp.expected_total,
                shards_verified=cp.expected_total,
                shards_failed=[],
                input_positions=positions,
                old_processes=old_procs,
                new_processes=new_procs,
                process_set_match=True,
                relocated_ranks=relocated,
                lease=lease.to_dict(),
                warnings=warnings,
            )
            job._last_restore = report
            job._log("restore_verified", {
                "generation": generation, "shards": cp.expected_total,
                "lease_id": lease.lease_id, "relocated": relocated})
            return report

    def _failed_report(
        self, job: Job, generation: int, *, checkpoint_state: str,
        error: str, failed: list[str] | None = None, cp=None,
    ) -> RestoreReport:
        positions, total = {}, 0
        if cp is not None:
            total = cp.expected_total
            for r in range(cp.world_size):
                for i in range(cp.shards_per_rank):
                    sid = shard_id(r, i)
                    if sid in cp.shards and sid not in (failed or []):
                        positions[sid] = cp.shards[sid].input_pos
        report = RestoreReport(
            job_id=job.job_id,
            requested_generation=generation,
            success=False,
            ts=time.time(),
            checkpoint_state=checkpoint_state,
            manifest_digest=(
                (cp.manifest or {}).get("manifest_digest") if cp else None
            ),
            shards_total=total,
            shards_verified=len(positions),
            shards_failed=failed or [],
            input_positions=positions,
            old_processes=(
                [{"rank": r, "node": n} for r, n in cp.processes]
                if cp else []
            ),
            error=error,
        )
        job._log("restore_rejected", {"generation": generation, "error": error})
        return report

    def last_restore_report(self, job_id: str) -> RestoreReport:
        job = self.get(job_id)
        if job._last_restore is None:
            raise NotFound(f"no restore attempt for job {job_id}")
        return job._last_restore

    # ------------------------------------------------------------ progress
    def progress_page_data(self, job_id: str) -> dict:
        """节点进度页面/进度 API 的聚合数据。"""
        job = self.get(job_id)
        with job.lock:
            cps = [c.to_dict() for c in job.cpm.list_all()]
            shard_index: dict[str, dict] = {}
            for c in job.cpm.list_all():
                for sid, sh in c.shards.items():
                    shard_index[f"gen{c.generation}:{sid}"] = {
                        **sh.ref(),
                        "generation": c.generation,
                        "uploaded_ts": sh.uploaded_ts,
                    }
            return {
                "job": job.snapshot(),
                "nodes": [n.to_dict() for n in
                          sorted(job.nodes.values(), key=lambda n: n.node_id)],
                "processes": [job.processes[r].to_dict()
                              for r in sorted(job.processes)],
                "checkpoints": cps,
                "shards": shard_index,
                "outputs": [r.to_dict() for r in job.outputs.list()],
                "audit": list(reversed(job.audit)),
                "server_ts": time.time(),
            }
