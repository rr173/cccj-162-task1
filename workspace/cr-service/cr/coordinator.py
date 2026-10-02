"""检查点协调器：抢占纪元 + 内容寻址分片 + 全量校验封印状态机。

代际生命周期::

    OPEN ──(全部分片与进程记录校验通过)──▶ SEALED   # 唯一「可用」状态
      │
      └──(抢占失败/节点回收撤销)──────────▶ ABORTED

关键规则：

* ``begin_preemption`` 单调推进 epoch，并开出绑定该 epoch 的 OPEN 代际；
* 分片规格（承诺摘要）在 OPEN 时由进程集合确定，上传内容必须与承诺一致；
* 重复检查点：相同 (job, epoch) 得到同一个代际，重复上传同摘要分片是
  幂等的；承诺不同则返回 ``checkpoint_conflict``；
* 封印前会对 CAS 中的全部 blob **重新拉取并重新哈希**（不信任任何缓存
  状态），任一缺失/不符即拒绝封印；
* 只有 SEALED 才允许恢复；OPEN 永远不会被当作可用检查点。
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from . import errors
from .events import EventLog
from .storage import CASStore, atomic_write, canonical_json, load_json, sha256_hex

# 代际状态
OPEN = "OPEN"
SEALED = "SEALED"
ABORTED = "ABORTED"

# 分片校验状态
EXPECTED = "EXPECTED"    # 已登记承诺，等待上传
VERIFIED = "VERIFIED"    # 内容与承诺一致
REJECTED = "REJECTED"    # 上传内容与承诺不符（损坏/错误分片）


@dataclass
class ProcessDesc:
    """检查点中的一个进程（rank）：身份、数据分片、输入位点。"""

    rank: int
    node: str
    pid: int
    shards: List[str] = field(default_factory=list)   # 本分片 id 列表
    input_positions: Dict[str, int] = field(default_factory=dict)  # 输入流 -> 位点
    last_output_seq: int = -1
    record_digest: Optional[str] = None  # 进程记录 blob（内容寻址）

    def spec(self) -> dict:
        return {
            "rank": self.rank,
            "node": self.node,
            "pid": self.pid,
            "shards": list(self.shards),
            "input_positions": dict(self.input_positions),
            "last_output_seq": self.last_output_seq,
        }


@dataclass
class ShardEntry:
    shard_id: str
    size: int
    expected_digest: Optional[str] = None
    status: str = EXPECTED
    digest: Optional[str] = None
    attempts: int = 0

    def to_dict(self) -> dict:
        return {
            "shard_id": self.shard_id,
            "size": self.size,
            "expected_digest": self.expected_digest,
            "status": self.status,
            "digest": self.digest,
            "attempts": self.attempts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ShardEntry":
        return cls(
            shard_id=d["shard_id"],
            size=d["size"],
            expected_digest=d.get("expected_digest"),
            status=d["status"],
            digest=d.get("digest"),
            attempts=d.get("attempts", 0),
        )


class Generation:
    """一个检查点代际。"""

    def __init__(
        self,
        gen_id: str,
        job_id: str,
        epoch: int,
        processes: Optional[Dict[int, ProcessDesc]] = None,
        shards: Optional[Dict[str, ShardEntry]] = None,
    ) -> None:
        self.gen_id = gen_id
        self.job_id = job_id
        self.epoch = epoch
        self.status = OPEN
        self.processes: Dict[int, ProcessDesc] = processes or {}
        self.shards: Dict[str, ShardEntry] = shards or {}
        self.manifest_digest: Optional[str] = None
        self.created_at = time.time()
        self.sealed_at: Optional[float] = None
        self.aborted_at: Optional[float] = None
        self.reason: Optional[str] = None
        # 冲突尝试留痕（重复但内容不同的检查点）
        self.conflicts: List[dict] = []

    # ---- 序列化 -------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "gen_id": self.gen_id,
            "job_id": self.job_id,
            "epoch": self.epoch,
            "status": self.status,
            "processes": {
                str(rank): {
                    "spec": p.spec(),
                    "record_digest": p.record_digest,
                }
                for rank, p in sorted(self.processes.items())
            },
            "shards": {sid: s.to_dict() for sid, s in sorted(self.shards.items())},
            "manifest_digest": self.manifest_digest,
            "created_at": self.created_at,
            "sealed_at": self.sealed_at,
            "aborted_at": self.aborted_at,
            "reason": self.reason,
            "conflicts": self.conflicts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Generation":
        gen = cls(d["gen_id"], d["job_id"], d["epoch"])
        gen.status = d["status"]
        for rank_s, pd in d.get("processes", {}).items():
            spec = pd["spec"]
            gen.processes[int(rank_s)] = ProcessDesc(
                rank=spec["rank"],
                node=spec["node"],
                pid=spec["pid"],
                shards=list(spec["shards"]),
                input_positions=dict(spec["input_positions"]),
                last_output_seq=spec.get("last_output_seq", -1),
                record_digest=pd.get("record_digest"),
            )
        gen.shards = {
            sid: ShardEntry.from_dict(sd)
            for sid, sd in d.get("shards", {}).items()
        }
        gen.manifest_digest = d.get("manifest_digest")
        gen.created_at = d.get("created_at", time.time())
        gen.sealed_at = d.get("sealed_at")
        gen.aborted_at = d.get("aborted_at")
        gen.reason = d.get("reason")
        gen.conflicts = d.get("conflicts", [])
        return gen

    # ---- 视图 ---------------------------------------------------------

    def missing(self) -> Tuple[List[str], List[int]]:
        """返回 (未就绪的分片 id, 未提交记录的 rank)。"""

        bad_shards = [
            sid
            for sid, s in sorted(self.shards.items())
            if s.status != VERIFIED
        ]
        bad_procs = [
            rank
            for rank in sorted(self.processes)
            if self.processes[rank].record_digest is None
        ]
        return bad_shards, bad_procs

    def is_complete(self) -> bool:
        if not self.processes:
            return False
        missing_shards, missing_procs = self.missing()
        return not missing_shards and not missing_procs

    def progress(self) -> dict:
        total = len(self.shards)
        verified = sum(1 for s in self.shards.values() if s.status == VERIFIED)
        rejected = sum(1 for s in self.shards.values() if s.status == REJECTED)
        total_p = len(self.processes)
        rec_p = sum(1 for p in self.processes.values() if p.record_digest is not None)
        return {
            "shards_verified": verified,
            "shards_total": total,
            "shards_rejected": rejected,
            "records_submitted": rec_p,
            "records_total": total_p,
        }


class CheckpointCoordinator:
    """检查点元数据与封印状态机（进程内单例，锁保护）。"""

    def __init__(self, root: str, cas: CASStore, events: EventLog) -> None:
        self.root = root
        self.meta_path = os.path.join(root, "coordinator.json")
        self.cas = cas
        self.events = events
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self.jobs: Dict[str, dict] = {}                       # job_id -> 作业元数据
        self.generations: Dict[str, Generation] = {}          # gen_id -> 代际
        self.open_by_epoch: Dict[Tuple[str, int], str] = {}   # (job, epoch) -> gen_id
        self.epoch: Dict[str, int] = {}                       # job_id -> 当前纪元
        self.latest_sealed: Dict[str, str] = {}               # job_id -> gen_id
        self._load()

    # ============================ 持久化 ===============================

    def _load(self) -> None:
        data = load_json(self.meta_path, default=None)
        if not data:
            return
        self.jobs = data.get("jobs", {})
        self.generations = {
            gid: Generation.from_dict(gd)
            for gid, gd in data.get("generations", {}).items()
        }
        self.open_by_epoch = {
            tuple(k): v  # type: ignore[dict-item]
            for k, v in data.get("open_by_epoch", [])
        }
        self.epoch = data.get("epoch", {})
        self.latest_sealed = data.get("latest_sealed", {})

    def _persist_locked(self) -> None:
        data = {
            "jobs": self.jobs,
            "generations": {gid: g.to_dict() for gid, g in self.generations.items()},
            "open_by_epoch": [list(k) for k in self.open_by_epoch],
            "epoch": self.epoch,
            "latest_sealed": self.latest_sealed,
        }
        atomic_write(self.meta_path, canonical_json(data))

    def _gen(self, gen_id: str) -> Generation:
        try:
            return self.generations[gen_id]
        except KeyError:
            raise errors.GenerationNotFound(gen_id=gen_id)

    def _require_job_locked(self, job_id: str) -> dict:
        job = self.jobs.get(job_id)
        if job is None:
            raise errors.JobNotFound(job_id=job_id)
        return job

    # ============================ 作业 =================================

    def create_job(self, job_id: str, ranks: int, name: str = "") -> dict:
        with self._lock:
            if job_id in self.jobs:
                return self.jobs[job_id]
            self.jobs[job_id] = {
                "job_id": job_id,
                "name": name or job_id,
                "ranks": ranks,
                "created_at": time.time(),
            }
            self.epoch.setdefault(job_id, 0)
            self._persist_locked()
            self.events.emit(
                "coordinator", "info", f"作业 {job_id} 注册（{ranks} 个进程）",
                job_id=job_id, ranks=ranks,
            )
            return dict(self.jobs[job_id])

    def get_job(self, job_id: str) -> dict:
        with self._lock:
            return dict(self._require_job_locked(job_id))

    # ============================ 纪元/代际 ============================

    def begin_preemption(
        self,
        job_id: str,
        process_specs: List[dict],
        shard_specs: List[dict],
    ) -> dict:
        """调度器签发抢占纪元并开启新的检查点代际。

        ``process_specs``: [{rank,node,pid,shards,input_positions,last_output_seq}]
        ``shard_specs``:   [{shard_id,size,expected_digest}]
        对同一 (作业, 纪元) 重复签发是幂等的，返回同一个 OPEN 代际；
        但规格必须一致，否则 ``checkpoint_conflict``。
        """

        with self._lock:
            self._require_job_locked(job_id)
            # 同一作业在「当前纪元仍有 OPEN 代际」时重复签发 => 幂等复用。
            current_epoch = self.epoch.get(job_id, 0)
            open_id = self.open_by_epoch.get((job_id, current_epoch))
            if open_id and self.generations[open_id].status == OPEN:
                self._assert_specs_match_locked(
                    self.generations[open_id], process_specs, shard_specs
                )
                return self.generation_view(self.generations[open_id])

            new_epoch = current_epoch + 1
            key = (job_id, new_epoch)
            existing = self.open_by_epoch.get(key)
            if existing:
                self._assert_specs_match_locked(
                    self.generations[existing], process_specs, shard_specs
                )
                return self.generation_view(self.generations[existing])

            gen_id = f"{job_id}-ep{new_epoch}"
            gen = Generation(gen_id=gen_id, job_id=job_id, epoch=new_epoch)

            for spec in process_specs:
                gen.processes[spec["rank"]] = ProcessDesc(
                    rank=spec["rank"],
                    node=spec["node"],
                    pid=spec["pid"],
                    shards=list(spec.get("shards", [])),
                    input_positions=dict(spec.get("input_positions", {})),
                    last_output_seq=spec.get("last_output_seq", -1),
                )
            for spec in shard_specs:
                gen.shards[spec["shard_id"]] = ShardEntry(
                    shard_id=spec["shard_id"],
                    size=spec["size"],
                    expected_digest=spec.get("expected_digest"),
                )

            self.generations[gen_id] = gen
            self.open_by_epoch[key] = gen_id
            self.epoch[job_id] = new_epoch
            self._persist_locked()
            self.events.emit(
                "scheduler", "info",
                f"抢占纪元 {new_epoch} 签发，代际 {gen_id} 开启",
                job_id=job_id, epoch=new_epoch, gen_id=gen_id,
                processes=len(gen.processes), shards=len(gen.shards),
            )
            return self.generation_view(gen)

    def _assert_specs_match_locked(
        self,
        gen: Generation,
        process_specs: List[dict],
        shard_specs: List[dict],
    ) -> None:
        """重复签发检查点时，承诺必须与首次完全一致。"""

        if {p["rank"] for p in process_specs} != set(gen.processes):
            raise errors.CheckpointConflict(
                "重复检查点的进程集合与首次签发不一致",
                gen_id=gen.gen_id,
            )
        existing = {sid: (s.size, s.expected_digest) for sid, s in gen.shards.items()}
        incoming = {
            s["shard_id"]: (s["size"], s.get("expected_digest")) for s in shard_specs
        }
        if existing != incoming:
            gen.conflicts.append({"at": time.time(), "kind": "spec_mismatch"})
            raise errors.CheckpointConflict(
                "重复检查点的分片承诺与首次签发不一致",
                gen_id=gen.gen_id,
            )

    def current_epoch(self, job_id: str) -> int:
        with self._lock:
            self._require_job_locked(job_id)
            return self.epoch.get(job_id, 0)

    def peek_next_epoch(self, job_id: str) -> int:
        """下一次抢占将使用的纪元号（不修改状态）。"""

        with self._lock:
            self._require_job_locked(job_id)
            return self.epoch.get(job_id, 0) + 1

    def open_generation_id(self, job_id: str, epoch: int) -> Optional[str]:
        with self._lock:
            return self.open_by_epoch.get((job_id, epoch))

    def get_generation(self, gen_id: str) -> dict:
        with self._lock:
            return self.generation_view(self._gen(gen_id))

    # ============================ 上传/校验 ============================

    def _guard_upload_locked(
        self, gen: Generation, epoch: int
    ) -> None:
        if epoch != gen.epoch:
            raise errors.EpochStale(
                f"纪元 {epoch} 已过期，当前为 {gen.epoch}，拒绝旧纪元写入",
                gen_id=gen.gen_id, epoch=epoch, current_epoch=gen.epoch,
            )
        if gen.status == ABORTED:
            raise errors.GenerationAborted(
                "代际已中止，旧进程的迟到上传被拒绝", gen_id=gen.gen_id,
            )
        if gen.status == SEALED:
            # SEALED 后相同摘要上传允许幂等成功，其他改动一律拒绝。
            pass

    def upload_shard(
        self,
        gen_id: str,
        epoch: int,
        shard_id: str,
        data: bytes,
        expected_digest: Optional[str] = None,
    ) -> dict:
        """上传一个数据分片；内容哈希必须匹配登记的承诺。"""

        with self._lock:
            gen = self._gen(gen_id)
            self._guard_upload_locked(gen, epoch)
            entry = gen.shards.get(shard_id)
            if entry is None:
                raise errors.ShardNotFound(gen_id=gen_id, shard_id=shard_id)
            entry.attempts += 1

            if entry.status == VERIFIED:
                # 幂等：相同内容重复上传（重复检查点/重传）直接成功。
                if sha256_hex(data) == entry.digest:
                    return self.generation_view(gen)
                entry.status = REJECTED
                gen.conflicts.append({
                    "at": time.time(), "kind": "shard_digest_changed",
                    "shard_id": shard_id,
                })
                self._persist_locked()
                raise errors.CheckpointConflict(
                    "同一分片被重复提交了不同内容", shard_id=shard_id,
                )

            promise = expected_digest or entry.expected_digest
            actual = sha256_hex(data)
            if promise is not None and actual != promise:
                entry.status = REJECTED
                self.events.emit(
                    "coordinator", "reject",
                    f"分片 {shard_id} 哈希与承诺不符，校验失败",
                    gen_id=gen_id, shard_id=shard_id,
                    expected=promise[:12], actual=actual[:12],
                )
                self._persist_locked()
                raise errors.ShardHashMismatch(
                    f"分片 {shard_id} 内容损坏或错误",
                    shard_id=shard_id, expected=promise, actual=actual,
                )
            # 写入 CAS（put 自身会再算一次哈希，双重保险）。
            digest = self.cas.put(data, expected_digest=promise)
            entry.digest = digest
            entry.expected_digest = digest
            entry.status = VERIFIED
            self._persist_locked()
            prog = gen.progress()
            self.events.emit(
                "coordinator", "info",
                f"分片 {shard_id} 校验入库 ({prog['shards_verified']}/{prog['shards_total']})",
                gen_id=gen_id, shard_id=shard_id, digest=digest[:12],
            )
            self._cond.notify_all()
            return self.generation_view(gen)

    def submit_rank_record(
        self,
        gen_id: str,
        epoch: int,
        rank: int,
        record: dict,
    ) -> dict:
        """提交进程记录（pid、输入位点等），内容寻址存入 CAS。"""

        with self._lock:
            gen = self._gen(gen_id)
            self._guard_upload_locked(gen, epoch)
            proc = gen.processes.get(rank)
            if proc is None:
                raise errors.CheckpointConflict(
                    "进程集合中不存在该 rank", gen_id=gen_id, rank=rank,
                )
            payload = canonical_json(record)
            digest = sha256_hex(payload)
            # 承诺的身份信息必须与签发时的进程集合一致。
            must = proc.spec()
            for key in ("rank", "node", "pid", "shards", "input_positions",
                        "last_output_seq"):
                if record.get(key) != must[key]:
                    raise errors.CheckpointConflict(
                        f"rank {rank} 的进程记录与签发承诺字段 {key} 不一致",
                        gen_id=gen_id, rank=rank, field=key,
                    )
            if proc.record_digest is not None:
                if proc.record_digest == digest:
                    return self.generation_view(gen)  # 幂等重复
                raise errors.CheckpointConflict(
                    "同一 rank 被重复提交了不同记录", gen_id=gen_id, rank=rank,
                )
            self.cas.put(payload, expected_digest=digest)
            proc.record_digest = digest
            self._persist_locked()
            prog = gen.progress()
            self.events.emit(
                "coordinator", "info",
                f"rank {rank} 进程记录已提交 "
                f"({prog['records_submitted']}/{prog['records_total']})",
                gen_id=gen_id, rank=rank, digest=digest[:12],
            )
            self._cond.notify_all()
            return self.generation_view(gen)

    # ============================ 封印 ================================

    def _verify_all_blobs_locked(
        self, gen: Generation, mark_rejected: bool = False
    ) -> List[str]:
        """从 CAS 重新拉取每个 blob 并重新哈希；返回有问题的对象列表。

        ``mark_rejected`` 用于封印路径：发现 blob 缺失/位腐烂时把对应
        分片状态回退为 REJECTED，使正确内容可以重新上传修复，而 OPEN
        代际本身保持不可封印（没有全绿就绝不 SEALED）。
        """

        problems: List[str] = []
        for sid, entry in gen.shards.items():
            if entry.status != VERIFIED or not entry.digest:
                problems.append(f"shard:{sid}:not_verified")
                continue
            try:
                raw = self.cas.get(entry.digest)
            except FileNotFoundError:
                problems.append(f"shard:{sid}:blob_missing")
                if mark_rejected:
                    entry.status = REJECTED
                continue
            if sha256_hex(raw) != entry.digest:
                problems.append(f"shard:{sid}:digest_changed")
                if mark_rejected:
                    entry.status = REJECTED
                continue
            if entry.expected_digest and entry.digest != entry.expected_digest:
                problems.append(f"shard:{sid}:promise_broken")
                if mark_rejected:
                    entry.status = REJECTED
        for rank, proc in gen.processes.items():
            if not proc.record_digest:
                problems.append(f"rank:{rank}:record_missing")
                continue
            try:
                raw = self.cas.get(proc.record_digest)
            except FileNotFoundError:
                problems.append(f"rank:{rank}:blob_missing")
                continue
            if sha256_hex(raw) != proc.record_digest:
                problems.append(f"rank:{rank}:digest_changed")
        return problems

    def _build_manifest_locked(self, gen: Generation) -> bytes:
        manifest = {
            "gen_id": gen.gen_id,
            "job_id": gen.job_id,
            "epoch": gen.epoch,
            "processes": {
                str(rank): {
                    "spec": proc.spec(),
                    "record_digest": proc.record_digest,
                }
                for rank, proc in sorted(gen.processes.items())
            },
            "shards": {
                sid: {"size": s.size, "digest": s.digest}
                for sid, s in sorted(gen.shards.items())
            },
        }
        return canonical_json(manifest)

    def seal(self, gen_id: str, epoch: int) -> dict:
        """全部校验通过后原子封印。封印是「资源可释放」的唯一凭证。"""

        with self._lock:
            gen = self._gen(gen_id)
            if epoch != gen.epoch:
                raise errors.EpochStale(
                    "旧纪元的封印请求被拒绝",
                    gen_id=gen_id, epoch=epoch, current_epoch=gen.epoch,
                )
            if gen.status == SEALED:
                return self.generation_view(gen)  # 封印幂等
            if gen.status == ABORTED:
                raise errors.GenerationAborted(gen_id=gen_id)

            missing_shards, missing_procs = gen.missing()
            if missing_shards or missing_procs:
                self.events.emit(
                    "coordinator", "reject",
                    f"拒绝封印代际 {gen_id}：检查点不完整",
                    gen_id=gen_id,
                    missing_shards=missing_shards, missing_ranks=missing_procs,
                )
                raise errors.CheckpointIncomplete(
                    "存在尚未校验完成的分片或进程记录",
                    gen_id=gen_id,
                    missing_shards=missing_shards,
                    missing_ranks=missing_procs,
                )

            problems = self._verify_all_blobs_locked(gen, mark_rejected=True)
            if problems:
                self._persist_locked()
                self.events.emit(
                    "coordinator", "reject",
                    f"拒绝封印代际 {gen_id}：全量复验发现 {len(problems)} 个问题",
                    gen_id=gen_id, problems=problems,
                )
                raise errors.ShardHashMismatch(
                    "全量复验失败，不完整/损坏的检查点不得标记可用",
                    gen_id=gen_id, problems=problems,
                )

            manifest = self._build_manifest_locked(gen)
            manifest_digest = self.cas.put(
                manifest, expected_digest=sha256_hex(manifest)
            )
            gen.manifest_digest = manifest_digest
            gen.status = SEALED
            gen.sealed_at = time.time()
            self.latest_sealed[gen.job_id] = gen.gen_id
            self._persist_locked()
            self._cond.notify_all()
            self.events.emit(
                "coordinator", "ok",
                f"代际 {gen_id} 已封印（manifest={manifest_digest[:12]}），"
                "检查点确认可用，允许释放节点",
                gen_id=gen_id, manifest=manifest_digest,
                blobs=len(gen.shards) + len(gen.processes) + 1,
            )
            return self.generation_view(gen)

    def wait_for_completion(
        self, gen_id: str, timeout: Optional[float] = None
    ) -> dict:
        """等待代际离开 OPEN（被封印或中止），供编排线程同步。"""

        deadline = None if timeout is None else time.time() + timeout
        with self._cond:
            while self._gen(gen_id).status == OPEN:
                remaining = None if deadline is None else deadline - time.time()
                if remaining is not None and remaining <= 0:
                    break
                self._cond.wait(remaining)
            return self.generation_view(self._gen(gen_id))

    def abort(self, gen_id: str, reason: str) -> dict:
        """抢占失败：中止 OPEN 代际。迟到上传此后一律被拒绝。"""

        with self._lock:
            gen = self._gen(gen_id)
            if gen.status == ABORTED:
                return self.generation_view(gen)
            if gen.status == SEALED:
                raise errors.PreemptionConflict(
                    "代际已封印，不能中止", gen_id=gen_id,
                )
            gen.status = ABORTED
            gen.aborted_at = time.time()
            gen.reason = reason
            self._persist_locked()
            self._cond.notify_all()
            self.events.emit(
                "coordinator", "warn",
                f"代际 {gen_id} 已中止（{reason}）；不完整检查点废弃，"
                "节点不予回收",
                gen_id=gen_id, reason=reason,
            )
            return self.generation_view(gen)

    def verify_generation(self, gen_id: str) -> dict:
        """恢复前的独立验证：必须 SEALED 且全量 blob 复验通过。"""

        with self._lock:
            gen = self._gen(gen_id)
            if gen.status != SEALED:
                raise errors.GenerationNotSealed(
                    f"代际状态为 {gen.status}，不可恢复", gen_id=gen_id,
                )
            problems = self._verify_all_blobs_locked(gen)
            try:
                manifest_raw = self.cas.get(gen.manifest_digest or "")
                if sha256_hex(manifest_raw) != gen.manifest_digest:
                    problems.append("manifest:digest_changed")
                elif manifest_raw != self._build_manifest_locked(gen):
                    # 元数据与封印时的清单内容必须逐字节一致（同一代际）。
                    problems.append("manifest:content_mismatch")
            except FileNotFoundError:
                problems.append("manifest:missing")
            return {
                "ok": not problems,
                "gen_id": gen_id,
                "problems": problems,
                "blob_count": len(gen.shards) + len(gen.processes) + 1,
            }

    def latest_sealed_generation(self, job_id: str) -> Optional[str]:
        with self._lock:
            self._require_job_locked(job_id)
            return self.latest_sealed.get(job_id)

    # ============================ 视图 ================================

    def generation_view(self, gen: Generation) -> dict:
        missing_shards, missing_procs = gen.missing()
        return {
            "gen_id": gen.gen_id,
            "job_id": gen.job_id,
            "epoch": gen.epoch,
            "status": gen.status,
            "manifest_digest": gen.manifest_digest,
            "created_at": gen.created_at,
            "sealed_at": gen.sealed_at,
            "aborted_at": gen.aborted_at,
            "reason": gen.reason,
            "progress": gen.progress(),
            "missing": {"shards": missing_shards, "ranks": missing_procs},
            "processes": {
                str(rank): {**p.spec(), "record_digest": p.record_digest}
                for rank, p in sorted(gen.processes.items())
            },
            "shards": {
                sid: s.to_dict() for sid, s in sorted(gen.shards.items())
            },
            "conflicts": list(gen.conflicts),
        }

    def list_generations(self, job_id: str) -> List[dict]:
        with self._lock:
            self._require_job_locked(job_id)
            return [
                self.generation_view(g)
                for g in sorted(
                    (g for g in self.generations.values() if g.job_id == job_id),
                    key=lambda g: g.epoch,
                )
            ]

    def status(self) -> dict:
        with self._lock:
            return {
                "jobs": dict(self.jobs),
                "epoch": dict(self.epoch),
                "latest_sealed": dict(self.latest_sealed),
                "generations": [
                    {
                        "gen_id": g.gen_id,
                        "job_id": g.job_id,
                        "epoch": g.epoch,
                        "status": g.status,
                        "progress": g.progress(),
                    }
                    for g in sorted(self.generations.values(), key=lambda x: x.gen_id)
                ],
            }
