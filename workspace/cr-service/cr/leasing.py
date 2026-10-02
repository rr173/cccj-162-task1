"""输出租约与输出汇：双重输出的两道防线。

防线一（租约栅栏）
    租约 ``(token 单调递增, epoch, gen_id)`` 由协调器在恢复时重新签发；
    签发新租约会吊销该作业的所有旧租约。旧进程在抢占/恢复后带着旧
    token 迟到写入时，``epoch`` 或 ``gen_id`` 或 ``token`` 任一不符
    即被拒绝。

防线二（键级幂等）
    即使旧进程在租约仍然有效的极短窗口内并发写入，输出汇对 ``out_key``
    做写一次（write-once）：
      * 相同内容 -> ``dedup``，不产生第二次输出；
      * 不同内容 -> ``conflict``，拒绝并留痕（双重输出企图被发现）。
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from . import errors
from .events import EventLog
from .storage import atomic_write, canonical_json, load_json, sha256_hex


@dataclass
class Lease:
    token: int
    job_id: str
    epoch: int
    gen_id: str
    issued_at: float
    revoked: bool = False
    revoked_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "token": self.token,
            "job_id": self.job_id,
            "epoch": self.epoch,
            "gen_id": self.gen_id,
            "issued_at": self.issued_at,
            "revoked": self.revoked,
            "revoked_reason": self.revoked_reason,
        }


class LeaseManager:
    """按作业维护当前有效输出租约；token 全局单调，绝不复用。"""

    def __init__(self, root: str, events: EventLog) -> None:
        self.path = os.path.join(root, "leases.json")
        self.events = events
        self._lock = threading.RLock()
        self._next_token = 1
        self.leases: Dict[int, Lease] = {}
        self.current: Dict[str, int] = {}   # job_id -> 当前租约 token
        self.history: Dict[str, List[int]] = {}
        self._load()

    def _load(self) -> None:
        data = load_json(self.path, default=None)
        if not data:
            return
        self._next_token = data.get("next_token", 1)
        self.leases = {
            int(t): Lease(**ld) for t, ld in data.get("leases", {}).items()
        }
        self.current = data.get("current", {})
        self.history = {j: list(v) for j, v in data.get("history", {}).items()}

    def _persist_locked(self) -> None:
        atomic_write(self.path, canonical_json({
            "next_token": self._next_token,
            "leases": {str(t): l.to_dict() for t, l in self.leases.items()},
            "current": self.current,
            "history": self.history,
        }))

    def acquire(self, job_id: str, epoch: int, gen_id: str, reason: str = "recover") -> Lease:
        """签发新租约并吊销同作业全部旧租约。"""

        with self._lock:
            old_token = self.current.get(job_id)
            if old_token is not None:
                old = self.leases.get(old_token)
                if old is not None:
                    old.revoked = True
                    old.revoked_reason = (
                        f"被新租约取代（reason={reason}, gen_id={gen_id}）"
                    )
                    self.events.emit(
                        "lease", "warn",
                        f"作业 {job_id} 的旧租约 token={old_token} 已吊销 "
                        f"（旧 epoch={old.epoch}/{old.gen_id}）",
                        job_id=job_id, old_token=old_token,
                        old_epoch=old.epoch, old_gen=old.gen_id,
                    )
            token = self._next_token
            self._next_token += 1
            lease = Lease(
                token=token, job_id=job_id, epoch=epoch,
                gen_id=gen_id, issued_at=time.time(),
            )
            self.leases[token] = lease
            self.current[job_id] = token
            self.history.setdefault(job_id, []).append(token)
            self._persist_locked()
            self.events.emit(
                "lease", "ok",
                f"作业 {job_id} 取得输出租约 token={token} "
                f"绑定 epoch={epoch} gen={gen_id}",
                job_id=job_id, token=token, epoch=epoch, gen_id=gen_id,
            )
            return lease

    def validate(self, job_id: str, token: int, epoch: int, gen_id: str) -> Lease:
        """栅栏校验：任一字段不符都拒绝迟到写入。"""

        with self._lock:
            lease = self.leases.get(token)
            if lease is None or lease.job_id != job_id:
                raise errors.LeaseInvalid("租约不存在", token=token)
            if lease.revoked or self.current.get(job_id) != token:
                raise errors.LeaseInvalid(
                    "租约已被恢复操作吊销，旧进程的迟到写入被拒绝",
                    token=token, current=self.current.get(job_id),
                )
            if lease.epoch != epoch:
                raise errors.LeaseEpochStale(
                    f"租约纪元 {lease.epoch} 与当前 {epoch} 不符",
                    token=token,
                )
            if lease.gen_id != gen_id:
                raise errors.LeaseGenerationStale(
                    f"租约代际 {lease.gen_id} 与当前 {gen_id} 不符",
                    token=token,
                )
            return lease

    def current_lease(self, job_id: str) -> Optional[Lease]:
        with self._lock:
            token = self.current.get(job_id)
            return self.leases.get(token) if token is not None else None

    def view(self) -> dict:
        with self._lock:
            return {
                "current": {
                    job: self.leases[t].to_dict()
                    for job, t in self.current.items()
                    if t in self.leases
                },
                "revoked_count": sum(1 for l in self.leases.values() if l.revoked),
            }


@dataclass
class OutputRecord:
    out_key: str
    digest: str
    size: int
    token: int
    epoch: int
    gen_id: str
    rank: int
    seq: int
    ts: float
    job_id: str = ""

    def to_dict(self) -> dict:
        return {
            "out_key": self.out_key,
            "digest": self.digest,
            "size": self.size,
            "token": self.token,
            "epoch": self.epoch,
            "gen_id": self.gen_id,
            "rank": self.rank,
            "seq": self.seq,
            "ts": self.ts,
            "job_id": self.job_id,
        }


class OutputSink:
    """内容寻址的输出汇：租约栅栏 + 键级写一次。"""

    # write 的判定结果
    ACCEPTED = "accepted"
    DEDUP = "dedup"          # 同键同内容：重放/重复，安全忽略
    CONFLICT = "conflict"    # 同键异内容：双重输出企图，拒绝

    def __init__(self, cas_root: str, meta_root: str,
                 leases: LeaseManager, events: EventLog) -> None:
        from .storage import CASStore

        self.cas = CASStore(os.path.join(cas_root, "outputs"))
        self.path = os.path.join(meta_root, "outputs.json")
        self.leases = leases
        self.events = events
        self._lock = threading.RLock()
        self.records: Dict[str, OutputRecord] = {}
        self.stats: Dict[str, dict] = {}
        # 每次拒绝都留痕（代码、键、新旧租约），供恢复验证报告引用
        self.rejections: List[dict] = []
        self._load()

    def _load(self) -> None:
        data = load_json(self.path, default=None)
        if not data:
            return
        self.records = {k: OutputRecord(**v) for k, v in data.get("records", {}).items()}
        self.stats = data.get("stats", {})
        self.rejections = data.get("rejections", [])

    def _persist_locked(self) -> None:
        # 拒绝日志只保留最近 500 条，避免无限增长
        self.rejections = self.rejections[-500:]
        atomic_write(self.path, canonical_json({
            "records": {k: r.to_dict() for k, r in self.records.items()},
            "stats": self.stats,
            "rejections": self.rejections,
        }))

    def _bump_locked(self, job_id: str, result: str) -> None:
        st = self.stats.setdefault(
            job_id,
            {"accepted": 0, "dedup": 0, "rejected": 0,
             "content_conflicts": 0, "fenced": 0},
        )
        if result == self.ACCEPTED:
            st["accepted"] += 1
        elif result == self.DEDUP:
            st["dedup"] += 1
        else:
            st["rejected"] += 1

    def _bump_fenced_locked(self, job_id: str) -> None:
        self._bump_locked(job_id, self.CONFLICT)
        self.stats[job_id]["fenced"] += 1

    def _bump_content_conflict_locked(self, job_id: str) -> None:
        self._bump_locked(job_id, self.CONFLICT)
        self.stats[job_id]["content_conflicts"] += 1

    def write(
        self,
        job_id: str,
        token: int,
        epoch: int,
        gen_id: str,
        out_key: str,
        payload: bytes,
        rank: int,
        seq: int,
    ) -> dict:
        """提交一次输出。返回 ``{result, ...}``，绝不产生双重输出。"""

        # 第一道防线：租约栅栏。
        try:
            lease = self.leases.validate(job_id, token, epoch, gen_id)
        except errors.LeaseInvalid as exc:
            with self._lock:
                self._bump_fenced_locked(job_id)
                self.rejections.append({
                    "ts": time.time(),
                    "job_id": job_id,
                    "stage": "lease_fence",
                    "code": exc.code,
                    "out_key": out_key,
                    "token": token,
                    "epoch": epoch,
                    "gen_id": gen_id,
                    "rank": rank,
                })
                self._persist_locked()
            self.events.emit(
                "output", "reject",
                f"租约栅栏拒绝迟到写入 {out_key}：{exc.code} "
                f"（token={token} epoch={epoch} gen={gen_id}）",
                job_id=job_id, out_key=out_key, code=exc.code, token=token,
            )
            raise

        with self._lock:
            digest = sha256_hex(payload)
            existing = self.records.get(out_key)
            if existing is not None:
                if existing.digest == digest:
                    self._bump_locked(job_id, self.DEDUP)
                    self._persist_locked()
                    self.events.emit(
                        "output", "info",
                        f"输出 {out_key} 相同内容重复提交，幂等去重（不产生第二份输出）",
                        job_id=job_id, out_key=out_key, token=token,
                    )
                    return {
                        "result": self.DEDUP,
                        "out_key": out_key,
                        "digest": digest,
                        "existing_token": existing.token,
                    }
                self._bump_content_conflict_locked(job_id)
                self.rejections.append({
                    "ts": time.time(),
                    "job_id": job_id,
                    "stage": "key_write_once",
                    "code": errors.DuplicateOutput.code,
                    "out_key": out_key,
                    "token": token,
                    "old_token": existing.token,
                    "rank": rank,
                })
                self._persist_locked()
                self.events.emit(
                    "output", "reject",
                    f"双重输出警报：{out_key} 被不同内容再次写入，已拒绝 "
                    f"（旧 token={existing.token} 新 token={token}）",
                    job_id=job_id, out_key=out_key,
                    old_digest=existing.digest[:12], new_digest=digest[:12],
                    old_token=existing.token, new_token=token,
                )
                raise errors.DuplicateOutput(
                    f"输出键 {out_key} 已存在不同内容",
                    out_key=out_key,
                    existing_digest=existing.digest,
                    incoming_digest=digest,
                )

            self.cas.put(payload, expected_digest=digest)
            rec = OutputRecord(
                out_key=out_key, digest=digest, size=len(payload),
                token=lease.token, epoch=epoch, gen_id=gen_id,
                rank=rank, seq=seq, ts=time.time(), job_id=job_id,
            )
            self.records[out_key] = rec
            self._bump_locked(job_id, self.ACCEPTED)
            self._persist_locked()
            return {
                "result": self.ACCEPTED,
                "out_key": out_key,
                "digest": digest,
                "token": lease.token,
            }

    def unique_outputs(self, job_id: str) -> List[dict]:
        with self._lock:
            return [
                r.to_dict()
                for r in self.records.values()
                if self._job_of(r) == job_id
            ]

    def rejections_for(self, job_id: str) -> List[dict]:
        with self._lock:
            return [dict(r) for r in self.rejections if r.get("job_id") == job_id]

    def stats_for(self, job_id: str) -> dict:
        with self._lock:
            base = {
                "accepted": 0, "dedup": 0, "rejected": 0,
                "content_conflicts": 0, "fenced": 0,
            }
            base.update(self.stats.get(job_id, {}))
            base["unique_keys"] = sum(
                1 for r in self.records.values()
                if self._job_of(r) == job_id
            )
            return base

    @staticmethod
    def _job_of(rec: OutputRecord) -> str:
        # 新记录带显式 job_id；兼容旧数据再从 gen_id/out_key 反解
        if rec.job_id:
            return rec.job_id
        if rec.gen_id and rec.gen_id != "origin":
            return rec.gen_id.rsplit("-ep", 1)[0]
        return rec.out_key.split("/", 1)[0]

    def view(self, job_id: str) -> dict:
        with self._lock:
            return {
                "stats": self.stats_for(job_id),
                "outputs": [
                    r.to_dict()
                    for r in sorted(self.records.values(), key=lambda r: r.seq)
                    if self._job_of(r) == job_id
                ],
            }
