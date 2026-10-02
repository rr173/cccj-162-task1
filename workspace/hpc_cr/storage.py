"""内容寻址存储 (CAS) 与输出提交日志。

ContentStore:
  put(data)  -> 若 sha256 已存在则直接复用 (重复检查点的天然去重),
                否则原子写入; 支持测试期注入损坏以验证提交期重校验。
  get(sha256) -> 重新从存储读出字节 (提交校验绝不信任客户端自报哈希)。
  verify(sha256) -> 重算哈希, 任何 bit-rot/损坏立即暴露。

OutputLog:
  输出条目以 (generation, output_id) 唯一。同代际同 idempotency key 的
  重复提交: 内容相同 -> 幂等返回已有条目; 内容不同 -> Conflict。
  代际不同的条目彼此隔离; 能否写入由 LeaseManager 的栅栏决定, OutputLog
  只负责"一旦写入, 永不重复落盘/交付"。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .errors import Conflict, NotFound, ValidationError
from .security import sha256_hex


class ContentStore:
    def __init__(self):
        self._blobs: dict[str, bytes] = {}
        self._lock = threading.Lock()

    def put(self, data: bytes) -> tuple[str, int, bool]:
        """存储数据, 返回 (sha256, 长度, 是否新写入)。"""
        if not isinstance(data, (bytes, bytearray)):
            raise ValidationError("blob payload must be bytes")
        digest = sha256_hex(data)
        with self._lock:
            existed = digest in self._blobs
            if not existed:
                self._blobs[digest] = bytes(data)
            return digest, len(data), not existed

    def get(self, digest: str) -> bytes:
        with self._lock:
            try:
                return self._blobs[digest]
            except KeyError as exc:
                raise NotFound(f"blob not found: {digest[:12]}") from exc

    def has(self, digest: str) -> bool:
        with self._lock:
            return digest in self._blobs

    def verify(self, digest: str) -> bool:
        """重算哈希, 损坏/缺失返回 False (调用方决定如何标记检查点)。"""
        with self._lock:
            data = self._blobs.get(digest)
        if data is None:
            return False
        return sha256_hex(data) == digest

    # -- 测试支持: 模拟存储 bit-rot / 节点磁盘损坏 --
    def corrupt(self, digest: str) -> None:
        with self._lock:
            if digest not in self._blobs:
                raise NotFound(f"blob not found: {digest[:12]}")
            self._blobs[digest] = self._blobs[digest] + b"\x00CORRUPT"

    def blob_count(self) -> int:
        with self._lock:
            return len(self._blobs)


@dataclass
class OutputRecord:
    generation: int
    output_id: str
    blob: str
    size: int
    rank: int | None
    ts: float

    def to_dict(self) -> dict:
        return {
            "generation": self.generation,
            "output_id": self.output_id,
            "blob": self.blob,
            "size": self.size,
            "rank": self.rank,
            "ts": self.ts,
        }


class OutputLog:
    """每作业一份的输出去重日志 (JobManager 内按 generation 隔离)。"""

    def __init__(self):
        self._lock = threading.Lock()
        # (generation, output_id) -> OutputRecord
        self._records: dict[tuple[int, str], OutputRecord] = {}

    def append(
        self,
        *,
        generation: int,
        output_id: str,
        blob: str,
        size: int,
        rank: int | None,
        ts: float,
    ) -> tuple[OutputRecord, bool]:
        """返回 (记录, 是否新写入)。重复但内容相同 -> 幂等; 冲突 -> 409。"""
        key = (generation, output_id)
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                if existing.blob != blob:
                    raise Conflict(
                        f"output_id {output_id!r} already committed in "
                        f"generation {generation} with different content"
                    )
                return existing, False
            rec = OutputRecord(
                generation=generation,
                output_id=output_id,
                blob=blob,
                size=size,
                rank=rank,
                ts=ts,
            )
            self._records[key] = rec
            return rec, True

    def list(self, generation: int | None = None) -> list[OutputRecord]:
        with self._lock:
            recs = list(self._records.values())
        if generation is not None:
            recs = [r for r in recs if r.generation == generation]
        recs.sort(key=lambda r: r.ts)
        return recs
