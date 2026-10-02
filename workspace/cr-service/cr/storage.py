"""持久化原语与内容寻址存储（CAS）。

* :func:`canonical_json` —— 确定性 JSON，用于计算摘要与磁盘持久化；
* :func:`atomic_write` —— 临时文件 + ``os.replace`` 的原子落盘；
* :class:`CASStore` —— blob 以 ``sha256`` 为键去重存储；损坏/重复
  内容在上传时即被哈希校验拒绝，检查点元数据只保存摘要引用。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Any


def canonical_json(obj: Any) -> bytes:
    """以确定性方式序列化（排序键、无多余空白），保证摘要可复现。"""

    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_write(path: str, data: bytes | str) -> None:
    """原子写入：先写同目录临时文件再 rename，崩溃不会留下半截状态。"""

    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    mode = "wb" if isinstance(data, (bytes, bytearray)) else "w"
    with open(tmp, mode) as fh:  # noqa: SIM117 - 显式 close 后再 replace
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


class CASStore:
    """内容寻址 blob 存储（线程安全，进程内单例）。

    磁盘布局::

        root/objects/ab/cdef...     # 以 sha256 为键的 blob
        root/tmp/...                # 上传中的临时分片
    """

    def __init__(self, root: str) -> None:
        self.root = root
        self.objects_dir = os.path.join(root, "objects")
        self.tmp_dir = os.path.join(root, "tmp")
        os.makedirs(self.objects_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self._lock = threading.Lock()

    @staticmethod
    def _relpath(digest: str) -> str:
        return os.path.join(digest[:2], digest[2:])

    def path_for(self, digest: str) -> str:
        return os.path.join(self.objects_dir, self._relpath(digest))

    def exists(self, digest: str) -> bool:
        return os.path.exists(self.path_for(digest))

    def put(self, data: bytes, expected_digest: str | None = None) -> str:
        """校验写入：内容哈希不符承诺则拒绝；相同内容重复写入是幂等的。"""

        digest = sha256_hex(data)
        if expected_digest is not None and digest != expected_digest:
            raise ValueError(f"哈希校验失败: 承诺 {expected_digest[:12]} 实际 {digest[:12]}")
        path = self.path_for(digest)
        with self._lock:
            if os.path.exists(path):
                # 路径以内容摘要命名：现存内容必然应为同摘要；若位腐烂
                # 导致现存字节与摘要不符，则用权威内容覆盖修复。
                with open(path, "rb") as fh:
                    if sha256_hex(fh.read()) == digest:
                        # 内容寻址的天然去重：重复分片/重复检查点只占一份存储。
                        return digest
            tmp = os.path.join(
                self.tmp_dir, f"blob-{os.getpid()}-{threading.get_ident()}"
            )
            with open(tmp, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.makedirs(os.path.dirname(path), exist_ok=True)
            os.replace(tmp, path)
        return digest

    def get(self, digest: str) -> bytes:
        with open(self.path_for(digest), "rb") as fh:
            return fh.read()

    def corrupt_for_test(self, digest: str, garbage: bytes = b"CORRUPTED-BYTES\n") -> None:
        """故障注入：把 blob 内容覆写成垃圾（模拟静默损坏/位腐烂）。

        恢复时的全量重新哈希会发现它，而不会把坏数据交给新进程。
        """

        path = self.path_for(digest)
        if not os.path.exists(path):
            raise FileNotFoundError(digest)
        atomic_write(path, garbage)
