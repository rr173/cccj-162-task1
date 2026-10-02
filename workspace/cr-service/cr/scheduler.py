"""调度器与节点监控。

调度器负责抢占状态机，并强制执行最重要的资源规则：

    只有检查点代际 SEALED（全量校验通过）之后，才允许 ``confirm_release``
    回收节点；抢占失败则中止纪元、保留节点继续运行。

抢占记录::

    PENDING ──(代际封印 + 确认)──▶ RELEASED   # 节点被回收
       │
       └────────(检查点不完整/超时/失联)──────▶ ABORTED    # 节点保留
"""

from __future__ import annotations

import os
import threading
import time
from typing import Dict, List, Optional

from . import errors
from .coordinator import SEALED, CheckpointCoordinator
from .events import EventLog
from .storage import atomic_write, canonical_json, load_json

PENDING = "PENDING"
RELEASED = "RELEASED"
ABORTED = "ABORTED"


class NodeMonitor:
    """节点名册与健康状态（失联/恢复）。"""

    def __init__(self, root: str, events: EventLog) -> None:
        self.path = os.path.join(root, "nodes.json")
        self.events = events
        self._lock = threading.RLock()
        self.nodes: Dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        data = load_json(self.path, default=None)
        if data:
            self.nodes = data.get("nodes", {})

    def _persist_locked(self) -> None:
        atomic_write(self.path, canonical_json({"nodes": self.nodes}))

    def register(self, node_id: str, capacity: int = 8) -> dict:
        with self._lock:
            if node_id not in self.nodes:
                self.nodes[node_id] = {
                    "node_id": node_id,
                    "capacity": capacity,
                    "healthy": True,
                    "allocated_to": None,
                    "ranks": [],
                    "lost_at": None,
                }
                self._persist_locked()
            return dict(self.nodes[node_id])

    def allocate(self, node_id: str, job_id: str, ranks: List[int]) -> None:
        with self._lock:
            node = self.nodes[node_id]
            node["allocated_to"] = job_id
            node["ranks"] = list(ranks)
            self._persist_locked()

    def release_by_job(self, job_id: str) -> List[str]:
        with self._lock:
            freed = []
            for node in self.nodes.values():
                if node["allocated_to"] == job_id:
                    node["allocated_to"] = None
                    node["ranks"] = []
                    freed.append(node["node_id"])
            self._persist_locked()
            return freed

    def mark_lost(self, node_id: str, reason: str = "unreachable") -> dict:
        with self._lock:
            node = self.nodes[node_id]
            node["healthy"] = False
            node["lost_at"] = time.time()
            self._persist_locked()
            self.events.emit(
                "nodes", "warn", f"节点 {node_id} 失联（{reason}）",
                node=node_id, job_id=node.get("allocated_to"),
            )
            return dict(node)

    def recover_node(self, node_id: str) -> dict:
        with self._lock:
            node = self.nodes[node_id]
            node["healthy"] = True
            node["lost_at"] = None
            self._persist_locked()
            self.events.emit("nodes", "info", f"节点 {node_id} 恢复在线", node=node_id)
            return dict(node)

    def healthy_nodes_for(self, job_id: str) -> List[str]:
        with self._lock:
            return [
                nid
                for nid, n in self.nodes.items()
                if n["allocated_to"] == job_id and n["healthy"]
            ]

    def lost_nodes_for(self, job_id: str) -> List[str]:
        with self._lock:
            return [
                nid
                for nid, n in self.nodes.items()
                if n["allocated_to"] == job_id and not n["healthy"]
            ]

    def view(self) -> dict:
        with self._lock:
            return {"nodes": [dict(n) for n in sorted(self.nodes.values(),
                                                      key=lambda x: x["node_id"])]}


class Scheduler:
    def __init__(
        self,
        root: str,
        coordinator: CheckpointCoordinator,
        nodes: NodeMonitor,
        events: EventLog,
    ) -> None:
        self.path = os.path.join(root, "scheduler.json")
        self.coordinator = coordinator
        self.nodes = nodes
        self.events = events
        self._lock = threading.RLock()
        self.preemptions: Dict[str, dict] = {}  # job_id -> 当前抢占记录
        self.history: List[dict] = []
        self._load()

    def _load(self) -> None:
        data = load_json(self.path, default=None)
        if data:
            self.preemptions = data.get("preemptions", {})
            self.history = data.get("history", [])

    def _persist_locked(self) -> None:
        atomic_write(self.path, canonical_json({
            "preemptions": self.preemptions,
            "history": self.history,
        }))

    # ------------------------------------------------------------------

    def issue_preemption(
        self,
        job_id: str,
        process_specs: List[dict],
        shard_specs: List[dict],
        deadline: Optional[float] = None,
    ) -> dict:
        """回收节点前签发抢占纪元（同时开启检查点代际）。"""

        with self._lock:
            active = self.preemptions.get(job_id)
            if active and active["status"] == PENDING:
                raise errors.PreemptionConflict(
                    "已有进行中的抢占纪元",
                    epoch=active["epoch"], gen_id=active["gen_id"],
                )
            gen_view = self.coordinator.begin_preemption(
                job_id, process_specs, shard_specs
            )
            record = {
                "job_id": job_id,
                "epoch": gen_view["epoch"],
                "gen_id": gen_view["gen_id"],
                "status": PENDING,
                "deadline": deadline,
                "issued_at": time.time(),
                "completed_at": None,
                "reason": None,
            }
            self.preemptions[job_id] = record
            self._persist_locked()
            self.events.emit(
                "scheduler", "info",
                f"调度器准备回收 {job_id} 的节点，等待检查点确认释放",
                job_id=job_id, epoch=record["epoch"], gen_id=record["gen_id"],
            )
            return dict(record)

    def active_preemption(self, job_id: str) -> Optional[dict]:
        with self._lock:
            rec = self.preemptions.get(job_id)
            return dict(rec) if rec else None

    def _require_pending_locked(self, job_id: str) -> dict:
        rec = self.preemptions.get(job_id)
        if rec is None or rec["status"] != PENDING:
            raise errors.NoActivePreemption(job_id=job_id)
        return rec

    def confirm_release(self, job_id: str) -> dict:
        """检查点 SEALED 后确认释放节点。封印检查在此强制执行。"""

        with self._lock:
            rec = self._require_pending_locked(job_id)
            gen = self.coordinator.get_generation(rec["gen_id"])
            if gen["status"] != SEALED:
                raise errors.GenerationNotSealed(
                    "检查点尚未确认可用，拒绝释放节点",
                    gen_id=rec["gen_id"], status=gen["status"],
                )
            rec["status"] = RELEASED
            rec["completed_at"] = time.time()
            freed = self.nodes.release_by_job(job_id)
            self.history.append(dict(rec))
            self._persist_locked()
            self.events.emit(
                "scheduler", "ok",
                f"检查点确认完整，节点 {freed} 已回收；作业 {job_id} 可从 "
                f"代际 {rec['gen_id']} 恢复",
                job_id=job_id, gen_id=rec["gen_id"], freed_nodes=freed,
            )
            out = dict(rec)
            out["freed_nodes"] = freed
            return out

    def abort_preemption(self, job_id: str, reason: str) -> dict:
        with self._lock:
            rec = self._require_pending_locked(job_id)
            self.coordinator.abort(rec["gen_id"], reason)
            rec["status"] = ABORTED
            rec["completed_at"] = time.time()
            rec["reason"] = reason
            self.history.append(dict(rec))
            self._persist_locked()
            self.events.emit(
                "scheduler", "warn",
                f"抢占纪元 {rec['epoch']} 中止，保留节点继续运行：{reason}",
                job_id=job_id, reason=reason,
            )
            return dict(rec)

    def history_for(self, job_id: str) -> List[dict]:
        with self._lock:
            return [dict(r) for r in self.history if r["job_id"] == job_id]
