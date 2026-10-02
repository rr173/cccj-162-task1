"""集群级事件日志（环形、线程安全）。

所有子系统（调度器、协调器、租约、输出端、作业运行时）把关键
状态迁移写入同一条时间线，节点进度页面据此渲染，恢复验证报告
也引用其中的拒绝/确认事件作为证据。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List


class EventLog:
    def __init__(self, capacity: int = 2000) -> None:
        self._events: Deque[Dict[str, Any]] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._seq = 0

    def emit(self, subsystem: str, kind: str, message: str, **fields: Any) -> Dict[str, Any]:
        with self._lock:
            self._seq += 1
            event = {
                "seq": self._seq,
                "ts": time.time(),
                "subsystem": subsystem,
                "kind": kind,  # info / ok / warn / reject / error
                "message": message,
                "fields": fields,
            }
            self._events.append(event)
            return event

    def tail(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._events)[-limit:]

    def since(self, seq: int) -> List[Dict[str, Any]]:
        with self._lock:
            return [e for e in self._events if e["seq"] > seq]
