"""输出租约 (output lease) 与代际栅栏。

每个运行代际至多一份 ACTIVE 输出租约。作业被抢占时租约立即 REVOKED
(原子切换 job epoch), 旧进程即使本地未及时收到信号, 其迟到写入携带的旧
租约令牌也会被 authorize() 拒绝 (FenceViolation)。恢复后签发新代际
租约, 输出从头具备写入资格。

租约令牌由 Scheduler 的 HMAC 签发, 绑定 job/generation/lease/until。
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field

from .epochs import Scheduler
from .errors import FenceViolation, InvalidToken, NotFound

ACTIVE = "ACTIVE"
REVOKED = "REVOKED"
EXPIRED = "EXPIRED"


@dataclass
class Lease:
    lease_id: str
    job_id: str
    generation: int
    issued_ts: float
    expires_ts: float
    state: str = ACTIVE
    revoked_ts: float | None = None
    token: str = ""

    def to_dict(self) -> dict:
        return {
            "lease_id": self.lease_id,
            "generation": self.generation,
            "state": self.state,
            "issued_ts": self.issued_ts,
            "expires_ts": self.expires_ts,
            "revoked_ts": self.revoked_ts,
        }


class LeaseManager:
    def __init__(self, job_id: str, scheduler: Scheduler):
        self.job_id = job_id
        self._sched = scheduler
        self._lock = threading.RLock()
        self._leases: dict[int, Lease] = {}  # generation -> current lease
        self._by_id: dict[str, Lease] = {}

    def grant(self, generation: int, ttl_seconds: float = 300.0) -> Lease:
        """为指定代际签发租约; 同代际重复获取幂等返回现有 ACTIVE 租约。"""
        now = time.time()
        with self._lock:
            existing = self._leases.get(generation)
            if existing is not None:
                if existing.state == ACTIVE and existing.expires_ts > now:
                    return existing
                # 过期则续签新 lease_id, 旧的保持 EXPIRED 仍可被识别
            lease = Lease(
                lease_id=uuid.uuid4().hex,
                job_id=self.job_id,
                generation=generation,
                issued_ts=now,
                expires_ts=now + ttl_seconds,
            )
            lease.token = self._sched.issue_lease_token(
                job_id=self.job_id,
                generation=generation,
                lease_id=lease.lease_id,
                until_ts=lease.expires_ts,
            )
            self._leases[generation] = lease
            self._by_id[lease.lease_id] = lease
            return lease

    def revoke_generation(self, generation: int) -> Lease | None:
        """抢占时吊销某代际的输出租约。"""
        with self._lock:
            lease = self._leases.get(generation)
            if lease is not None and lease.state == ACTIVE:
                lease.state = REVOKED
                lease.revoked_ts = time.time()
            return lease

    def current(self) -> Lease | None:
        with self._lock:
            for g in sorted(self._leases, reverse=True):
                return self._leases[g]
        return None

    def authorize(
        self, token: str, *, expected_generation: int, now: float | None = None
    ) -> Lease:
        """校验写入令牌: 签名合法 + 绑定本作业 + 代际匹配 + 租约仍 ACTIVE。

        旧进程迟到写入时 expected_generation 已是新代际 (或旧代际租约已
        REVOKED), 一律抛 FenceViolation, 从根本上阻止双重输出。
        """
        cur = now or time.time()
        payload = self._sched.verify_lease_token(token)
        if payload["job"] != self.job_id:
            raise FenceViolation("lease token belongs to another job")
        if payload["generation"] != expected_generation:
            raise FenceViolation(
                f"stale output write: token generation "
                f"{payload['generation']} != current {expected_generation}"
            )
        with self._lock:
            lease = self._by_id.get(payload["lease"])
            if lease is None:
                raise InvalidToken("unknown lease id")
            if lease.generation != expected_generation:
                raise FenceViolation("lease generation mismatch")
            if lease.state == REVOKED:
                raise FenceViolation(
                    f"lease {lease.lease_id} was revoked at preemption; "
                    "late write from old process rejected"
                )
            if lease.state == EXPIRED or lease.expires_ts <= cur:
                lease.state = EXPIRED
                raise FenceViolation("output lease expired")
            if cur > payload["until_ts"]:
                raise FenceViolation("token past its signed expiry")
            return lease

    def get(self, generation: int) -> Lease:
        with self._lock:
            lease = self._leases.get(generation)
            if lease is None:
                raise NotFound(f"no lease for generation {generation}")
            return lease
