"""领域错误定义。

每个错误都带有稳定的 ``code``，HTTP 层据此映射状态码，
节点页面与报告据此判断拒绝原因（例如区分「旧纪元迟到写入」与
「不完整检查点」），而不需要解析人类可读消息。
"""

from __future__ import annotations


class CRError(Exception):
    """所有领域错误的基类。"""

    code = "cr_error"
    http_status = 400

    def __init__(self, message: str = "", **details: object) -> None:
        super().__init__(message or self.__doc__ or self.code)
        self.details = details

    def to_dict(self) -> dict:
        return {"error": self.code, "message": str(self), "details": self.details}


class JobNotFound(CRError):
    """作业不存在。"""

    code = "job_not_found"
    http_status = 404


class GenerationNotFound(CRError):
    """检查点代际不存在。"""

    code = "generation_not_found"
    http_status = 404


class EpochStale(CRError):
    """纪元过期：迟到的抢占请求或检查点上传来自旧纪元，必须拒绝。"""

    code = "epoch_stale"
    http_status = 409


class CheckpointConflict(CRError):
    """同名检查点内容与既有承诺不一致（重复检查点携带不同内容）。"""

    code = "checkpoint_conflict"
    http_status = 409


class ShardHashMismatch(CRError):
    """分片内容哈希与登记承诺不符，拒绝入库并标记校验失败。"""

    code = "shard_hash_mismatch"
    http_status = 422


class CheckpointIncomplete(CRError):
    """检查点不完整：仍有分片或进程记录缺失，不可封印、不可恢复。"""

    code = "checkpoint_incomplete"
    http_status = 409


class GenerationNotSealed(CRError):
    """目标代际未封印（OPEN/ABORTED），不允许据此恢复。"""

    code = "generation_not_sealed"
    http_status = 409


class GenerationAborted(CRError):
    """抢占纪元已中止，代际被废弃；旧进程的迟到上传必须拒绝。"""

    code = "generation_aborted"
    http_status = 409


class LeaseInvalid(CRError):
    """输出租约无效（租约号过期或已被恢复操作吊销）。"""

    code = "lease_invalid"
    http_status = 403


class LeaseEpochStale(LeaseInvalid):
    """租约绑定旧纪元：旧进程在抢占/恢复之后的迟到写入。"""

    code = "lease_epoch_stale"
    http_status = 403


class LeaseGenerationStale(LeaseInvalid):
    """租约绑定旧代际：旧进程在新代际运行后的迟到写入。"""

    code = "lease_generation_stale"
    http_status = 403


class DuplicateOutput(CRError):
    """同键输出已存在且内容不同：检测到双重输出企图，拒绝写入。"""

    code = "duplicate_output"
    http_status = 409


class NoActivePreemption(CRError):
    """当前没有进行中的抢占纪元。"""

    code = "no_active_preemption"
    http_status = 409


class PreemptionConflict(CRError):
    """抢占状态机冲突（例如重复确认释放、对已中止纪元操作）。"""

    code = "preemption_conflict"
    http_status = 409


class NodeNotFound(CRError):
    """节点不存在。"""

    code = "node_not_found"
    http_status = 404


class ShardNotFound(CRError):
    """分片承诺不存在（上传前需先登记分片规格）。"""

    code = "shard_not_found"
    http_status = 404
