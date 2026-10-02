"""服务端错误类型。

每个领域错误携带 HTTP 状态码, 供 API 层统一映射为 JSON 错误响应。
"""


class CpError(Exception):
    """所有领域错误的基类。"""

    http_status = 500
    error_code = "internal_error"

    def to_dict(self):
        return {"error": self.error_code, "message": str(self)}


class NotFound(CpError):
    http_status = 404
    error_code = "not_found"


class Conflict(CpError):
    """状态冲突: 重复检查点 / 重复分片 / 状态机不允许的迁移。"""

    http_status = 409
    error_code = "conflict"


class FenceViolation(CpError):
    """栅栏拒绝: 旧纪元/旧代际令牌的迟到写入。"""

    http_status = 412
    error_code = "fence_violation"


class InvalidToken(CpError):
    http_status = 401
    error_code = "invalid_token"


class ValidationError(CpError):
    http_status = 400
    error_code = "validation_error"
