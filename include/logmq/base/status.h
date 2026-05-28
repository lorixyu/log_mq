#pragma once

#include <string>
#include <string_view>

namespace logmq {

enum class ErrorCode {
    kOk = 0,           // ok
    kInvalidArgument,  // 无效参数
    kNotFound,         // 未找到
    kIoError,          // IO 调用失败
    kCorruption,       // 数据损坏、校验失败
    kTimeout,          // 超时
    kNotLeader,        // 请求发送到的目标是非 Leader _ 预留分布式字段
    kInternal,         // xx内部错误
};

// Topic 不存在、offset 非法、IO 失败等可恢复错误。
// 状态获取类
class Status {
public:
    Status() = default;

    Status(ErrorCode code, std::string message);

    // 返回 ErrorCode & message
    static Status Ok();
    static Status InvalidArgument(std::string message);
    static Status NotFound(std::string message);
    static Status IoError(std::string message);
    static Status Corruption(std::string message);
    static Status Timeout(std::string message);
    static Status NotLeader(std::string message);
    static Status Internal(std::string message);

    [[nodiscard]] bool ok() const;

    [[nodiscard]] ErrorCode code() const;

    [[nodiscard]] std::string_view message() const;

    [[nodiscard]] std::string ToString() const;

private:
    ErrorCode code_{ErrorCode::kOk};

    // 调试文本
    std::string message_;
};

[[nodiscard]] std::string_view ErrorCodeName(ErrorCode code);

}  // namespace logmq
