#include "logmq/base/status.h"

#include <string>

namespace logmq {

Status::Status(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

Status Status::Ok() { return {}; }

Status Status::InvalidArgument(std::string message) {
    return {ErrorCode::kInvalidArgument, std::move(message)};
}

Status Status::NotFound(std::string message) { return {ErrorCode::kNotFound, std::move(message)}; }

Status Status::IoError(std::string message) { return {ErrorCode::kIoError, std::move(message)}; }

Status Status::Corruption(std::string message) {
    return {ErrorCode::kCorruption, std::move(message)};
}

Status Status::Timeout(std::string message) { return {ErrorCode::kTimeout, std::move(message)}; }

Status Status::NotLeader(std::string message) {
    return {ErrorCode::kNotLeader, std::move(message)};
}

Status Status::Internal(std::string message) { return {ErrorCode::kInternal, std::move(message)}; }

bool Status::ok() const { return code_ == ErrorCode::kOk; }

ErrorCode Status::code() const { return code_; }

std::string_view Status::message() const { return message_; }

std::string Status::ToString() const {
    if (ok()) {
        return "OK";
    }

    std::string result(ErrorCodeName(code_));
    if (!message_.empty()) {
        result += ": ";
        result += message_;
    }
    return result;
}

std::string_view ErrorCodeName(ErrorCode code) {
    switch (code) {
        case ErrorCode::kOk:
            return "OK";
        case ErrorCode::kInvalidArgument:
            return "InvalidArgument";
        case ErrorCode::kNotFound:
            return "NotFound";
        case ErrorCode::kIoError:
            return "IoError";
        case ErrorCode::kCorruption:
            return "Corruption";
        case ErrorCode::kTimeout:
            return "Timeout";
        case ErrorCode::kNotLeader:
            return "NotLeader";
        case ErrorCode::kInternal:
            return "Internal";
    }
    return "Unknown";
}

}  // namespace logmq
