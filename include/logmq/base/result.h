#pragma once

#include <utility>
#include <variant>

#include "logmq/base/status.h"

namespace logmq {

template <typename T>
class Result {
public:
    Result(const T& value) : storage_(value) {}
    Result(T&& value) : storage_(std::move(value)) {}

    Result(const Status& status) : storage_(status) {}
    Result(Status&& status) : storage_(std::move(status)) {}

    // check T
    [[nodiscard]] bool ok() const { return std::holds_alternative<T>(storage_); }

    // 获取状态
    [[nodiscard]] const Status& status() const {
        if (ok()) {  // storage_ has <T> type
            static const Status ok_status = Status::Ok();
            return ok_status;
        }
        return std::get<Status>(storage_);
    }

    [[nodiscard]] const T& value() const& { return std::get<T>(storage_); }

    [[nodiscard]] T& value() & { return std::get<T>(storage_); }

    [[nodiscard]] T&& value() && { return std::move(std::get<T>(storage_)); }

private:
    std::variant<T, Status> storage_;
};

template <>
class Result<void> {
public:
    Result() = default;

    Result(const Status& status) : status_(status) {}
    Result(Status&& status) : status_(std::move(status)) {}

    [[nodiscard]] bool ok() const { return status_.ok(); }

    [[nodiscard]] const Status& status() const { return status_; }

private:
    Status status_;
};

}  // namespace logmq
