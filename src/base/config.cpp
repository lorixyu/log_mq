#include "logmq/base/config.h"

namespace logmq {

Status Config::Validate() const {
    if (broker.id <= 0) {
        return Status::InvalidArgument("broker.id must be positive");
    }
    if (broker.host.empty()) {
        return Status::InvalidArgument("broker.host must not be empty");
    }
    if (broker.port == 0) {
        return Status::InvalidArgument("broker.port must be in 1..65535");
    }
    if (storage.data_dir.empty()) {
        return Status::InvalidArgument("storage.data_dir must not be empty");
    }
    if (storage.segment_bytes == 0) {
        return Status::InvalidArgument("storage.segment_bytes must be positive");
    }
    if (storage.flush_interval.count() < 0) {
        return Status::InvalidArgument("storage.flush_interval must not be negative");
    }
    if (network.io_threads <= 0) {
        return Status::InvalidArgument("network.io_threads must be positive");
    }
    if (network.worker_threads <= 0) {
        return Status::InvalidArgument("network.worker_threads must be positive");
    }
    if (network.max_connections == 0) {
        return Status::InvalidArgument("network.max_connections must be positive");
    }
    if (network.read_buffer_bytes == 0) {
        return Status::InvalidArgument("network.read_buffer_bytes must be positive");
    }
    if (consumer.session_timeout.count() <= 0) {
        return Status::InvalidArgument("consumer.session_timeout must be positive");
    }
    if (consumer.rebalance_timeout.count() <= 0) {
        return Status::InvalidArgument("consumer.rebalance_timeout must be positive");
    }
    return Status::Ok();
}

std::string_view FlushPolicyName(FlushPolicy policy) {
    switch (policy) {
        case FlushPolicy::kSync:
            return "sync";
        case FlushPolicy::kAsync:
            return "async";
    }
    return "unknown";
}

Result<FlushPolicy> ParseFlushPolicy(std::string_view value) {
    if (value == "sync") {
        return FlushPolicy::kSync;
    }
    if (value == "async") {
        return FlushPolicy::kAsync;
    }
    return Status::InvalidArgument("flush_policy must be sync or async");
}

}  // namespace logmq
