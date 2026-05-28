#pragma once

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <string>

#include "logmq/base/result.h"
#include "logmq/base/types.h"

namespace logmq {

enum class FlushPolicy {
    kSync,
    kAsync,
};

// Broker 身份和监听地址配置
struct BrokerConfig {
    BrokerId id{1};
    std::string host{"0.0.0.0"};
    std::uint16_t port{9092};
};

// 存储配置
struct StorageConfig {
    std::filesystem::path data_dir{"./data"};
    std::size_t segment_bytes{1024 * 1024 * 1024ULL};
    FlushPolicy flush_policy{FlushPolicy::kAsync};

    std::chrono::milliseconds flush_interval{1000};
};

// 网络程和连接数限制配置。
struct NetworkConfig {
    int io_threads{4};
    int worker_threads{8};
    std::size_t max_connections{10000};
    std::size_t read_buffer_bytes{64 * 1024};
};

// Consumer Group 相关超时配置
struct ConsumerConfig {
    std::chrono::milliseconds session_timeout{10000};

    std::chrono::milliseconds rebalance_timeout{30000};
};

// 由默认值和配置文件共同组成的进程级总配置
struct Config {
    // Broker 身份和监听地址。
    BrokerConfig broker;

    // CommitLog 和索引存储配置。
    StorageConfig storage;

    // TCP 服务和请求处理配置。
    NetworkConfig network;

    // Consumer Group 行为配置。
    ConsumerConfig consumer;

    // 启动前检查数值边界和必填字符串是否合法。
    [[nodiscard]] Status Validate() const;
};

// 获取刷盘策略name
[[nodiscard]] std::string_view FlushPolicyName(FlushPolicy policy);

// 解析 "sync" / "async" 为 FlushPolicy。
[[nodiscard]] Result<FlushPolicy> ParseFlushPolicy(std::string_view value);

}  // namespace logmq
