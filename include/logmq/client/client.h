#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "logmq/base/result.h"
#include "logmq/base/types.h"
#include "logmq/protocol/types.h"

namespace logmq {

struct ClientOptions {
    std::string host{"127.0.0.1"};
    std::uint16_t port{9092};
    std::chrono::milliseconds request_timeout{5000};
};

class ClientConnection {
public:
    explicit ClientConnection(ClientOptions options);

    [[nodiscard]] Result<ResponseEnvelope> RoundTrip(const RequestEnvelope& request) const;

private:
    ClientOptions options_;
};

class AdminClient {
public:
    explicit AdminClient(ClientOptions options);

    [[nodiscard]] Result<CreateTopicResponse> CreateTopic(TopicName topic,
                                                          std::uint32_t partition_count) const;
    [[nodiscard]] Result<MetadataResponse> Metadata(TopicName topic = "") const;

private:
    ClientConnection connection_;
};

struct ProducerOptions {
    ClientOptions client;
    std::uint32_t max_retries{1};
};

class Producer {
public:
    explicit Producer(ProducerOptions options);

    [[nodiscard]] Result<ProduceResponse> Send(TopicName topic,
                                               std::string key,
                                               std::string value,
                                               PartitionId partition_id = kInvalidPartitionId) const;
    [[nodiscard]] Result<ProduceResponse> SendBatch(
        TopicName topic,
        std::vector<ProtocolRecord> records,
        PartitionId partition_id = kInvalidPartitionId) const;

private:
    ProducerOptions options_;
    ClientConnection connection_;
};

struct ConsumerOptions {
    ClientOptions client;
    std::string group_id;
    std::uint32_t max_bytes{4096};
    std::chrono::milliseconds heartbeat_interval{3000};
};

struct ConsumerRecord {
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    Offset offset{kInvalidOffset};
    std::uint64_t timestamp{0};
    std::string key;
    std::string value;
};

class Consumer {
public:
    explicit Consumer(ConsumerOptions options);
    ~Consumer();

    [[nodiscard]] Result<void> Subscribe(TopicName topic);
    [[nodiscard]] Result<std::vector<ConsumerRecord>> Poll(
        std::chrono::milliseconds timeout);
    [[nodiscard]] Result<void> Seek(PartitionId partition_id, Offset offset);
    [[nodiscard]] Result<void> CommitSync();
    [[nodiscard]] Result<void> Close();
    [[nodiscard]] std::vector<PartitionId> AssignedPartitions() const;

private:
    struct Assignment {
        PartitionId partition_id{kInvalidPartitionId};
        Offset next_offset{0};
    };

    [[nodiscard]] Result<void> JoinAndSync();
    [[nodiscard]] Result<FetchCommittedOffsetResponse> FetchCommittedOffset(
        const TopicName& topic, PartitionId partition_id) const;
    [[nodiscard]] Result<JoinGroupResponse> SendJoinGroup(const TopicName& topic,
                                                          const std::string& member_id) const;
    [[nodiscard]] Result<SyncGroupResponse> SendSyncGroup(
        const JoinGroupResponse& joined) const;
    [[nodiscard]] Result<void> SendLeaveGroup(const std::string& member_id) const;
    void StartHeartbeat();
    void RunHeartbeat();

    ConsumerOptions options_;
    ClientConnection connection_;
    mutable std::mutex mutex_;
    std::condition_variable heartbeat_condition_;
    std::thread heartbeat_thread_;
    TopicName topic_;
    std::string member_id_;
    std::int32_t generation_id_{0};
    std::vector<Assignment> assignments_;
    bool subscribed_{false};
    bool closed_{false};
    bool heartbeat_stop_{false};
    bool needs_rejoin_{false};
};

}  // namespace logmq
