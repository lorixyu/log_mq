#pragma once

#include <chrono>
#include <cstdint>
#include <string>
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

    [[nodiscard]] Result<void> Subscribe(TopicName topic);
    [[nodiscard]] Result<std::vector<ConsumerRecord>> Poll(
        std::chrono::milliseconds timeout);
    [[nodiscard]] Result<void> Seek(PartitionId partition_id, Offset offset);
    [[nodiscard]] Result<void> CommitSync();

private:
    struct Assignment {
        PartitionId partition_id{kInvalidPartitionId};
        Offset next_offset{0};
    };

    [[nodiscard]] Result<FetchCommittedOffsetResponse> FetchCommittedOffset(
        PartitionId partition_id) const;

    ConsumerOptions options_;
    ClientConnection connection_;
    TopicName topic_;
    std::vector<Assignment> assignments_;
};

}  // namespace logmq
