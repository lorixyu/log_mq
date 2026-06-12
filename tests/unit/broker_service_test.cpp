#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "logmq/broker/broker_service.h"

namespace logmq {
namespace {

class TempDir {
public:
    TempDir() {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() / ("logmq_broker_" + std::to_string(now));
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

RequestEnvelope MakeCreateTopic(std::uint64_t request_id, std::string topic,
                                std::uint32_t partitions) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kCreateTopic;
    envelope.request_id = request_id;
    envelope.body = CreateTopicRequest{std::move(topic), partitions};
    return envelope;
}

RequestEnvelope MakeProduce(std::uint64_t request_id, std::string topic,
                            PartitionId partition, std::string key, std::string value) {
    ProduceRequest request;
    request.topic = std::move(topic);
    request.partition_id = partition;
    request.records.push_back(ProtocolRecord{1'700'000'000 + request_id, std::move(key),
                                             std::move(value)});

    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kProduce;
    envelope.request_id = request_id;
    envelope.body = std::move(request);
    return envelope;
}

RequestEnvelope MakeFetch(std::uint64_t request_id, std::string topic,
                          PartitionId partition, Offset offset) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kFetch;
    envelope.request_id = request_id;
    envelope.body = FetchRequest{std::move(topic), partition, offset, 4096};
    return envelope;
}

RequestEnvelope MakeMetadata(std::uint64_t request_id, std::string topic) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kMetadata;
    envelope.request_id = request_id;
    envelope.body = MetadataRequest{std::move(topic)};
    return envelope;
}

BrokerService MakeService(const TempDir& temp_dir) {
    BrokerServiceOptions options;
    options.storage.data_dir = temp_dir.path();
    options.storage.segment_bytes = 1024 * 1024;
    options.storage.flush_policy = FlushPolicy::kAsync;
    return BrokerService(std::move(options));
}

TEST(BrokerServiceTest, CreateTopicProduceAndFetch) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);

    auto create = service.Handle(MakeCreateTopic(1, "orders", 2));
    ASSERT_EQ(create.error.error_code, ProtocolErrorCode::kNone) << create.error.message;
    ASSERT_TRUE(std::holds_alternative<CreateTopicResponse>(create.body));
    EXPECT_EQ(std::get<CreateTopicResponse>(create.body).partition_count, 2U);

    auto produce = service.Handle(MakeProduce(2, "orders", 1, "k1", "v1"));
    ASSERT_EQ(produce.error.error_code, ProtocolErrorCode::kNone) << produce.error.message;
    ASSERT_TRUE(std::holds_alternative<ProduceResponse>(produce.body));
    EXPECT_EQ(std::get<ProduceResponse>(produce.body).base_offset, 0);

    auto fetch = service.Handle(MakeFetch(3, "orders", 1, 0));
    ASSERT_EQ(fetch.error.error_code, ProtocolErrorCode::kNone) << fetch.error.message;
    ASSERT_TRUE(std::holds_alternative<FetchResponse>(fetch.body));
    const auto& body = std::get<FetchResponse>(fetch.body);
    EXPECT_EQ(body.base_offset, 0);
    EXPECT_EQ(body.high_watermark, 1);
    ASSERT_EQ(body.records.size(), 1U);
    EXPECT_EQ(body.records[0].key, "k1");
    EXPECT_EQ(body.records[0].value, "v1");

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, MetadataSupportsSingleTopicAndAllTopics) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);

    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 3)).error.error_code,
              ProtocolErrorCode::kNone);
    ASSERT_EQ(service.Handle(MakeCreateTopic(2, "payments", 1)).error.error_code,
              ProtocolErrorCode::kNone);

    auto single = service.Handle(MakeMetadata(3, "orders"));
    ASSERT_EQ(single.error.error_code, ProtocolErrorCode::kNone) << single.error.message;
    const auto& single_body = std::get<MetadataResponse>(single.body);
    ASSERT_EQ(single_body.topics.size(), 1U);
    EXPECT_EQ(single_body.topics[0].topic, "orders");
    EXPECT_EQ(single_body.topics[0].partition_count, 3U);

    auto all = service.Handle(MakeMetadata(4, ""));
    ASSERT_EQ(all.error.error_code, ProtocolErrorCode::kNone) << all.error.message;
    const auto& all_body = std::get<MetadataResponse>(all.body);
    ASSERT_EQ(all_body.topics.size(), 2U);
    EXPECT_EQ(all_body.topics[0].topic, "orders");
    EXPECT_EQ(all_body.topics[1].topic, "payments");

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, RejectsInvalidRequests) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);

    EXPECT_EQ(service.Handle(MakeProduce(1, "missing", 0, "k", "v")).error.error_code,
              ProtocolErrorCode::kTopicNotFound);

    ASSERT_EQ(service.Handle(MakeCreateTopic(2, "orders", 1)).error.error_code,
              ProtocolErrorCode::kNone);

    EXPECT_EQ(service.Handle(MakeCreateTopic(3, "orders", 2)).error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeProduce(4, "orders", 4, "k", "v")).error.error_code,
              ProtocolErrorCode::kTopicNotFound);
    EXPECT_EQ(service.Handle(MakeFetch(5, "orders", 0, 10)).error.error_code,
              ProtocolErrorCode::kOffsetOutOfRange);

    ProduceRequest empty;
    empty.topic = "orders";
    empty.partition_id = 0;
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kProduce;
    envelope.request_id = 6;
    envelope.body = std::move(empty);
    EXPECT_EQ(service.Handle(envelope).error.error_code, ProtocolErrorCode::kInvalidRequest);

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, ConcurrentProduceAssignsUniqueOffsets) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 1)).error.error_code,
              ProtocolErrorCode::kNone);

    constexpr int kThreads = 8;
    constexpr int kPerThread = 64;
    std::mutex mutex;
    std::vector<Offset> offsets;
    offsets.reserve(kThreads * kPerThread);

    std::vector<std::thread> threads;
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            for (int i = 0; i < kPerThread; ++i) {
                auto response = service.Handle(MakeProduce(
                    static_cast<std::uint64_t>(1000 + thread * kPerThread + i), "orders", 0,
                    "k", "v"));
                ASSERT_EQ(response.error.error_code, ProtocolErrorCode::kNone)
                    << response.error.message;
                Offset offset = std::get<ProduceResponse>(response.body).base_offset;
                std::lock_guard lock(mutex);
                offsets.push_back(offset);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(offsets.size(), static_cast<std::size_t>(kThreads * kPerThread));
    std::sort(offsets.begin(), offsets.end());
    for (Offset offset = 0; offset < static_cast<Offset>(offsets.size()); ++offset) {
        EXPECT_EQ(offsets[static_cast<std::size_t>(offset)], offset);
    }

    ASSERT_TRUE(service.Close().ok());
}

}  // namespace
}  // namespace logmq
