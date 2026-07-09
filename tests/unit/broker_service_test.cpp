#include <gtest/gtest.h>

#include <algorithm>
#include <array>
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

RequestEnvelope MakeCommitOffset(std::uint64_t request_id,
                                 std::string group_id,
                                 std::string topic,
                                 PartitionId partition,
                                 Offset offset) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kCommitOffset;
    envelope.request_id = request_id;
    envelope.body =
        CommitOffsetRequest{std::move(group_id), std::move(topic), partition, offset};
    return envelope;
}

RequestEnvelope MakeFetchCommittedOffset(std::uint64_t request_id,
                                         std::string group_id,
                                         std::string topic,
                                         PartitionId partition) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kFetchCommittedOffset;
    envelope.request_id = request_id;
    envelope.body =
        FetchCommittedOffsetRequest{std::move(group_id), std::move(topic), partition};
    return envelope;
}

RequestEnvelope MakeJoinGroup(std::uint64_t request_id,
                              std::string group_id,
                              std::string member_id,
                              std::string topic) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kJoinGroup;
    envelope.request_id = request_id;
    envelope.body =
        JoinGroupRequest{std::move(group_id), std::move(member_id), std::move(topic)};
    return envelope;
}

RequestEnvelope MakeSyncGroup(std::uint64_t request_id,
                              std::string group_id,
                              std::string member_id,
                              std::int32_t generation_id) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kSyncGroup;
    envelope.request_id = request_id;
    envelope.body =
        SyncGroupRequest{std::move(group_id), std::move(member_id), generation_id};
    return envelope;
}

RequestEnvelope MakeHeartbeat(std::uint64_t request_id,
                              std::string group_id,
                              std::string member_id,
                              std::int32_t generation_id) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kHeartbeat;
    envelope.request_id = request_id;
    envelope.body =
        HeartbeatRequest{std::move(group_id), std::move(member_id), generation_id};
    return envelope;
}

RequestEnvelope MakeLeaveGroup(std::uint64_t request_id,
                               std::string group_id,
                               std::string member_id) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kLeaveGroup;
    envelope.request_id = request_id;
    envelope.body = LeaveGroupRequest{std::move(group_id), std::move(member_id)};
    return envelope;
}

BrokerService MakeService(const TempDir& temp_dir,
                          std::chrono::milliseconds session_timeout =
                              std::chrono::milliseconds(10000)) {
    BrokerServiceOptions options;
    options.storage.data_dir = temp_dir.path();
    options.storage.segment_bytes = 1024 * 1024;
    options.storage.flush_policy = FlushPolicy::kAsync;
    options.consumer.session_timeout = session_timeout;
    return BrokerService(std::move(options));
}

TEST(BrokerServiceTest, CreateTopicProduceAndFetch) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());

    auto create = service.Handle(MakeCreateTopic(1, "orders", 2));
    ASSERT_EQ(create.error.error_code, ProtocolErrorCode::kNone) << create.error.message;
    ASSERT_TRUE(std::holds_alternative<CreateTopicResponse>(create.body));
    EXPECT_EQ(std::get<CreateTopicResponse>(create.body).partition_count, 2U);

    auto produce = service.Handle(MakeProduce(2, "orders", 1, "k1", "v1"));
    ASSERT_EQ(produce.error.error_code, ProtocolErrorCode::kNone) << produce.error.message;
    ASSERT_TRUE(std::holds_alternative<ProduceResponse>(produce.body));
    EXPECT_EQ(std::get<ProduceResponse>(produce.body).partition_id, 1);
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
    ASSERT_TRUE(service.Start().ok());

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

TEST(BrokerServiceTest, TopicMetadataPersistsAcrossRestart) {
    TempDir temp_dir;
    {
        auto service = MakeService(temp_dir);
        ASSERT_TRUE(service.Start().ok());
        ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 3)).error.error_code,
                  ProtocolErrorCode::kNone);
        ASSERT_EQ(service.Handle(MakeCreateTopic(2, "payments", 1)).error.error_code,
                  ProtocolErrorCode::kNone);
        ASSERT_EQ(service.Handle(MakeProduce(3, "orders", 2, "k", "v")).error.error_code,
                  ProtocolErrorCode::kNone);
        ASSERT_TRUE(service.Close().ok());
    }

    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());

    auto metadata = service.Handle(MakeMetadata(4, ""));
    ASSERT_EQ(metadata.error.error_code, ProtocolErrorCode::kNone) << metadata.error.message;
    const auto& metadata_body = std::get<MetadataResponse>(metadata.body);
    ASSERT_EQ(metadata_body.topics.size(), 2U);
    EXPECT_EQ(metadata_body.topics[0].topic, "orders");
    EXPECT_EQ(metadata_body.topics[0].partition_count, 3U);
    EXPECT_EQ(metadata_body.topics[1].topic, "payments");
    EXPECT_EQ(metadata_body.topics[1].partition_count, 1U);

    auto fetch = service.Handle(MakeFetch(5, "orders", 2, 0));
    ASSERT_EQ(fetch.error.error_code, ProtocolErrorCode::kNone) << fetch.error.message;
    const auto& fetch_body = std::get<FetchResponse>(fetch.body);
    ASSERT_EQ(fetch_body.records.size(), 1U);
    EXPECT_EQ(fetch_body.records[0].key, "k");
    EXPECT_EQ(fetch_body.records[0].value, "v");

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, CommitOffsetPersistsAcrossRestart) {
    TempDir temp_dir;
    {
        auto service = MakeService(temp_dir);
        ASSERT_TRUE(service.Start().ok());
        ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 1)).error.error_code,
                  ProtocolErrorCode::kNone);
        ASSERT_EQ(service.Handle(MakeProduce(2, "orders", 0, "k1", "v1")).error.error_code,
                  ProtocolErrorCode::kNone);
        ASSERT_EQ(service.Handle(MakeProduce(3, "orders", 0, "k2", "v2")).error.error_code,
                  ProtocolErrorCode::kNone);

        auto missing = service.Handle(MakeFetchCommittedOffset(4, "group-a", "orders", 0));
        ASSERT_EQ(missing.error.error_code, ProtocolErrorCode::kNone) << missing.error.message;
        const auto& missing_body = std::get<FetchCommittedOffsetResponse>(missing.body);
        EXPECT_FALSE(missing_body.committed);
        EXPECT_EQ(missing_body.offset, 0);

        auto commit = service.Handle(MakeCommitOffset(5, "group-a", "orders", 0, 2));
        ASSERT_EQ(commit.error.error_code, ProtocolErrorCode::kNone) << commit.error.message;
        const auto& commit_body = std::get<CommitOffsetResponse>(commit.body);
        EXPECT_EQ(commit_body.group_id, "group-a");
        EXPECT_EQ(commit_body.topic, "orders");
        EXPECT_EQ(commit_body.partition_id, 0);
        EXPECT_EQ(commit_body.offset, 2);
        ASSERT_TRUE(service.Close().ok());
    }

    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());
    auto fetched = service.Handle(MakeFetchCommittedOffset(6, "group-a", "orders", 0));
    ASSERT_EQ(fetched.error.error_code, ProtocolErrorCode::kNone) << fetched.error.message;
    const auto& body = std::get<FetchCommittedOffsetResponse>(fetched.body);
    EXPECT_TRUE(body.committed);
    EXPECT_EQ(body.offset, 2);

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, RejectsInvalidRequests) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());

    EXPECT_EQ(service.Handle(MakeProduce(1, "missing", 0, "k", "v")).error.error_code,
              ProtocolErrorCode::kTopicNotFound);

    ASSERT_EQ(service.Handle(MakeCreateTopic(2, "orders", 1)).error.error_code,
              ProtocolErrorCode::kNone);

    EXPECT_EQ(service.Handle(MakeCreateTopic(3, "orders", 2)).error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeCreateTopic(30, "orders", 1)).error.error_code,
              ProtocolErrorCode::kNone);
    EXPECT_EQ(service.Handle(MakeProduce(4, "orders", 4, "k", "v")).error.error_code,
              ProtocolErrorCode::kTopicNotFound);
    EXPECT_EQ(service.Handle(MakeFetch(5, "orders", 0, 10)).error.error_code,
              ProtocolErrorCode::kOffsetOutOfRange);

    auto end = service.Handle(MakeFetch(50, "orders", 0, 0));
    ASSERT_EQ(end.error.error_code, ProtocolErrorCode::kNone) << end.error.message;
    const auto& end_body = std::get<FetchResponse>(end.body);
    EXPECT_EQ(end_body.base_offset, 0);
    EXPECT_EQ(end_body.high_watermark, 0);
    EXPECT_TRUE(end_body.records.empty());

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

TEST(BrokerServiceTest, RejectsInvalidTopicNames) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());

    EXPECT_EQ(service.Handle(MakeCreateTopic(1, "", 1)).error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeCreateTopic(2, ".", 1)).error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeCreateTopic(3, "..", 1)).error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeCreateTopic(4, "bad/name", 1)).error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeCreateTopic(5, "bad name", 1)).error.error_code,
              ProtocolErrorCode::kInvalidRequest);

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, RejectsInvalidConsumerOffsets) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());
    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 1)).error.error_code,
              ProtocolErrorCode::kNone);
    ASSERT_EQ(service.Handle(MakeProduce(2, "orders", 0, "k", "v")).error.error_code,
              ProtocolErrorCode::kNone);

    EXPECT_EQ(service.Handle(MakeCommitOffset(3, "bad/group", "orders", 0, 1))
                  .error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeCommitOffset(4, "group-a", "missing", 0, 1))
                  .error.error_code,
              ProtocolErrorCode::kTopicNotFound);
    EXPECT_EQ(service.Handle(MakeCommitOffset(5, "group-a", "orders", 2, 1))
                  .error.error_code,
              ProtocolErrorCode::kTopicNotFound);
    EXPECT_EQ(service.Handle(MakeCommitOffset(6, "group-a", "orders", 0, -1))
                  .error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeCommitOffset(7, "group-a", "orders", 0, 2))
                  .error.error_code,
              ProtocolErrorCode::kOffsetOutOfRange);
    EXPECT_EQ(service.Handle(MakeFetchCommittedOffset(8, "bad/group", "orders", 0))
                  .error.error_code,
              ProtocolErrorCode::kInvalidRequest);

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, ConsumerGroupAssignsPartitionsWithRangeAssignor) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());
    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 6)).error.error_code,
              ProtocolErrorCode::kNone);

    auto join1 = service.Handle(MakeJoinGroup(2, "group-a", "", "orders"));
    ASSERT_EQ(join1.error.error_code, ProtocolErrorCode::kNone) << join1.error.message;
    auto join2 = service.Handle(MakeJoinGroup(3, "group-a", "", "orders"));
    ASSERT_EQ(join2.error.error_code, ProtocolErrorCode::kNone) << join2.error.message;
    auto join3 = service.Handle(MakeJoinGroup(4, "group-a", "", "orders"));
    ASSERT_EQ(join3.error.error_code, ProtocolErrorCode::kNone) << join3.error.message;

    const std::string member1 = std::get<JoinGroupResponse>(join1.body).member_id;
    const std::string member2 = std::get<JoinGroupResponse>(join2.body).member_id;
    const std::string member3 = std::get<JoinGroupResponse>(join3.body).member_id;
    EXPECT_EQ(member1, "member-1");
    EXPECT_EQ(member2, "member-2");
    EXPECT_EQ(member3, "member-3");

    auto refresh1 = service.Handle(MakeJoinGroup(5, "group-a", member1, "orders"));
    auto refresh2 = service.Handle(MakeJoinGroup(6, "group-a", member2, "orders"));
    auto refresh3 = service.Handle(MakeJoinGroup(7, "group-a", member3, "orders"));
    ASSERT_EQ(refresh1.error.error_code, ProtocolErrorCode::kNone) << refresh1.error.message;
    ASSERT_EQ(refresh2.error.error_code, ProtocolErrorCode::kNone) << refresh2.error.message;
    ASSERT_EQ(refresh3.error.error_code, ProtocolErrorCode::kNone) << refresh3.error.message;
    const auto generation = std::get<JoinGroupResponse>(refresh1.body).generation_id;
    EXPECT_EQ(std::get<JoinGroupResponse>(refresh2.body).generation_id, generation);
    EXPECT_EQ(std::get<JoinGroupResponse>(refresh3.body).generation_id, generation);

    auto sync1 = service.Handle(MakeSyncGroup(8, "group-a", member1, generation));
    auto sync2 = service.Handle(MakeSyncGroup(9, "group-a", member2, generation));
    auto sync3 = service.Handle(MakeSyncGroup(10, "group-a", member3, generation));
    ASSERT_EQ(sync1.error.error_code, ProtocolErrorCode::kNone) << sync1.error.message;
    ASSERT_EQ(sync2.error.error_code, ProtocolErrorCode::kNone) << sync2.error.message;
    ASSERT_EQ(sync3.error.error_code, ProtocolErrorCode::kNone) << sync3.error.message;

    EXPECT_EQ(std::get<SyncGroupResponse>(sync1.body).assignment.partition_ids,
              std::vector<PartitionId>({0, 1}));
    EXPECT_EQ(std::get<SyncGroupResponse>(sync2.body).assignment.partition_ids,
              std::vector<PartitionId>({2, 3}));
    EXPECT_EQ(std::get<SyncGroupResponse>(sync3.body).assignment.partition_ids,
              std::vector<PartitionId>({4, 5}));

    auto leave2 = service.Handle(MakeLeaveGroup(11, "group-a", member2));
    ASSERT_EQ(leave2.error.error_code, ProtocolErrorCode::kNone) << leave2.error.message;
    const auto next_generation = std::get<LeaveGroupResponse>(leave2.body).generation_id;
    auto after_leave1 = service.Handle(MakeSyncGroup(12, "group-a", member1, next_generation));
    auto after_leave3 = service.Handle(MakeSyncGroup(13, "group-a", member3, next_generation));
    ASSERT_EQ(after_leave1.error.error_code, ProtocolErrorCode::kNone)
        << after_leave1.error.message;
    ASSERT_EQ(after_leave3.error.error_code, ProtocolErrorCode::kNone)
        << after_leave3.error.message;
    EXPECT_EQ(std::get<SyncGroupResponse>(after_leave1.body).assignment.partition_ids,
              std::vector<PartitionId>({0, 1, 2}));
    EXPECT_EQ(std::get<SyncGroupResponse>(after_leave3.body).assignment.partition_ids,
              std::vector<PartitionId>({3, 4, 5}));

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, ConsumerGroupRejectsUnknownMembersAndStaleGenerations) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());
    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 2)).error.error_code,
              ProtocolErrorCode::kNone);
    ASSERT_EQ(service.Handle(MakeCreateTopic(2, "payments", 1)).error.error_code,
              ProtocolErrorCode::kNone);

    auto join1 = service.Handle(MakeJoinGroup(3, "group-a", "", "orders"));
    ASSERT_EQ(join1.error.error_code, ProtocolErrorCode::kNone) << join1.error.message;
    const auto member1 = std::get<JoinGroupResponse>(join1.body).member_id;
    const auto generation1 = std::get<JoinGroupResponse>(join1.body).generation_id;

    auto join2 = service.Handle(MakeJoinGroup(4, "group-a", "", "orders"));
    ASSERT_EQ(join2.error.error_code, ProtocolErrorCode::kNone) << join2.error.message;

    EXPECT_EQ(service.Handle(MakeHeartbeat(5, "group-a", member1, generation1))
                  .error.error_code,
              ProtocolErrorCode::kIllegalGeneration);
    EXPECT_EQ(service.Handle(MakeSyncGroup(6, "group-a", "missing-member", generation1))
                  .error.error_code,
              ProtocolErrorCode::kUnknownMember);
    EXPECT_EQ(service.Handle(MakeJoinGroup(7, "group-a", "", "payments")).error.error_code,
              ProtocolErrorCode::kInvalidRequest);
    EXPECT_EQ(service.Handle(MakeJoinGroup(8, "group-a", "bad/member", "orders"))
                  .error.error_code,
              ProtocolErrorCode::kInvalidRequest);

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, ConsumerGroupTimesOutMembersAndReassignsPartitions) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir, std::chrono::milliseconds(40));
    ASSERT_TRUE(service.Start().ok());
    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "orders", 4)).error.error_code,
              ProtocolErrorCode::kNone);

    auto join1 = service.Handle(MakeJoinGroup(2, "group-a", "", "orders"));
    auto join2 = service.Handle(MakeJoinGroup(3, "group-a", "", "orders"));
    ASSERT_EQ(join1.error.error_code, ProtocolErrorCode::kNone) << join1.error.message;
    ASSERT_EQ(join2.error.error_code, ProtocolErrorCode::kNone) << join2.error.message;
    const auto member1 = std::get<JoinGroupResponse>(join1.body).member_id;
    const auto generation = std::get<JoinGroupResponse>(join2.body).generation_id;

    std::int32_t refreshed_generation = 0;
    for (int attempt = 0; attempt < 80; ++attempt) {
        auto heartbeat = service.Handle(
            MakeHeartbeat(static_cast<std::uint64_t>(10 + attempt), "group-a",
                          member1, generation));
        if (heartbeat.error.error_code == ProtocolErrorCode::kIllegalGeneration) {
            auto refreshed = service.Handle(MakeJoinGroup(200, "group-a", member1, "orders"));
            ASSERT_EQ(refreshed.error.error_code, ProtocolErrorCode::kNone)
                << refreshed.error.message;
            refreshed_generation = std::get<JoinGroupResponse>(refreshed.body).generation_id;
            break;
        }
        ASSERT_EQ(heartbeat.error.error_code, ProtocolErrorCode::kNone)
            << heartbeat.error.message;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_GT(refreshed_generation, generation);

    auto sync = service.Handle(MakeSyncGroup(300, "group-a", member1, refreshed_generation));
    ASSERT_EQ(sync.error.error_code, ProtocolErrorCode::kNone) << sync.error.message;
    EXPECT_EQ(std::get<SyncGroupResponse>(sync.body).assignment.partition_ids,
              std::vector<PartitionId>({0, 1, 2, 3}));

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, ConcurrentProduceAssignsUniqueOffsets) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());
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

TEST(BrokerServiceTest, AutoPartitionUsesStableKeyHashAndRoundRobin) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());
    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "keyed", 4)).error.error_code,
              ProtocolErrorCode::kNone);
    ASSERT_EQ(service.Handle(MakeCreateTopic(2, "rr", 4)).error.error_code,
              ProtocolErrorCode::kNone);

    PartitionId keyed_partition = kInvalidPartitionId;
    for (int i = 0; i < 100; ++i) {
        auto response = service.Handle(
            MakeProduce(static_cast<std::uint64_t>(100 + i), "keyed", kInvalidPartitionId,
                        "same-key", "v"));
        ASSERT_EQ(response.error.error_code, ProtocolErrorCode::kNone)
            << response.error.message;
        const auto& body = std::get<ProduceResponse>(response.body);
        if (i == 0) {
            keyed_partition = body.partition_id;
        }
        EXPECT_EQ(body.partition_id, keyed_partition);
    }

    std::array<int, 4> counts{};
    for (int i = 0; i < 8; ++i) {
        auto response = service.Handle(
            MakeProduce(static_cast<std::uint64_t>(300 + i), "rr", kInvalidPartitionId, "",
                        "v"));
        ASSERT_EQ(response.error.error_code, ProtocolErrorCode::kNone)
            << response.error.message;
        const auto partition = std::get<ProduceResponse>(response.body).partition_id;
        ASSERT_GE(partition, 0);
        ASSERT_LT(partition, static_cast<PartitionId>(counts.size()));
        ++counts[static_cast<std::size_t>(partition)];
    }
    for (int count : counts) {
        EXPECT_EQ(count, 2);
    }

    ASSERT_TRUE(service.Close().ok());
}

TEST(BrokerServiceTest, ConcurrentProduceAcrossPartitionsKeepsOffsetsLocal) {
    TempDir temp_dir;
    auto service = MakeService(temp_dir);
    ASSERT_TRUE(service.Start().ok());
    ASSERT_EQ(service.Handle(MakeCreateTopic(1, "parallel", 8)).error.error_code,
              ProtocolErrorCode::kNone);

    constexpr int kPartitions = 8;
    constexpr int kPerPartition = 64;
    std::array<std::vector<Offset>, kPartitions> offsets;
    std::array<std::mutex, kPartitions> mutexes;

    std::vector<std::thread> threads;
    for (int partition = 0; partition < kPartitions; ++partition) {
        threads.emplace_back([&, partition] {
            for (int i = 0; i < kPerPartition; ++i) {
                auto response = service.Handle(MakeProduce(
                    static_cast<std::uint64_t>(1000 + partition * kPerPartition + i),
                    "parallel", partition, "k", "v"));
                ASSERT_EQ(response.error.error_code, ProtocolErrorCode::kNone)
                    << response.error.message;
                Offset offset = std::get<ProduceResponse>(response.body).base_offset;
                std::lock_guard lock(mutexes[static_cast<std::size_t>(partition)]);
                offsets[static_cast<std::size_t>(partition)].push_back(offset);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    for (auto& partition_offsets : offsets) {
        ASSERT_EQ(partition_offsets.size(), static_cast<std::size_t>(kPerPartition));
        std::sort(partition_offsets.begin(), partition_offsets.end());
        for (Offset offset = 0; offset < kPerPartition; ++offset) {
            EXPECT_EQ(partition_offsets[static_cast<std::size_t>(offset)], offset);
        }
    }

    ASSERT_TRUE(service.Close().ok());
}

}  // namespace
}  // namespace logmq
