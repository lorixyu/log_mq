#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <poll.h>
#include <span>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <variant>
#include <vector>

#include "logmq/broker/broker_service.h"
#include "logmq/client/client.h"
#include "logmq/net/tcp_server.h"
#include "logmq/protocol/codec.h"

namespace logmq {
namespace {

class TempDir {
public:
    TempDir() {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() / ("logmq_network_" + std::to_string(now));
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

class RunningServer {
public:
    explicit RunningServer(RetryOptions retry = {}) {
        BrokerServiceOptions service_options;
        service_options.storage.data_dir = temp_dir_.path();
        service_options.storage.segment_bytes = 1024 * 1024;
        service_options.retry = retry;
        service_ = std::make_unique<BrokerService>(std::move(service_options));
        Status service_status = service_->Start();
        if (!service_status.ok()) {
            throw std::runtime_error(service_status.ToString());
        }

        TcpServerOptions server_options;
        server_options.host = "127.0.0.1";
        server_options.port = 0;
        server_options.io_threads = 2;
        server_options.worker_threads = 4;
        server_options.max_connections = 256;
        server_options.worker_queue_size = 256;
        server_ = std::make_unique<TcpServer>(server_options, service_.get());
        Status status = server_->Start();
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
        port_ = server_->bound_port();
        thread_ = std::thread([this] { server_->Loop(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    ~RunningServer() {
        server_->Stop();
        if (thread_.joinable()) {
            thread_.join();
        }
        EXPECT_TRUE(service_->Close().ok());
    }

    [[nodiscard]] std::uint16_t port() const { return port_; }

private:
    TempDir temp_dir_;
    std::unique_ptr<BrokerService> service_;
    std::unique_ptr<TcpServer> server_;
    std::thread thread_;
    std::uint16_t port_{0};
};

int Connect(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
#ifdef SO_NOSIGPIPE
    int enabled = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

void WriteAll(int fd, std::span<const std::byte> bytes) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + written, bytes.size() - written, 0);
        ASSERT_GE(n, 0) << std::strerror(errno);
        written += static_cast<std::size_t>(n);
    }
}

void ReadExact(int fd, std::byte* data, std::size_t size) {
    std::size_t read = 0;
    while (read < size) {
        const ssize_t n = ::recv(fd, data + read, size - read, 0);
        ASSERT_GT(n, 0) << "connection closed";
        read += static_cast<std::size_t>(n);
    }
}

std::uint32_t ReadFrameLength(const std::array<std::byte, 4>& bytes) {
    std::uint32_t value = 0;
    for (std::byte byte : bytes) {
        value <<= 8U;
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(byte));
    }
    return value;
}

ResponseEnvelope ReadResponse(int fd) {
    std::array<std::byte, 4> header{};
    ReadExact(fd, header.data(), header.size());
    const std::uint32_t frame_len = ReadFrameLength(header);
    EXPECT_GE(frame_len, kFrameHeaderBytes);
    EXPECT_LE(frame_len, kMaxFrameBytes);

    std::vector<std::byte> frame(frame_len);
    std::copy(header.begin(), header.end(), frame.begin());
    ReadExact(fd, frame.data() + header.size(), frame.size() - header.size());
    auto decoded = DecodeResponse(std::span<const std::byte>(frame.data(), frame.size()));
    EXPECT_TRUE(decoded.ok()) << decoded.status().ToString();
    return std::move(decoded).value();
}

RequestEnvelope CreateTopicRequestEnvelope(std::uint64_t request_id, std::string topic,
                                           std::uint32_t partitions) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kCreateTopic;
    envelope.request_id = request_id;
    envelope.body = CreateTopicRequest{std::move(topic), partitions};
    return envelope;
}

RequestEnvelope ProduceRequestEnvelope(std::uint64_t request_id, std::string topic,
                                       PartitionId partition, std::string key,
                                       std::string value) {
    ProduceRequest request;
    request.topic = std::move(topic);
    request.partition_id = partition;
    request.records.push_back(ProtocolRecord{request_id, std::move(key), std::move(value)});

    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kProduce;
    envelope.request_id = request_id;
    envelope.body = std::move(request);
    return envelope;
}

RequestEnvelope FetchRequestEnvelope(std::uint64_t request_id, std::string topic,
                                     PartitionId partition, Offset offset) {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kFetch;
    envelope.request_id = request_id;
    envelope.body = FetchRequest{std::move(topic), partition, offset, 4096};
    return envelope;
}

RequestEnvelope CommitOffsetRequestEnvelope(std::uint64_t request_id,
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

RequestEnvelope FetchCommittedOffsetRequestEnvelope(std::uint64_t request_id,
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

ResponseEnvelope RoundTrip(std::uint16_t port, const RequestEnvelope& request) {
    auto encoded = EncodeRequest(request);
    EXPECT_TRUE(encoded.ok()) << encoded.status().ToString();
    const int fd = Connect(port);
    EXPECT_GE(fd, 0);
    WriteAll(fd, std::span<const std::byte>(encoded.value().data(), encoded.value().size()));
    ResponseEnvelope response = ReadResponse(fd);
    ::close(fd);
    return response;
}

void AppendBigEndian32(std::uint32_t value, std::vector<std::byte>& bytes) {
    bytes.push_back(static_cast<std::byte>((value >> 24U) & 0xFFU));
    bytes.push_back(static_cast<std::byte>((value >> 16U) & 0xFFU));
    bytes.push_back(static_cast<std::byte>((value >> 8U) & 0xFFU));
    bytes.push_back(static_cast<std::byte>(value & 0xFFU));
}

TEST(NetworkIntegrationTest, LoopbackCreateProduceFetch) {
    RunningServer server;

    auto create = RoundTrip(server.port(), CreateTopicRequestEnvelope(1, "orders", 1));
    ASSERT_EQ(create.error.error_code, ProtocolErrorCode::kNone) << create.error.message;

    auto produce = RoundTrip(server.port(), ProduceRequestEnvelope(2, "orders", 0, "k1", "v1"));
    ASSERT_EQ(produce.error.error_code, ProtocolErrorCode::kNone) << produce.error.message;
    EXPECT_EQ(std::get<ProduceResponse>(produce.body).partition_id, 0);
    EXPECT_EQ(std::get<ProduceResponse>(produce.body).base_offset, 0);

    auto fetch = RoundTrip(server.port(), FetchRequestEnvelope(3, "orders", 0, 0));
    ASSERT_EQ(fetch.error.error_code, ProtocolErrorCode::kNone) << fetch.error.message;
    const auto& body = std::get<FetchResponse>(fetch.body);
    ASSERT_EQ(body.records.size(), 1U);
    EXPECT_EQ(body.records[0].key, "k1");
    EXPECT_EQ(body.records[0].value, "v1");
}

TEST(NetworkIntegrationTest, LoopbackAutoPartitionProduceFetch) {
    RunningServer server;

    auto create = RoundTrip(server.port(), CreateTopicRequestEnvelope(1, "auto", 4));
    ASSERT_EQ(create.error.error_code, ProtocolErrorCode::kNone) << create.error.message;

    auto produce = RoundTrip(
        server.port(), ProduceRequestEnvelope(2, "auto", kInvalidPartitionId, "k1", "v1"));
    ASSERT_EQ(produce.error.error_code, ProtocolErrorCode::kNone) << produce.error.message;
    const auto& produce_body = std::get<ProduceResponse>(produce.body);
    ASSERT_GE(produce_body.partition_id, 0);
    ASSERT_LT(produce_body.partition_id, 4);
    EXPECT_EQ(produce_body.base_offset, 0);

    auto fetch = RoundTrip(server.port(),
                           FetchRequestEnvelope(3, "auto", produce_body.partition_id, 0));
    ASSERT_EQ(fetch.error.error_code, ProtocolErrorCode::kNone) << fetch.error.message;
    const auto& fetch_body = std::get<FetchResponse>(fetch.body);
    ASSERT_EQ(fetch_body.records.size(), 1U);
    EXPECT_EQ(fetch_body.records[0].key, "k1");
    EXPECT_EQ(fetch_body.records[0].value, "v1");
}

TEST(NetworkIntegrationTest, LoopbackCommitAndFetchCommittedOffset) {
    RunningServer server;

    auto create = RoundTrip(server.port(), CreateTopicRequestEnvelope(1, "offsets", 1));
    ASSERT_EQ(create.error.error_code, ProtocolErrorCode::kNone) << create.error.message;
    auto produce = RoundTrip(server.port(), ProduceRequestEnvelope(2, "offsets", 0, "k", "v"));
    ASSERT_EQ(produce.error.error_code, ProtocolErrorCode::kNone) << produce.error.message;

    auto missing =
        RoundTrip(server.port(), FetchCommittedOffsetRequestEnvelope(3, "group-a", "offsets", 0));
    ASSERT_EQ(missing.error.error_code, ProtocolErrorCode::kNone) << missing.error.message;
    EXPECT_FALSE(std::get<FetchCommittedOffsetResponse>(missing.body).committed);

    auto commit =
        RoundTrip(server.port(), CommitOffsetRequestEnvelope(4, "group-a", "offsets", 0, 1));
    ASSERT_EQ(commit.error.error_code, ProtocolErrorCode::kNone) << commit.error.message;

    auto fetched =
        RoundTrip(server.port(), FetchCommittedOffsetRequestEnvelope(5, "group-a", "offsets", 0));
    ASSERT_EQ(fetched.error.error_code, ProtocolErrorCode::kNone) << fetched.error.message;
    const auto& body = std::get<FetchCommittedOffsetResponse>(fetched.body);
    EXPECT_TRUE(body.committed);
    EXPECT_EQ(body.offset, 1);
}

TEST(NetworkIntegrationTest, ClientSdkProducesPollsAndCommitsOffsets) {
    RunningServer server;
    ClientOptions options;
    options.host = "127.0.0.1";
    options.port = server.port();

    AdminClient admin(options);
    auto created = admin.CreateTopic("sdk", 2);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto metadata = admin.Metadata("sdk");
    ASSERT_TRUE(metadata.ok()) << metadata.status().ToString();
    ASSERT_EQ(metadata.value().topics.size(), 1U);
    EXPECT_EQ(metadata.value().topics[0].partition_count, 2U);

    Producer producer(ProducerOptions{options, 1});
    auto produced = producer.Send("sdk", "key-a", "value-a");
    ASSERT_TRUE(produced.ok()) << produced.status().ToString();
    ASSERT_GE(produced.value().partition_id, 0);
    ASSERT_LT(produced.value().partition_id, 2);

    Consumer consumer(ConsumerOptions{options, "group-sdk", 4096});
    auto subscribed = consumer.Subscribe("sdk");
    ASSERT_TRUE(subscribed.ok()) << subscribed.status().ToString();
    auto records = consumer.Poll(std::chrono::milliseconds(200));
    ASSERT_TRUE(records.ok()) << records.status().ToString();
    ASSERT_EQ(records.value().size(), 1U);
    EXPECT_EQ(records.value()[0].key, "key-a");
    EXPECT_EQ(records.value()[0].value, "value-a");
    EXPECT_TRUE(consumer.CommitSync().ok());
    EXPECT_TRUE(consumer.Close().ok());

    Consumer resumed(ConsumerOptions{options, "group-sdk", 4096});
    ASSERT_TRUE(resumed.Subscribe("sdk").ok());
    auto empty = resumed.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(empty.ok()) << empty.status().ToString();
    EXPECT_TRUE(empty.value().empty());
}

TEST(NetworkIntegrationTest, ClientSdkFetchesOrdinaryMessageAtFrameLimit) {
    RunningServer server;
    ClientOptions options{"127.0.0.1", server.port(), std::chrono::seconds(15)};
    AdminClient admin(options);
    ASSERT_TRUE(admin.CreateTopic("a", 1).ok());
    // A v1 Fetch response for topic "a" and an empty key adds 58 bytes.
    const std::string value(kMaxFrameBytes - 58, 'x');
    Producer producer(ProducerOptions{options});
    auto produced = producer.Send("a", "", value, 0);
    ASSERT_TRUE(produced.ok()) << produced.status().ToString();

    Consumer consumer(ConsumerOptions{options, "frame-limit-group", kMaxFrameBytes});
    ASSERT_TRUE(consumer.Subscribe("a").ok());
    auto records = consumer.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(records.ok()) << records.status().ToString();
    ASSERT_EQ(records.value().size(), 1U);
    EXPECT_EQ(records.value().front().topic, "a");
    EXPECT_EQ(records.value().front().value, value);
}

TEST(NetworkIntegrationTest, ClientSdkRetriesAfterDefaultFiveSeconds) {
    RunningServer server;
    ClientOptions options{"127.0.0.1", server.port()};
    AdminClient admin(options);
    ASSERT_TRUE(admin.CreateTopic("retry_default", 1).ok());
    Producer producer(ProducerOptions{options});
    ASSERT_TRUE(producer.Send("retry_default", "key", "value", 0).ok());
    Consumer consumer(ConsumerOptions{options, "retry-default-group"});
    ASSERT_TRUE(consumer.Subscribe("retry_default").ok());
    auto records = consumer.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(records.ok()) << records.status().ToString();
    ASSERT_EQ(records.value().size(), 1);
    const auto start = std::chrono::steady_clock::now();
    auto nack = consumer.Nack(records.value().front(), "handler failed");
    ASSERT_TRUE(nack.ok()) << nack.status().ToString();
    EXPECT_EQ(nack.value().topic, "%RETRY%retry_default");
    EXPECT_EQ(nack.value().retry.retry_count, 1);
    EXPECT_FALSE(nack.value().dead_letter);
    // Nack does not implicitly commit the original record.
    ClientConnection connection(options);
    RequestEnvelope committed;
    committed.api_key = ApiKey::kFetchCommittedOffset;
    committed.body = FetchCommittedOffsetRequest{"retry-default-group", "retry_default", 0};
    auto offset = connection.RoundTrip(committed);
    ASSERT_TRUE(offset.ok());
    EXPECT_FALSE(std::get<FetchCommittedOffsetResponse>(offset.value().body).committed);
    ASSERT_TRUE(consumer.CommitSync().ok());
    auto early = consumer.Poll(std::chrono::milliseconds(100));
    ASSERT_TRUE(early.ok());
    EXPECT_TRUE(early.value().empty());
    auto retried = consumer.Poll(std::chrono::seconds(7));
    ASSERT_TRUE(retried.ok()) << retried.status().ToString();
    ASSERT_EQ(retried.value().size(), 1);
    EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(4900));
    const auto& record = retried.value().front();
    EXPECT_EQ(record.topic, "%RETRY%retry_default");
    EXPECT_EQ(record.offset, 0);
    EXPECT_EQ(record.value, "value");
    ASSERT_TRUE(record.retry);
    EXPECT_EQ(record.retry->original_topic, "retry_default");
    EXPECT_EQ(record.retry->failure_reason, "handler failed");
    EXPECT_EQ(record.retry->retry_count, 1);
}

TEST(NetworkIntegrationTest, ClientSdkThreeRetriesDlqAndSeparateCommittedOffsets) {
    RunningServer server(RetryOptions{std::chrono::milliseconds(30), 3});
    ClientOptions options{"127.0.0.1", server.port()};
    AdminClient admin(options);
    ASSERT_TRUE(admin.CreateTopic("retry_sdk", 1).ok());
    Producer producer(ProducerOptions{options});
    ASSERT_TRUE(producer.Send("retry_sdk", "key", "value", 0).ok());
    ConsumerOptions consumer_options{options, "retry-sdk-group"};
    Consumer consumer(consumer_options);
    ASSERT_TRUE(consumer.Subscribe("retry_sdk").ok());
    auto records = consumer.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(records.ok());
    ASSERT_EQ(records.value().size(), 1);
    for (std::uint32_t count = 1; count <= 4; ++count) {
        auto nack = consumer.Nack(records.value().front(), "failure-" + std::to_string(count));
        ASSERT_TRUE(nack.ok()) << nack.status().ToString();
        EXPECT_EQ(nack.value().retry.retry_count, count);
        EXPECT_EQ(nack.value().dead_letter, count == 4);
        ASSERT_TRUE(consumer.CommitSync().ok());
        if (count == 4) break;
        records = consumer.Poll(std::chrono::seconds(2));
        ASSERT_TRUE(records.ok()) << records.status().ToString();
        ASSERT_EQ(records.value().size(), 1);
        ASSERT_TRUE(records.value().front().retry);
        EXPECT_EQ(records.value().front().retry->retry_count, count);
        EXPECT_EQ(records.value().front().offset, count - 1);
    }
    ASSERT_TRUE(consumer.Close().ok());
    Consumer resumed(consumer_options);
    ASSERT_TRUE(resumed.Subscribe("retry_sdk").ok());
    auto empty = resumed.Poll(std::chrono::milliseconds(100));
    ASSERT_TRUE(empty.ok()) << empty.status().ToString();
    EXPECT_TRUE(empty.value().empty());
    // Explicitly subscribing to DLQ reads it without automatically retrying it.
    Consumer dlq(ConsumerOptions{options, "dlq-inspector"});
    ASSERT_TRUE(dlq.Subscribe("%DLQ%retry_sdk").ok());
    auto dead = dlq.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(dead.ok()) << dead.status().ToString();
    ASSERT_EQ(dead.value().size(), 1);
    ASSERT_TRUE(dead.value().front().retry);
    EXPECT_EQ(dead.value().front().retry->retry_count, 4);
    EXPECT_EQ(dead.value().front().value, "value");
    EXPECT_FALSE(dlq.Nack(dead.value().front(), "again").ok());
    ASSERT_TRUE(dlq.CommitSync().ok());
    auto no_more = dlq.Poll(std::chrono::milliseconds(50));
    ASSERT_TRUE(no_more.ok());
    EXPECT_TRUE(no_more.value().empty());
    EXPECT_FALSE(consumer.Nack(dead.value().front(), "closed").ok());
}

TEST(NetworkIntegrationTest, ClientSdkRetrySeekAndResubscribeKeepTopicsSeparate) {
    RunningServer server(RetryOptions{std::chrono::milliseconds(20), 3});
    ClientOptions options{"127.0.0.1", server.port()};
    AdminClient admin(options);
    ASSERT_TRUE(admin.CreateTopic("retry_seek", 1).ok());
    ASSERT_TRUE(admin.CreateTopic("other", 1).ok());
    Producer producer(ProducerOptions{options});
    ASSERT_TRUE(producer.Send("retry_seek", "key", "failed", 0).ok());
    Consumer consumer(ConsumerOptions{options, "retry-seek-group"});
    ASSERT_TRUE(consumer.Subscribe("retry_seek").ok());
    auto original = consumer.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(original.ok());
    ASSERT_EQ(original.value().size(), 1);
    ASSERT_TRUE(consumer.Nack(original.value().front(), "failure").ok());
    ASSERT_TRUE(consumer.CommitSync().ok());
    auto retry = consumer.Poll(std::chrono::seconds(2));
    ASSERT_TRUE(retry.ok());
    ASSERT_EQ(retry.value().size(), 1);
    ASSERT_TRUE(consumer.Seek("%RETRY%retry_seek", 0, 0).ok());
    auto reread = consumer.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(reread.ok());
    ASSERT_EQ(reread.value().size(), 1);
    EXPECT_EQ(reread.value().front().topic, "%RETRY%retry_seek");
    ASSERT_TRUE(consumer.CommitSync().ok());
    ASSERT_TRUE(consumer.Subscribe("other").ok());
    auto empty = consumer.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(empty.ok());
    EXPECT_TRUE(empty.value().empty());
    EXPECT_FALSE(consumer.Nack(original.value().front(), "wrong subscription").ok());
    ASSERT_TRUE(consumer.Subscribe("retry_seek").ok());
    auto restored = consumer.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(restored.ok());
    EXPECT_TRUE(restored.value().empty());
}

TEST(NetworkIntegrationTest, ClientSdkRebalanceRestoresRetryPositions) {
    RunningServer server(RetryOptions{std::chrono::milliseconds(50), 3});
    ClientOptions options{"127.0.0.1", server.port()};
    AdminClient admin(options);
    ASSERT_TRUE(admin.CreateTopic("retry_rebalance", 2).ok());
    Producer producer(ProducerOptions{options});
    for (int partition = 0; partition < 2; ++partition) {
        ASSERT_TRUE(producer.Send("retry_rebalance", "key", "value", partition).ok());
    }
    ConsumerOptions consumer_options{options, "retry-rebalance-group", 4096,
                                      std::chrono::milliseconds(20)};
    Consumer first(consumer_options);
    ASSERT_TRUE(first.Subscribe("retry_rebalance").ok());
    auto original = first.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(original.ok());
    ASSERT_EQ(original.value().size(), 2);
    for (const auto& record : original.value()) ASSERT_TRUE(first.Nack(record, "failure").ok());
    ASSERT_TRUE(first.CommitSync().ok());
    // Poll may return one partition before the other's retry becomes visible.
    std::vector<ConsumerRecord> retried;
    const auto retry_limit = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (retried.size() < 2 && std::chrono::steady_clock::now() < retry_limit) {
        auto polled = first.Poll(std::chrono::milliseconds(30));
        ASSERT_TRUE(polled.ok()) << polled.status().ToString();
        retried.insert(retried.end(), polled.value().begin(), polled.value().end());
    }
    ASSERT_EQ(retried.size(), 2);
    EXPECT_NE(retried[0].partition_id, retried[1].partition_id);
    ASSERT_TRUE(first.CommitSync().ok());
    Consumer second(consumer_options);
    ASSERT_TRUE(second.Subscribe("retry_rebalance").ok());
    const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (first.AssignedPartitions().size() != 1 && std::chrono::steady_clock::now() < limit) {
        auto empty = first.Poll(std::chrono::milliseconds(30));
        ASSERT_TRUE(empty.ok()) << empty.status().ToString();
        EXPECT_TRUE(empty.value().empty());
    }
    ASSERT_EQ(first.AssignedPartitions().size(), 1);
    ASSERT_EQ(second.AssignedPartitions().size(), 1);
    auto second_empty = second.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(second_empty.ok()) << second_empty.status().ToString();
    EXPECT_TRUE(second_empty.value().empty());
    const auto retained = first.AssignedPartitions().front();
    // Seek and commit are scoped to the retained Retry partition.
    ASSERT_TRUE(first.Seek("%RETRY%retry_rebalance", retained, 0).ok());
    auto replay = first.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(replay.ok());
    ASSERT_EQ(replay.value().size(), 1);
    EXPECT_EQ(replay.value().front().topic, "%RETRY%retry_rebalance");
    EXPECT_EQ(replay.value().front().partition_id, retained);
    ASSERT_TRUE(first.CommitSync().ok());
}

TEST(NetworkIntegrationTest, ClientSdkConsumerGroupSplitsAndReassignsPartitions) {
    RunningServer server;
    ClientOptions options;
    options.host = "127.0.0.1";
    options.port = server.port();

    AdminClient admin(options);
    ASSERT_TRUE(admin.CreateTopic("balanced", 4).ok());

    ConsumerOptions consumer_options{options, "group-balanced", 4096,
                                     std::chrono::milliseconds(10)};
    Consumer first(consumer_options);
    Consumer second(consumer_options);
    ASSERT_TRUE(first.Subscribe("balanced").ok());
    ASSERT_TRUE(second.Subscribe("balanced").ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    ASSERT_TRUE(first.Poll(std::chrono::milliseconds(0)).ok());
    ASSERT_TRUE(second.Poll(std::chrono::milliseconds(0)).ok());

    auto first_partitions = first.AssignedPartitions();
    auto second_partitions = second.AssignedPartitions();
    EXPECT_EQ(first_partitions, std::vector<PartitionId>({0, 1}));
    EXPECT_EQ(second_partitions, std::vector<PartitionId>({2, 3}));

    Producer producer(ProducerOptions{options, 1});
    for (PartitionId partition = 0; partition < 4; ++partition) {
        auto produced = producer.Send("balanced", "k" + std::to_string(partition),
                                      "v" + std::to_string(partition), partition);
        ASSERT_TRUE(produced.ok()) << produced.status().ToString();
    }

    auto first_records = first.Poll(std::chrono::milliseconds(200));
    auto second_records = second.Poll(std::chrono::milliseconds(200));
    ASSERT_TRUE(first_records.ok()) << first_records.status().ToString();
    ASSERT_TRUE(second_records.ok()) << second_records.status().ToString();
    ASSERT_EQ(first_records.value().size(), 2U);
    ASSERT_EQ(second_records.value().size(), 2U);
    for (const ConsumerRecord& record : first_records.value()) {
        EXPECT_TRUE(record.partition_id == 0 || record.partition_id == 1);
    }
    for (const ConsumerRecord& record : second_records.value()) {
        EXPECT_TRUE(record.partition_id == 2 || record.partition_id == 3);
    }

    ASSERT_TRUE(second.Close().ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    ASSERT_TRUE(first.Poll(std::chrono::milliseconds(0)).ok());
    EXPECT_EQ(first.AssignedPartitions(), std::vector<PartitionId>({0, 1, 2, 3}));
    EXPECT_TRUE(first.Close().ok());
}

TEST(NetworkIntegrationTest, ClientSdkReturnsStatusOnConnectionFailure) {
    ClientOptions options;
    options.host = "127.0.0.1";
    options.port = 9;
    options.request_timeout = std::chrono::milliseconds(100);

    Producer producer(ProducerOptions{options, 0});
    auto produced = producer.Send("missing", "k", "v");
    ASSERT_FALSE(produced.ok());
    EXPECT_NE(produced.status().code(), ErrorCode::kOk);
}

TEST(NetworkIntegrationTest, ClientConnectionReusesSocketForSequentialRequests) {
    const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(0);
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    ASSERT_EQ(::listen(listener, 1), 0);
    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> served{0};
    std::thread server([&] {
        pollfd event{listener, POLLIN, 0};
        if (::poll(&event, 1, 1000) <= 0) {
            ::close(listener);
            return;
        }
        const int fd = ::accept(listener, nullptr, nullptr);
        ::close(listener);
        if (fd < 0) {
            return;
        }
        timeval timeout{};
        timeout.tv_sec = 1;
        (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        auto read_bytes = [fd](std::byte* data, std::size_t size) {
            std::size_t received = 0;
            while (received < size) {
                const ssize_t count = ::recv(fd, data + received, size - received, 0);
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                if (count <= 0) {
                    return false;
                }
                received += static_cast<std::size_t>(count);
            }
            return true;
        };
        for (std::uint64_t id = 1; id <= 2; ++id) {
            std::array<std::byte, 4> header{};
            if (!read_bytes(header.data(), header.size())) {
                break;
            }
            const std::uint32_t frame_size = ReadFrameLength(header);
            if (frame_size < header.size() || frame_size > kMaxFrameBytes) {
                break;
            }
            std::vector<std::byte> body(frame_size - header.size());
            if (!read_bytes(body.data(), body.size())) {
                break;
            }
            ResponseEnvelope response;
            response.api_key = ApiKey::kMetadata;
            response.request_id = id;
            response.body = MetadataResponse{};
            auto encoded = EncodeResponse(response);
            if (!encoded.ok()) {
                break;
            }
            WriteAll(fd, encoded.value());
            ++served;
        }
        ::close(fd);
    });

    ClientOptions options;
    options.port = ntohs(address.sin_port);
    options.request_timeout = std::chrono::milliseconds(500);
    ClientConnection connection(options);
    RequestEnvelope request;
    request.api_key = ApiKey::kMetadata;
    request.body = MetadataRequest{};
    request.request_id = 1;
    auto first = connection.RoundTrip(request);
    request.request_id = 2;
    auto second = connection.RoundTrip(request);
    server.join();

    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(served.load(), 2);
}

TEST(NetworkIntegrationTest, ServerStopsWithActiveConnections) {
    for (int attempt = 0; attempt < 10; ++attempt) {
        auto server = std::make_unique<RunningServer>();
        std::vector<int> clients;
        for (int index = 0; index < 8; ++index) {
            const int fd = Connect(server->port());
            ASSERT_GE(fd, 0);
            clients.push_back(fd);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        server.reset();
        for (int fd : clients) {
            ::close(fd);
        }
    }
}

TEST(NetworkIntegrationTest, ServerStopsWithPendingConnectionRegistrations) {
    for (int attempt = 0; attempt < 20; ++attempt) {
        auto server = std::make_unique<RunningServer>();
        std::vector<int> clients;
        for (int index = 0; index < 32; ++index) {
            const int fd = Connect(server->port());
            ASSERT_GE(fd, 0);
            clients.push_back(fd);
        }
        server.reset();
        for (int fd : clients) {
            ::close(fd);
        }
    }
}

TEST(NetworkIntegrationTest, ServerCanStopBeforeLoopStartsFromAnotherThread) {
    TempDir temp_dir;
    BrokerServiceOptions service_options;
    service_options.storage.data_dir = temp_dir.path();
    BrokerService service(service_options);
    TcpServerOptions server_options;
    server_options.host = "127.0.0.1";
    server_options.port = 0;
    TcpServer server(server_options, &service);
    ASSERT_TRUE(server.Start().ok());

    auto stopped = std::async(std::launch::async, [&] { server.Stop(); });
    const bool stopped_without_loop =
        stopped.wait_for(std::chrono::milliseconds(200)) == std::future_status::ready;
    if (!stopped_without_loop) {
        server.Loop();
    }
    stopped.get();
    EXPECT_TRUE(stopped_without_loop);
}

TEST(NetworkIntegrationTest, HandlesStickyFramesOnOneConnection) {
    RunningServer server;
    const int fd = Connect(server.port());
    ASSERT_GE(fd, 0);

    auto create = EncodeRequest(CreateTopicRequestEnvelope(1, "sticky", 1));
    auto produce = EncodeRequest(ProduceRequestEnvelope(2, "sticky", 0, "k", "v"));
    ASSERT_TRUE(create.ok()) << create.status().ToString();
    ASSERT_TRUE(produce.ok()) << produce.status().ToString();

    std::vector<std::byte> bytes;
    bytes.insert(bytes.end(), create.value().begin(), create.value().end());
    bytes.insert(bytes.end(), produce.value().begin(), produce.value().end());
    WriteAll(fd, std::span<const std::byte>(bytes.data(), bytes.size()));

    ResponseEnvelope first = ReadResponse(fd);
    ResponseEnvelope second = ReadResponse(fd);
    ::close(fd);

    EXPECT_EQ(first.error.error_code, ProtocolErrorCode::kNone);
    EXPECT_EQ(second.error.error_code, ProtocolErrorCode::kNone);
    EXPECT_EQ(std::get<ProduceResponse>(second.body).base_offset, 0);
}

TEST(NetworkIntegrationTest, HandlesPartialFrame) {
    RunningServer server;
    const int fd = Connect(server.port());
    ASSERT_GE(fd, 0);

    auto encoded = EncodeRequest(CreateTopicRequestEnvelope(1, "partial", 1));
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    ASSERT_GT(encoded.value().size(), 8U);

    WriteAll(fd, std::span<const std::byte>(encoded.value().data(), 8));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    WriteAll(fd, std::span<const std::byte>(encoded.value().data() + 8,
                                            encoded.value().size() - 8));

    ResponseEnvelope response = ReadResponse(fd);
    ::close(fd);
    EXPECT_EQ(response.error.error_code, ProtocolErrorCode::kNone);
}

TEST(NetworkIntegrationTest, RejectsOversizedFrameAndKeepsServerAlive) {
    RunningServer server;
    const int fd = Connect(server.port());
    ASSERT_GE(fd, 0);

    std::vector<std::byte> invalid;
    AppendBigEndian32(kMaxFrameBytes + 1, invalid);
    WriteAll(fd, std::span<const std::byte>(invalid.data(), invalid.size()));
    ResponseEnvelope response = ReadResponse(fd);
    ::close(fd);
    EXPECT_EQ(response.error.error_code, ProtocolErrorCode::kInvalidRequest);

    auto create = RoundTrip(server.port(), CreateTopicRequestEnvelope(1, "alive", 1));
    EXPECT_EQ(create.error.error_code, ProtocolErrorCode::kNone);
}

TEST(NetworkIntegrationTest, ConcurrentClientsProduceUniqueOffsets) {
    RunningServer server;
    ASSERT_EQ(RoundTrip(server.port(), CreateTopicRequestEnvelope(1, "concurrent", 1))
                  .error.error_code,
              ProtocolErrorCode::kNone);

    constexpr int kClients = 16;
    std::vector<std::thread> threads;
    std::mutex mutex;
    std::vector<Offset> offsets;
    offsets.reserve(kClients);

    for (int i = 0; i < kClients; ++i) {
        threads.emplace_back([&, i] {
            auto response = RoundTrip(
                server.port(), ProduceRequestEnvelope(100 + i, "concurrent", 0, "k", "v"));
            ASSERT_EQ(response.error.error_code, ProtocolErrorCode::kNone)
                << response.error.message;
            std::lock_guard lock(mutex);
            offsets.push_back(std::get<ProduceResponse>(response.body).base_offset);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(offsets.size(), static_cast<std::size_t>(kClients));
    std::sort(offsets.begin(), offsets.end());
    for (Offset offset = 0; offset < kClients; ++offset) {
        EXPECT_EQ(offsets[static_cast<std::size_t>(offset)], offset);
    }
}

}  // namespace
}  // namespace logmq
