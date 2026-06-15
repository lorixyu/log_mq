#include <gtest/gtest.h>

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
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
    RunningServer() {
        BrokerServiceOptions service_options;
        service_options.storage.data_dir = temp_dir_.path();
        service_options.storage.segment_bytes = 1024 * 1024;
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

    Consumer resumed(ConsumerOptions{options, "group-sdk", 4096});
    ASSERT_TRUE(resumed.Subscribe("sdk").ok());
    auto empty = resumed.Poll(std::chrono::milliseconds(0));
    ASSERT_TRUE(empty.ok()) << empty.status().ToString();
    EXPECT_TRUE(empty.value().empty());
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
