#include "logmq/client/client.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <variant>

#include <arpa/inet.h>

#include "logmq/protocol/codec.h"

namespace logmq {
namespace {

std::uint64_t NextRequestId() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::uint64_t NowTimestamp() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

Result<void> SetTimeout(int fd, std::chrono::milliseconds timeout) {
    timeval value{};
    value.tv_sec = static_cast<time_t>(timeout.count() / 1000);
    value.tv_usec = static_cast<suseconds_t>((timeout.count() % 1000) * 1000);
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value)) < 0) {
        return Status::IoError("setsockopt SO_RCVTIMEO: " + std::string(std::strerror(errno)));
    }
    if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value)) < 0) {
        return Status::IoError("setsockopt SO_SNDTIMEO: " + std::string(std::strerror(errno)));
    }
    return {};
}

Result<int> Connect(const ClientOptions& options) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return Status::IoError("socket: " + std::string(std::strerror(errno)));
    }
#ifdef SO_NOSIGPIPE
    int enabled = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif

    auto timeout_status = SetTimeout(fd, options.request_timeout);
    if (!timeout_status.ok()) {
        ::close(fd);
        return timeout_status.status();
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options.port);
    if (::inet_pton(AF_INET, options.host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return Status::InvalidArgument("host must be an IPv4 address");
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        const int error = errno;
        ::close(fd);
        if (error == EAGAIN || error == EWOULDBLOCK || error == ETIMEDOUT) {
            return Status::Timeout("connect: " + std::string(std::strerror(error)));
        }
        return Status::IoError("connect: " + std::string(std::strerror(error)));
    }
    return fd;
}

Status WriteAll(int fd, std::span<const std::byte> bytes) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + written, bytes.size() - written, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
                return Status::Timeout("send: " + std::string(std::strerror(errno)));
            }
            return Status::IoError("send: " + std::string(std::strerror(errno)));
        }
        written += static_cast<std::size_t>(n);
    }
    return Status::Ok();
}

Status ReadExact(int fd, std::byte* data, std::size_t size) {
    std::size_t read = 0;
    while (read < size) {
        const ssize_t n = ::recv(fd, data + read, size - read, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
                return Status::Timeout("recv: " + std::string(std::strerror(errno)));
            }
            return Status::IoError("recv: " + std::string(std::strerror(errno)));
        }
        if (n == 0) {
            return Status::IoError("connection closed while reading response");
        }
        read += static_cast<std::size_t>(n);
    }
    return Status::Ok();
}

std::uint32_t ReadFrameLength(const std::array<std::byte, 4>& bytes) {
    std::uint32_t value = 0;
    for (std::byte byte : bytes) {
        value <<= 8U;
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(byte));
    }
    return value;
}

Status ProtocolErrorToStatus(const ResponseEnvelope& response) {
    switch (response.error.error_code) {
        case ProtocolErrorCode::kNone:
            return Status::Ok();
        case ProtocolErrorCode::kInvalidRequest:
        case ProtocolErrorCode::kUnsupportedVersion:
            return Status::InvalidArgument(response.error.message);
        case ProtocolErrorCode::kTopicNotFound:
        case ProtocolErrorCode::kOffsetOutOfRange:
            return Status::NotFound(response.error.message);
        case ProtocolErrorCode::kInternal:
            return Status::Internal(response.error.message);
    }
    return Status::Internal(response.error.message);
}

template <typename Response>
Result<Response> TypedResponse(Result<ResponseEnvelope> envelope,
                               ApiKey expected_api_key,
                               const char* expected_name) {
    if (!envelope.ok()) {
        return envelope.status();
    }
    if (envelope.value().error.error_code != ProtocolErrorCode::kNone) {
        return ProtocolErrorToStatus(envelope.value());
    }
    if (envelope.value().api_key != expected_api_key ||
        !std::holds_alternative<Response>(envelope.value().body)) {
        return Status::Internal(std::string("unexpected ") + expected_name + " response");
    }
    return std::get<Response>(std::move(envelope).value().body);
}

}  // namespace

ClientConnection::ClientConnection(ClientOptions options) : options_(std::move(options)) {}

Result<ResponseEnvelope> ClientConnection::RoundTrip(const RequestEnvelope& request) const {
    auto encoded = EncodeRequest(request);
    if (!encoded.ok()) {
        return encoded.status();
    }

    auto fd = Connect(options_);
    if (!fd.ok()) {
        return fd.status();
    }

    Status status = WriteAll(fd.value(), std::span<const std::byte>(encoded.value().data(),
                                                                    encoded.value().size()));
    if (!status.ok()) {
        ::close(fd.value());
        return status;
    }

    std::array<std::byte, 4> header{};
    status = ReadExact(fd.value(), header.data(), header.size());
    if (!status.ok()) {
        ::close(fd.value());
        return status;
    }

    const std::uint32_t frame_len = ReadFrameLength(header);
    if (frame_len < kFrameHeaderBytes || frame_len > kMaxFrameBytes) {
        ::close(fd.value());
        return Status::InvalidArgument("invalid response frame length");
    }

    std::vector<std::byte> frame(frame_len);
    std::copy(header.begin(), header.end(), frame.begin());
    status = ReadExact(fd.value(), frame.data() + header.size(), frame.size() - header.size());
    ::close(fd.value());
    if (!status.ok()) {
        return status;
    }

    return DecodeResponse(std::span<const std::byte>(frame.data(), frame.size()));
}

AdminClient::AdminClient(ClientOptions options) : connection_(std::move(options)) {}

Result<CreateTopicResponse> AdminClient::CreateTopic(TopicName topic,
                                                     std::uint32_t partition_count) const {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kCreateTopic;
    envelope.version = kProtocolVersion;
    envelope.request_id = NextRequestId();
    envelope.body.emplace<CreateTopicRequest>(
        CreateTopicRequest{std::move(topic), partition_count});
    return TypedResponse<CreateTopicResponse>(connection_.RoundTrip(envelope),
                                              ApiKey::kCreateTopic, "create topic");
}

Result<MetadataResponse> AdminClient::Metadata(TopicName topic) const {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kMetadata;
    envelope.version = kProtocolVersion;
    envelope.request_id = NextRequestId();
    envelope.body.emplace<MetadataRequest>(MetadataRequest{std::move(topic)});
    return TypedResponse<MetadataResponse>(connection_.RoundTrip(envelope), ApiKey::kMetadata,
                                           "metadata");
}

Producer::Producer(ProducerOptions options)
    : options_(std::move(options)),
      connection_(options_.client) {}

Result<ProduceResponse> Producer::Send(TopicName topic,
                                       std::string key,
                                       std::string value,
                                       PartitionId partition_id) const {
    ProtocolRecord record;
    record.timestamp = NowTimestamp();
    record.key = std::move(key);
    record.value = std::move(value);
    std::vector<ProtocolRecord> records;
    records.push_back(std::move(record));
    return SendBatch(std::move(topic), std::move(records), partition_id);
}

Result<ProduceResponse> Producer::SendBatch(TopicName topic,
                                            std::vector<ProtocolRecord> records,
                                            PartitionId partition_id) const {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kProduce;
    envelope.version = kProtocolVersion;
    envelope.request_id = NextRequestId();
    ProduceRequest request;
    request.topic = std::move(topic);
    request.partition_id = partition_id;
    request.records = std::move(records);
    envelope.body.emplace<ProduceRequest>(std::move(request));

    Result<ProduceResponse> last_status = Status::Internal("producer send did not run");
    for (std::uint32_t attempt = 0; attempt <= options_.max_retries; ++attempt) {
        last_status = TypedResponse<ProduceResponse>(connection_.RoundTrip(envelope),
                                                     ApiKey::kProduce, "produce");
        if (last_status.ok()) {
            return last_status;
        }
        if (last_status.status().code() != ErrorCode::kIoError &&
            last_status.status().code() != ErrorCode::kTimeout) {
            return last_status;
        }
    }
    return last_status.status();
}

Consumer::Consumer(ConsumerOptions options)
    : options_(std::move(options)),
      connection_(options_.client) {}

Result<void> Consumer::Subscribe(TopicName topic) {
    AdminClient admin(options_.client);
    auto metadata = admin.Metadata(topic);
    if (!metadata.ok()) {
        return metadata.status();
    }
    if (metadata.value().topics.empty()) {
        return Status::NotFound("topic not found");
    }

    topic_ = std::move(topic);
    assignments_.clear();
    const std::uint32_t partition_count = metadata.value().topics.front().partition_count;
    assignments_.reserve(partition_count);
    for (std::uint32_t partition = 0; partition < partition_count; ++partition) {
        auto committed = FetchCommittedOffset(static_cast<PartitionId>(partition));
        if (!committed.ok()) {
            return committed.status();
        }
        assignments_.push_back(Assignment{
            static_cast<PartitionId>(partition),
            committed.value().committed ? committed.value().offset : 0});
    }
    return {};
}

Result<std::vector<ConsumerRecord>> Consumer::Poll(std::chrono::milliseconds timeout) {
    if (topic_.empty()) {
        return Status::InvalidArgument("consumer must subscribe before poll");
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::vector<ConsumerRecord> output;
    while (true) {
        for (Assignment& assignment : assignments_) {
            RequestEnvelope envelope;
            envelope.api_key = ApiKey::kFetch;
            envelope.version = kProtocolVersion;
            envelope.request_id = NextRequestId();
            envelope.body.emplace<FetchRequest>(
                FetchRequest{topic_, assignment.partition_id,
                             assignment.next_offset, options_.max_bytes});

            auto fetch = TypedResponse<FetchResponse>(connection_.RoundTrip(envelope),
                                                      ApiKey::kFetch, "fetch");
            if (!fetch.ok()) {
                return fetch.status();
            }

            Offset next_offset = assignment.next_offset;
            for (std::size_t i = 0; i < fetch.value().records.size(); ++i) {
                const ProtocolRecord& record = fetch.value().records[i];
                const Offset offset = fetch.value().base_offset + static_cast<Offset>(i);
                if (offset < assignment.next_offset) {
                    continue;
                }
                output.push_back(ConsumerRecord{topic_,
                                                assignment.partition_id,
                                                offset,
                                                record.timestamp,
                                                record.key,
                                                record.value});
                next_offset = offset + 1;
            }
            assignment.next_offset = next_offset;
        }

        if (!output.empty() || timeout.count() == 0 ||
            std::chrono::steady_clock::now() >= deadline) {
            return output;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

Result<void> Consumer::Seek(PartitionId partition_id, Offset offset) {
    if (offset < 0) {
        return Status::InvalidArgument("seek offset must not be negative");
    }
    for (Assignment& assignment : assignments_) {
        if (assignment.partition_id == partition_id) {
            assignment.next_offset = offset;
            return {};
        }
    }
    return Status::NotFound("partition is not assigned");
}

Result<void> Consumer::CommitSync() {
    if (topic_.empty()) {
        return Status::InvalidArgument("consumer must subscribe before commit");
    }

    for (const Assignment& assignment : assignments_) {
        RequestEnvelope envelope;
        envelope.api_key = ApiKey::kCommitOffset;
        envelope.version = kProtocolVersion;
        envelope.request_id = NextRequestId();
        envelope.body.emplace<CommitOffsetRequest>(
            CommitOffsetRequest{options_.group_id, topic_,
                                assignment.partition_id,
                                assignment.next_offset});
        auto committed = TypedResponse<CommitOffsetResponse>(connection_.RoundTrip(envelope),
                                                             ApiKey::kCommitOffset,
                                                             "commit offset");
        if (!committed.ok()) {
            return committed.status();
        }
    }
    return {};
}

Result<FetchCommittedOffsetResponse> Consumer::FetchCommittedOffset(
    PartitionId partition_id) const {
    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kFetchCommittedOffset;
    envelope.version = kProtocolVersion;
    envelope.request_id = NextRequestId();
    envelope.body.emplace<FetchCommittedOffsetRequest>(
        FetchCommittedOffsetRequest{options_.group_id, topic_, partition_id});
    return TypedResponse<FetchCommittedOffsetResponse>(connection_.RoundTrip(envelope),
                                                       ApiKey::kFetchCommittedOffset,
                                                       "fetch committed offset");
}

}  // namespace logmq
