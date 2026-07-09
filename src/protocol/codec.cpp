#include "logmq/protocol/codec.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace logmq {
namespace {

template <typename UInt>
void AppendBigEndian(UInt value, std::vector<std::byte>& output) {
    static_assert(std::is_unsigned_v<UInt>);

    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        const std::size_t shift = (sizeof(UInt) - 1U - i) * 8U;
        output.push_back(static_cast<std::byte>((value >> shift) & 0xFFU));
    }
}

template <typename UInt>
Result<UInt> ReadBigEndian(std::span<const std::byte> data, std::size_t offset) {
    static_assert(std::is_unsigned_v<UInt>);

    if (data.size() < offset + sizeof(UInt)) {
        return Status::InvalidArgument("protocol frame is truncated");
    }

    UInt value = 0;
    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        value <<= 8U;
        value |= static_cast<UInt>(std::to_integer<std::uint8_t>(data[offset + i]));
    }
    return value;
}

void AppendBytes(std::span<const std::byte> bytes, std::vector<std::byte>& output) {
    output.insert(output.end(), bytes.begin(), bytes.end());
}

Status AppendString(std::string_view value, std::vector<std::byte>& output) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("protocol string is too large");
    }

    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(value.size()), output);
    const auto* data = reinterpret_cast<const std::byte*>(value.data());
    output.insert(output.end(), data, data + value.size());
    return Status::Ok();
}

class BodyReader {
public:
    explicit BodyReader(std::span<const std::byte> data) : data_(data) {}

    template <typename UInt>
    Result<UInt> ReadUInt() {
        auto value = ReadBigEndian<UInt>(data_, offset_);
        if (!value.ok()) {
            return value.status();
        }
        offset_ += sizeof(UInt);
        return value.value();
    }

    Result<std::string> ReadString() {
        auto size = ReadUInt<std::uint32_t>();
        if (!size.ok()) {
            return size.status();
        }
        if (size.value() > data_.size() - offset_) {
            return Status::InvalidArgument("protocol string payload is truncated");
        }

        std::string value;
        value.assign(reinterpret_cast<const char*>(data_.data() + offset_), size.value());
        offset_ += size.value();
        return value;
    }

    [[nodiscard]] bool consumed() const { return offset_ == data_.size(); }

private:
    std::span<const std::byte> data_;
    std::size_t offset_{0};
};

Status AppendFrameHeader(ApiKey api_key, std::uint16_t version, std::uint64_t request_id,
                         std::span<const std::byte> body,
                         std::vector<std::byte>& output) {
    if (body.size() > kMaxFrameBytes - kFrameHeaderBytes) {
        return Status::InvalidArgument("protocol frame body is too large");
    }

    const auto total_len = static_cast<std::uint32_t>(kFrameHeaderBytes + body.size());
    AppendBigEndian<std::uint32_t>(total_len, output);
    AppendBigEndian<std::uint16_t>(static_cast<std::uint16_t>(api_key), output);
    AppendBigEndian<std::uint16_t>(version, output);
    AppendBigEndian<std::uint64_t>(request_id, output);
    return Status::Ok();
}

Result<ApiKey> ParseApiKey(std::uint16_t value) {
    switch (value) {
        case static_cast<std::uint16_t>(ApiKey::kProduce):
            return ApiKey::kProduce;
        case static_cast<std::uint16_t>(ApiKey::kFetch):
            return ApiKey::kFetch;
        case static_cast<std::uint16_t>(ApiKey::kMetadata):
            return ApiKey::kMetadata;
        case static_cast<std::uint16_t>(ApiKey::kCreateTopic):
            return ApiKey::kCreateTopic;
        case static_cast<std::uint16_t>(ApiKey::kCommitOffset):
            return ApiKey::kCommitOffset;
        case static_cast<std::uint16_t>(ApiKey::kFetchCommittedOffset):
            return ApiKey::kFetchCommittedOffset;
        case static_cast<std::uint16_t>(ApiKey::kJoinGroup):
            return ApiKey::kJoinGroup;
        case static_cast<std::uint16_t>(ApiKey::kSyncGroup):
            return ApiKey::kSyncGroup;
        case static_cast<std::uint16_t>(ApiKey::kHeartbeat):
            return ApiKey::kHeartbeat;
        case static_cast<std::uint16_t>(ApiKey::kLeaveGroup):
            return ApiKey::kLeaveGroup;
    }
    return Status::InvalidArgument("unknown api key");
}

Result<ProtocolErrorCode> ParseProtocolErrorCode(std::uint16_t value) {
    switch (value) {
        case static_cast<std::uint16_t>(ProtocolErrorCode::kNone):
            return ProtocolErrorCode::kNone;
        case static_cast<std::uint16_t>(ProtocolErrorCode::kInvalidRequest):
            return ProtocolErrorCode::kInvalidRequest;
        case static_cast<std::uint16_t>(ProtocolErrorCode::kTopicNotFound):
            return ProtocolErrorCode::kTopicNotFound;
        case static_cast<std::uint16_t>(ProtocolErrorCode::kOffsetOutOfRange):
            return ProtocolErrorCode::kOffsetOutOfRange;
        case static_cast<std::uint16_t>(ProtocolErrorCode::kInternal):
            return ProtocolErrorCode::kInternal;
        case static_cast<std::uint16_t>(ProtocolErrorCode::kUnsupportedVersion):
            return ProtocolErrorCode::kUnsupportedVersion;
        case static_cast<std::uint16_t>(ProtocolErrorCode::kUnknownMember):
            return ProtocolErrorCode::kUnknownMember;
        case static_cast<std::uint16_t>(ProtocolErrorCode::kIllegalGeneration):
            return ProtocolErrorCode::kIllegalGeneration;
    }
    return Status::InvalidArgument("unknown protocol error code");
}

void AppendPartitionId(PartitionId partition_id, std::vector<std::byte>& output) {
    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(partition_id), output);
}

PartitionId DecodePartitionId(std::uint32_t value) {
    if (value <= static_cast<std::uint32_t>(std::numeric_limits<PartitionId>::max())) {
        return static_cast<PartitionId>(value);
    }
    const auto signed_value = static_cast<std::int64_t>(value) -
                              (static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max()) + 1);
    return static_cast<PartitionId>(signed_value);
}

Status AppendGenerationId(std::int32_t generation_id, std::vector<std::byte>& output) {
    if (generation_id < 0) {
        return Status::InvalidArgument("generation_id must not be negative");
    }
    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(generation_id), output);
    return Status::Ok();
}

Result<std::int32_t> DecodeGenerationId(std::uint32_t value) {
    if (value > static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max())) {
        return Status::InvalidArgument("generation_id is too large");
    }
    return static_cast<std::int32_t>(value);
}

Status ValidateFrame(std::span<const std::byte> frame) {
    if (frame.size() < kFrameHeaderBytes) {
        return Status::InvalidArgument("protocol frame header is truncated");
    }

    auto total_len = ReadBigEndian<std::uint32_t>(frame, 0);
    if (!total_len.ok()) {
        return total_len.status();
    }
    if (total_len.value() < kFrameHeaderBytes) {
        return Status::InvalidArgument("protocol frame length is smaller than header");
    }
    if (total_len.value() > kMaxFrameBytes) {
        return Status::InvalidArgument("protocol frame length exceeds max frame size");
    }
    if (total_len.value() != frame.size()) {
        return Status::InvalidArgument("protocol frame length does not match buffer size");
    }

    return Status::Ok();
}

Result<std::vector<std::byte>> EncodeProduceBody(const ProduceRequest& request) {
    if (request.records.empty()) {
        return Status::InvalidArgument("produce request must contain at least one record");
    }
    if (request.records.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("produce request contains too many records");
    }

    std::vector<std::byte> body;
    Status status = AppendString(request.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendPartitionId(request.partition_id, body);
    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(request.records.size()), body);

    for (const ProtocolRecord& record : request.records) {
        AppendBigEndian<std::uint64_t>(record.timestamp, body);
        status = AppendString(record.key, body);
        if (!status.ok()) {
            return status;
        }
        status = AppendString(record.value, body);
        if (!status.ok()) {
            return status;
        }
    }
    return body;
}

Result<std::vector<std::byte>> EncodeFetchBody(const FetchRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendPartitionId(request.partition_id, body);
    AppendBigEndian<std::uint64_t>(static_cast<std::uint64_t>(request.offset), body);
    AppendBigEndian<std::uint32_t>(request.max_bytes, body);
    return body;
}

Result<std::vector<std::byte>> EncodeMetadataBody(const MetadataRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.topic, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeCreateTopicBody(const CreateTopicRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendBigEndian<std::uint32_t>(request.partition_count, body);
    return body;
}

Result<std::vector<std::byte>> EncodeCommitOffsetBody(const CommitOffsetRequest& request) {
    if (request.offset < 0) {
        return Status::InvalidArgument("commit offset request offset must not be negative");
    }

    std::vector<std::byte> body;
    Status status = AppendString(request.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(request.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendPartitionId(request.partition_id, body);
    AppendBigEndian<std::uint64_t>(static_cast<std::uint64_t>(request.offset), body);
    return body;
}

Result<std::vector<std::byte>> EncodeFetchCommittedOffsetBody(
    const FetchCommittedOffsetRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(request.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendPartitionId(request.partition_id, body);
    return body;
}

Result<std::vector<std::byte>> EncodeJoinGroupBody(const JoinGroupRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(request.member_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(request.topic, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeSyncGroupBody(const SyncGroupRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(request.member_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendGenerationId(request.generation_id, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeHeartbeatBody(const HeartbeatRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(request.member_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendGenerationId(request.generation_id, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeLeaveGroupBody(const LeaveGroupRequest& request) {
    std::vector<std::byte> body;
    Status status = AppendString(request.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(request.member_id, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Status AppendProtocolRecord(const ProtocolRecord& record, std::vector<std::byte>& body) {
    AppendBigEndian<std::uint64_t>(record.timestamp, body);
    Status status = AppendString(record.key, body);
    if (!status.ok()) {
        return status;
    }
    return AppendString(record.value, body);
}

Result<ProtocolRecord> DecodeProtocolRecord(BodyReader& reader) {
    auto timestamp = reader.ReadUInt<std::uint64_t>();
    if (!timestamp.ok()) {
        return timestamp.status();
    }
    auto key = reader.ReadString();
    if (!key.ok()) {
        return key.status();
    }
    auto value = reader.ReadString();
    if (!value.ok()) {
        return value.status();
    }

    ProtocolRecord record;
    record.timestamp = timestamp.value();
    record.key = std::move(key).value();
    record.value = std::move(value).value();
    return record;
}

// Parse xxBody(Produce, Fetch..)
Result<ProduceRequest> DecodeProduceBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition = reader.ReadUInt<std::uint32_t>();
    if (!partition.ok()) {
        return partition.status();
    }
    auto record_count = reader.ReadUInt<std::uint32_t>();
    if (!record_count.ok()) {
        return record_count.status();
    }
    if (record_count.value() == 0) {
        return Status::InvalidArgument("produce request record_count is zero");
    }

    ProduceRequest request;
    request.topic = std::move(topic).value();
    request.partition_id = DecodePartitionId(partition.value());
    request.records.reserve(record_count.value());

    for (std::uint32_t i = 0; i < record_count.value(); ++i) {
        auto record = DecodeProtocolRecord(reader);
        if (!record.ok()) {
            return record.status();
        }
        request.records.push_back(std::move(record).value());
    }

    if (!reader.consumed()) {
        return Status::InvalidArgument("produce request has trailing bytes");
    }
    return request;
}

Result<FetchRequest> DecodeFetchBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition = reader.ReadUInt<std::uint32_t>();
    if (!partition.ok()) {
        return partition.status();
    }
    auto offset = reader.ReadUInt<std::uint64_t>();
    if (!offset.ok()) {
        return offset.status();
    }
    auto max_bytes = reader.ReadUInt<std::uint32_t>();
    if (!max_bytes.ok()) {
        return max_bytes.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("fetch request has trailing bytes");
    }

    FetchRequest request;
    request.topic = std::move(topic).value();
    request.partition_id = DecodePartitionId(partition.value());
    request.offset = static_cast<Offset>(offset.value());
    request.max_bytes = max_bytes.value();
    return request;
}

Result<MetadataRequest> DecodeMetadataBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("metadata request has trailing bytes");
    }

    MetadataRequest request;
    request.topic = std::move(topic).value();
    return request;
}

Result<CreateTopicRequest> DecodeCreateTopicBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition_count = reader.ReadUInt<std::uint32_t>();
    if (!partition_count.ok()) {
        return partition_count.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("create topic request has trailing bytes");
    }

    CreateTopicRequest request;
    request.topic = std::move(topic).value();
    request.partition_count = partition_count.value();
    return request;
}

Result<CommitOffsetRequest> DecodeCommitOffsetBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition = reader.ReadUInt<std::uint32_t>();
    if (!partition.ok()) {
        return partition.status();
    }
    auto offset = reader.ReadUInt<std::uint64_t>();
    if (!offset.ok()) {
        return offset.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("commit offset request has trailing bytes");
    }

    CommitOffsetRequest request;
    request.group_id = std::move(group_id).value();
    request.topic = std::move(topic).value();
    request.partition_id = DecodePartitionId(partition.value());
    request.offset = static_cast<Offset>(offset.value());
    return request;
}

Result<FetchCommittedOffsetRequest> DecodeFetchCommittedOffsetBody(
    std::span<const std::byte> body) {
    BodyReader reader(body);
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition = reader.ReadUInt<std::uint32_t>();
    if (!partition.ok()) {
        return partition.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("fetch committed offset request has trailing bytes");
    }

    FetchCommittedOffsetRequest request;
    request.group_id = std::move(group_id).value();
    request.topic = std::move(topic).value();
    request.partition_id = DecodePartitionId(partition.value());
    return request;
}

Result<JoinGroupRequest> DecodeJoinGroupBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("join group request has trailing bytes");
    }

    JoinGroupRequest request;
    request.group_id = std::move(group_id).value();
    request.member_id = std::move(member_id).value();
    request.topic = std::move(topic).value();
    return request;
}

Result<SyncGroupRequest> DecodeSyncGroupBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    auto generation = reader.ReadUInt<std::uint32_t>();
    if (!generation.ok()) {
        return generation.status();
    }
    auto generation_id = DecodeGenerationId(generation.value());
    if (!generation_id.ok()) {
        return generation_id.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("sync group request has trailing bytes");
    }

    SyncGroupRequest request;
    request.group_id = std::move(group_id).value();
    request.member_id = std::move(member_id).value();
    request.generation_id = generation_id.value();
    return request;
}

Result<HeartbeatRequest> DecodeHeartbeatBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    auto generation = reader.ReadUInt<std::uint32_t>();
    if (!generation.ok()) {
        return generation.status();
    }
    auto generation_id = DecodeGenerationId(generation.value());
    if (!generation_id.ok()) {
        return generation_id.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("heartbeat request has trailing bytes");
    }

    HeartbeatRequest request;
    request.group_id = std::move(group_id).value();
    request.member_id = std::move(member_id).value();
    request.generation_id = generation_id.value();
    return request;
}

Result<LeaveGroupRequest> DecodeLeaveGroupBody(std::span<const std::byte> body) {
    BodyReader reader(body);
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("leave group request has trailing bytes");
    }

    LeaveGroupRequest request;
    request.group_id = std::move(group_id).value();
    request.member_id = std::move(member_id).value();
    return request;
}

Result<std::vector<std::byte>> EncodeRequestBody(const RequestEnvelope& request) {
    switch (request.api_key) {
        case ApiKey::kProduce:
            if (!std::holds_alternative<ProduceRequest>(request.body)) {
                return Status::InvalidArgument("produce request envelope has wrong body type");
            }
            return EncodeProduceBody(std::get<ProduceRequest>(request.body));
        case ApiKey::kFetch:
            if (!std::holds_alternative<FetchRequest>(request.body)) {
                return Status::InvalidArgument("fetch request envelope has wrong body type");
            }
            return EncodeFetchBody(std::get<FetchRequest>(request.body));
        case ApiKey::kMetadata:
            if (!std::holds_alternative<MetadataRequest>(request.body)) {
                return Status::InvalidArgument("metadata request envelope has wrong body type");
            }
            return EncodeMetadataBody(std::get<MetadataRequest>(request.body));
        case ApiKey::kCreateTopic:
            if (!std::holds_alternative<CreateTopicRequest>(request.body)) {
                return Status::InvalidArgument("create topic request envelope has wrong body type");
            }
            return EncodeCreateTopicBody(std::get<CreateTopicRequest>(request.body));
        case ApiKey::kCommitOffset:
            if (!std::holds_alternative<CommitOffsetRequest>(request.body)) {
                return Status::InvalidArgument("commit offset request envelope has wrong body type");
            }
            return EncodeCommitOffsetBody(std::get<CommitOffsetRequest>(request.body));
        case ApiKey::kFetchCommittedOffset:
            if (!std::holds_alternative<FetchCommittedOffsetRequest>(request.body)) {
                return Status::InvalidArgument(
                    "fetch committed offset request envelope has wrong body type");
            }
            return EncodeFetchCommittedOffsetBody(
                std::get<FetchCommittedOffsetRequest>(request.body));
        case ApiKey::kJoinGroup:
            if (!std::holds_alternative<JoinGroupRequest>(request.body)) {
                return Status::InvalidArgument("join group request envelope has wrong body type");
            }
            return EncodeJoinGroupBody(std::get<JoinGroupRequest>(request.body));
        case ApiKey::kSyncGroup:
            if (!std::holds_alternative<SyncGroupRequest>(request.body)) {
                return Status::InvalidArgument("sync group request envelope has wrong body type");
            }
            return EncodeSyncGroupBody(std::get<SyncGroupRequest>(request.body));
        case ApiKey::kHeartbeat:
            if (!std::holds_alternative<HeartbeatRequest>(request.body)) {
                return Status::InvalidArgument("heartbeat request envelope has wrong body type");
            }
            return EncodeHeartbeatBody(std::get<HeartbeatRequest>(request.body));
        case ApiKey::kLeaveGroup:
            if (!std::holds_alternative<LeaveGroupRequest>(request.body)) {
                return Status::InvalidArgument("leave group request envelope has wrong body type");
            }
            return EncodeLeaveGroupBody(std::get<LeaveGroupRequest>(request.body));
    }
    return Status::InvalidArgument("unknown api key");
}

Result<std::vector<std::byte>> EncodeProduceResponseBody(const ProduceResponse& response) {
    if (response.partition_id < 0) {
        return Status::InvalidArgument("produce response partition_id must not be negative");
    }
    if (response.base_offset < 0) {
        return Status::InvalidArgument("produce response base_offset must not be negative");
    }

    std::vector<std::byte> body;
    AppendPartitionId(response.partition_id, body);
    AppendBigEndian<std::uint64_t>(static_cast<std::uint64_t>(response.base_offset), body);
    AppendBigEndian<std::uint32_t>(response.record_count, body);
    return body;
}

Result<std::vector<std::byte>> EncodeFetchResponseBody(const FetchResponse& response) {
    if (response.base_offset < 0) {
        return Status::InvalidArgument("fetch response base_offset must not be negative");
    }
    if (response.high_watermark < 0) {
        return Status::InvalidArgument("fetch response high_watermark must not be negative");
    }
    if (response.records.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("fetch response contains too many records");
    }

    std::vector<std::byte> body;
    AppendBigEndian<std::uint64_t>(static_cast<std::uint64_t>(response.base_offset), body);
    AppendBigEndian<std::uint64_t>(static_cast<std::uint64_t>(response.high_watermark), body);
    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(response.records.size()), body);
    for (const ProtocolRecord& record : response.records) {
        Status status = AppendProtocolRecord(record, body);
        if (!status.ok()) {
            return status;
        }
    }
    return body;
}

Result<std::vector<std::byte>> EncodeMetadataResponseBody(const MetadataResponse& response) {
    if (response.topics.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("metadata response contains too many topics");
    }

    std::vector<std::byte> body;
    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(response.topics.size()), body);
    for (const TopicMetadata& topic : response.topics) {
        Status status = AppendString(topic.topic, body);
        if (!status.ok()) {
            return status;
        }
        AppendBigEndian<std::uint32_t>(topic.partition_count, body);
    }
    return body;
}

Result<std::vector<std::byte>> EncodeCreateTopicResponseBody(
    const CreateTopicResponse& response) {
    std::vector<std::byte> body;
    Status status = AppendString(response.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendBigEndian<std::uint32_t>(response.partition_count, body);
    return body;
}

Result<std::vector<std::byte>> EncodeCommitOffsetResponseBody(
    const CommitOffsetResponse& response) {
    if (response.partition_id < 0) {
        return Status::InvalidArgument("commit offset response partition_id must not be negative");
    }
    if (response.offset < 0) {
        return Status::InvalidArgument("commit offset response offset must not be negative");
    }

    std::vector<std::byte> body;
    Status status = AppendString(response.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(response.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendPartitionId(response.partition_id, body);
    AppendBigEndian<std::uint64_t>(static_cast<std::uint64_t>(response.offset), body);
    return body;
}

Result<std::vector<std::byte>> EncodeFetchCommittedOffsetResponseBody(
    const FetchCommittedOffsetResponse& response) {
    if (response.partition_id < 0) {
        return Status::InvalidArgument(
            "fetch committed offset response partition_id must not be negative");
    }
    if (response.offset < 0) {
        return Status::InvalidArgument("fetch committed offset response offset must not be negative");
    }

    std::vector<std::byte> body;
    Status status = AppendString(response.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(response.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendPartitionId(response.partition_id, body);
    AppendBigEndian<std::uint32_t>(response.committed ? 1U : 0U, body);
    AppendBigEndian<std::uint64_t>(static_cast<std::uint64_t>(response.offset), body);
    return body;
}

Status AppendPartitionAssignment(const PartitionAssignment& assignment,
                                 std::vector<std::byte>& body) {
    if (assignment.partition_ids.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("assignment contains too many partitions");
    }

    Status status = AppendString(assignment.topic, body);
    if (!status.ok()) {
        return status;
    }
    AppendBigEndian<std::uint32_t>(
        static_cast<std::uint32_t>(assignment.partition_ids.size()), body);
    for (PartitionId partition_id : assignment.partition_ids) {
        if (partition_id < 0) {
            return Status::InvalidArgument("assignment partition_id must not be negative");
        }
        AppendPartitionId(partition_id, body);
    }
    return Status::Ok();
}

Result<std::vector<std::byte>> EncodeJoinGroupResponseBody(
    const JoinGroupResponse& response) {
    std::vector<std::byte> body;
    Status status = AppendString(response.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(response.member_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendGenerationId(response.generation_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(response.leader_id, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeSyncGroupResponseBody(
    const SyncGroupResponse& response) {
    std::vector<std::byte> body;
    Status status = AppendString(response.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(response.member_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendGenerationId(response.generation_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendPartitionAssignment(response.assignment, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeHeartbeatResponseBody(
    const HeartbeatResponse& response) {
    std::vector<std::byte> body;
    Status status = AppendString(response.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(response.member_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendGenerationId(response.generation_id, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeLeaveGroupResponseBody(
    const LeaveGroupResponse& response) {
    std::vector<std::byte> body;
    Status status = AppendString(response.group_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendString(response.member_id, body);
    if (!status.ok()) {
        return status;
    }
    status = AppendGenerationId(response.generation_id, body);
    if (!status.ok()) {
        return status;
    }
    return body;
}

Result<std::vector<std::byte>> EncodeResponseBody(const ResponseEnvelope& response) {
    // The api_key selects the response schema. This keeps callers from sending
    // a MetadataResponse inside a Produce frame by mistake.
    switch (response.api_key) {
        case ApiKey::kProduce:
            if (!std::holds_alternative<ProduceResponse>(response.body)) {
                return Status::InvalidArgument("produce response envelope has wrong body type");
            }
            return EncodeProduceResponseBody(std::get<ProduceResponse>(response.body));
        case ApiKey::kFetch:
            if (!std::holds_alternative<FetchResponse>(response.body)) {
                return Status::InvalidArgument("fetch response envelope has wrong body type");
            }
            return EncodeFetchResponseBody(std::get<FetchResponse>(response.body));
        case ApiKey::kMetadata:
            if (!std::holds_alternative<MetadataResponse>(response.body)) {
                return Status::InvalidArgument("metadata response envelope has wrong body type");
            }
            return EncodeMetadataResponseBody(std::get<MetadataResponse>(response.body));
        case ApiKey::kCreateTopic:
            if (!std::holds_alternative<CreateTopicResponse>(response.body)) {
                return Status::InvalidArgument("create topic response envelope has wrong body type");
            }
            return EncodeCreateTopicResponseBody(std::get<CreateTopicResponse>(response.body));
        case ApiKey::kCommitOffset:
            if (!std::holds_alternative<CommitOffsetResponse>(response.body)) {
                return Status::InvalidArgument("commit offset response envelope has wrong body type");
            }
            return EncodeCommitOffsetResponseBody(std::get<CommitOffsetResponse>(response.body));
        case ApiKey::kFetchCommittedOffset:
            if (!std::holds_alternative<FetchCommittedOffsetResponse>(response.body)) {
                return Status::InvalidArgument(
                    "fetch committed offset response envelope has wrong body type");
            }
            return EncodeFetchCommittedOffsetResponseBody(
                std::get<FetchCommittedOffsetResponse>(response.body));
        case ApiKey::kJoinGroup:
            if (!std::holds_alternative<JoinGroupResponse>(response.body)) {
                return Status::InvalidArgument("join group response envelope has wrong body type");
            }
            return EncodeJoinGroupResponseBody(std::get<JoinGroupResponse>(response.body));
        case ApiKey::kSyncGroup:
            if (!std::holds_alternative<SyncGroupResponse>(response.body)) {
                return Status::InvalidArgument("sync group response envelope has wrong body type");
            }
            return EncodeSyncGroupResponseBody(std::get<SyncGroupResponse>(response.body));
        case ApiKey::kHeartbeat:
            if (!std::holds_alternative<HeartbeatResponse>(response.body)) {
                return Status::InvalidArgument("heartbeat response envelope has wrong body type");
            }
            return EncodeHeartbeatResponseBody(std::get<HeartbeatResponse>(response.body));
        case ApiKey::kLeaveGroup:
            if (!std::holds_alternative<LeaveGroupResponse>(response.body)) {
                return Status::InvalidArgument("leave group response envelope has wrong body type");
            }
            return EncodeLeaveGroupResponseBody(std::get<LeaveGroupResponse>(response.body));
    }
    return Status::InvalidArgument("unknown api key");
}

ResponseBody EmptyResponseBodyFor(ApiKey api_key) {
    // Error responses have no success payload, but keeping the matching variant
    // alternative makes decoded responses easier to inspect in tests and clients.
    switch (api_key) {
        case ApiKey::kProduce:
            return ResponseBody{std::in_place_type<ProduceResponse>};
        case ApiKey::kFetch:
            return ResponseBody{std::in_place_type<FetchResponse>};
        case ApiKey::kMetadata:
            return ResponseBody{std::in_place_type<MetadataResponse>};
        case ApiKey::kCreateTopic:
            return ResponseBody{std::in_place_type<CreateTopicResponse>};
        case ApiKey::kCommitOffset:
            return ResponseBody{std::in_place_type<CommitOffsetResponse>};
        case ApiKey::kFetchCommittedOffset:
            return ResponseBody{std::in_place_type<FetchCommittedOffsetResponse>};
        case ApiKey::kJoinGroup:
            return ResponseBody{std::in_place_type<JoinGroupResponse>};
        case ApiKey::kSyncGroup:
            return ResponseBody{std::in_place_type<SyncGroupResponse>};
        case ApiKey::kHeartbeat:
            return ResponseBody{std::in_place_type<HeartbeatResponse>};
        case ApiKey::kLeaveGroup:
            return ResponseBody{std::in_place_type<LeaveGroupResponse>};
    }
    return ResponseBody{std::in_place_type<ProduceResponse>};
}

Result<ProduceResponse> DecodeProduceResponseBody(BodyReader& reader) {
    auto partition_id = reader.ReadUInt<std::uint32_t>();
    if (!partition_id.ok()) {
        return partition_id.status();
    }
    auto base_offset = reader.ReadUInt<std::uint64_t>();
    if (!base_offset.ok()) {
        return base_offset.status();
    }
    auto record_count = reader.ReadUInt<std::uint32_t>();
    if (!record_count.ok()) {
        return record_count.status();
    }

    ProduceResponse response;
    response.partition_id = DecodePartitionId(partition_id.value());
    response.base_offset = static_cast<Offset>(base_offset.value());
    response.record_count = record_count.value();
    return response;
}

Result<FetchResponse> DecodeFetchResponseBody(BodyReader& reader) {
    auto base_offset = reader.ReadUInt<std::uint64_t>();
    if (!base_offset.ok()) {
        return base_offset.status();
    }
    auto high_watermark = reader.ReadUInt<std::uint64_t>();
    if (!high_watermark.ok()) {
        return high_watermark.status();
    }
    auto record_count = reader.ReadUInt<std::uint32_t>();
    if (!record_count.ok()) {
        return record_count.status();
    }

    FetchResponse response;
    response.base_offset = static_cast<Offset>(base_offset.value());
    response.high_watermark = static_cast<Offset>(high_watermark.value());
    response.records.reserve(record_count.value());
    for (std::uint32_t i = 0; i < record_count.value(); ++i) {
        auto record = DecodeProtocolRecord(reader);
        if (!record.ok()) {
            return record.status();
        }
        response.records.push_back(std::move(record).value());
    }
    return response;
}

Result<MetadataResponse> DecodeMetadataResponseBody(BodyReader& reader) {
    auto topic_count = reader.ReadUInt<std::uint32_t>();
    if (!topic_count.ok()) {
        return topic_count.status();
    }

    MetadataResponse response;
    response.topics.reserve(topic_count.value());
    for (std::uint32_t i = 0; i < topic_count.value(); ++i) {
        auto topic = reader.ReadString();
        if (!topic.ok()) {
            return topic.status();
        }
        auto partition_count = reader.ReadUInt<std::uint32_t>();
        if (!partition_count.ok()) {
            return partition_count.status();
        }
        response.topics.push_back(TopicMetadata{std::move(topic).value(), partition_count.value()});
    }
    return response;
}

Result<CreateTopicResponse> DecodeCreateTopicResponseBody(BodyReader& reader) {
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition_count = reader.ReadUInt<std::uint32_t>();
    if (!partition_count.ok()) {
        return partition_count.status();
    }

    CreateTopicResponse response;
    response.topic = std::move(topic).value();
    response.partition_count = partition_count.value();
    return response;
}

Result<CommitOffsetResponse> DecodeCommitOffsetResponseBody(BodyReader& reader) {
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition = reader.ReadUInt<std::uint32_t>();
    if (!partition.ok()) {
        return partition.status();
    }
    auto offset = reader.ReadUInt<std::uint64_t>();
    if (!offset.ok()) {
        return offset.status();
    }

    CommitOffsetResponse response;
    response.group_id = std::move(group_id).value();
    response.topic = std::move(topic).value();
    response.partition_id = DecodePartitionId(partition.value());
    response.offset = static_cast<Offset>(offset.value());
    return response;
}

Result<FetchCommittedOffsetResponse> DecodeFetchCommittedOffsetResponseBody(
    BodyReader& reader) {
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition = reader.ReadUInt<std::uint32_t>();
    if (!partition.ok()) {
        return partition.status();
    }
    auto committed = reader.ReadUInt<std::uint32_t>();
    if (!committed.ok()) {
        return committed.status();
    }
    if (committed.value() > 1) {
        return Status::InvalidArgument("fetch committed offset response has invalid flag");
    }
    auto offset = reader.ReadUInt<std::uint64_t>();
    if (!offset.ok()) {
        return offset.status();
    }

    FetchCommittedOffsetResponse response;
    response.group_id = std::move(group_id).value();
    response.topic = std::move(topic).value();
    response.partition_id = DecodePartitionId(partition.value());
    response.committed = committed.value() == 1;
    response.offset = static_cast<Offset>(offset.value());
    return response;
}

Result<PartitionAssignment> DecodePartitionAssignment(BodyReader& reader) {
    auto topic = reader.ReadString();
    if (!topic.ok()) {
        return topic.status();
    }
    auto partition_count = reader.ReadUInt<std::uint32_t>();
    if (!partition_count.ok()) {
        return partition_count.status();
    }

    PartitionAssignment assignment;
    assignment.topic = std::move(topic).value();
    assignment.partition_ids.reserve(partition_count.value());
    for (std::uint32_t i = 0; i < partition_count.value(); ++i) {
        auto partition = reader.ReadUInt<std::uint32_t>();
        if (!partition.ok()) {
            return partition.status();
        }
        const PartitionId partition_id = DecodePartitionId(partition.value());
        if (partition_id < 0) {
            return Status::InvalidArgument("assignment partition_id must not be negative");
        }
        assignment.partition_ids.push_back(partition_id);
    }
    return assignment;
}

Result<JoinGroupResponse> DecodeJoinGroupResponseBody(BodyReader& reader) {
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    auto generation = reader.ReadUInt<std::uint32_t>();
    if (!generation.ok()) {
        return generation.status();
    }
    auto generation_id = DecodeGenerationId(generation.value());
    if (!generation_id.ok()) {
        return generation_id.status();
    }
    auto leader_id = reader.ReadString();
    if (!leader_id.ok()) {
        return leader_id.status();
    }

    JoinGroupResponse response;
    response.group_id = std::move(group_id).value();
    response.member_id = std::move(member_id).value();
    response.generation_id = generation_id.value();
    response.leader_id = std::move(leader_id).value();
    return response;
}

Result<SyncGroupResponse> DecodeSyncGroupResponseBody(BodyReader& reader) {
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    auto generation = reader.ReadUInt<std::uint32_t>();
    if (!generation.ok()) {
        return generation.status();
    }
    auto generation_id = DecodeGenerationId(generation.value());
    if (!generation_id.ok()) {
        return generation_id.status();
    }
    auto assignment = DecodePartitionAssignment(reader);
    if (!assignment.ok()) {
        return assignment.status();
    }

    SyncGroupResponse response;
    response.group_id = std::move(group_id).value();
    response.member_id = std::move(member_id).value();
    response.generation_id = generation_id.value();
    response.assignment = std::move(assignment).value();
    return response;
}

Result<HeartbeatResponse> DecodeHeartbeatResponseBody(BodyReader& reader) {
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    auto generation = reader.ReadUInt<std::uint32_t>();
    if (!generation.ok()) {
        return generation.status();
    }
    auto generation_id = DecodeGenerationId(generation.value());
    if (!generation_id.ok()) {
        return generation_id.status();
    }

    HeartbeatResponse response;
    response.group_id = std::move(group_id).value();
    response.member_id = std::move(member_id).value();
    response.generation_id = generation_id.value();
    return response;
}

Result<LeaveGroupResponse> DecodeLeaveGroupResponseBody(BodyReader& reader) {
    auto group_id = reader.ReadString();
    if (!group_id.ok()) {
        return group_id.status();
    }
    auto member_id = reader.ReadString();
    if (!member_id.ok()) {
        return member_id.status();
    }
    auto generation = reader.ReadUInt<std::uint32_t>();
    if (!generation.ok()) {
        return generation.status();
    }
    auto generation_id = DecodeGenerationId(generation.value());
    if (!generation_id.ok()) {
        return generation_id.status();
    }

    LeaveGroupResponse response;
    response.group_id = std::move(group_id).value();
    response.member_id = std::move(member_id).value();
    response.generation_id = generation_id.value();
    return response;
}

Result<ResponseBody> DecodeResponseBody(ApiKey api_key, BodyReader& reader) {
    switch (api_key) {
        case ApiKey::kProduce: {
            auto response = DecodeProduceResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<ProduceResponse>, std::move(response).value()};
        }
        case ApiKey::kFetch: {
            auto response = DecodeFetchResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<FetchResponse>, std::move(response).value()};
        }
        case ApiKey::kMetadata: {
            auto response = DecodeMetadataResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<MetadataResponse>, std::move(response).value()};
        }
        case ApiKey::kCreateTopic: {
            auto response = DecodeCreateTopicResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<CreateTopicResponse>, std::move(response).value()};
        }
        case ApiKey::kCommitOffset: {
            auto response = DecodeCommitOffsetResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<CommitOffsetResponse>,
                                std::move(response).value()};
        }
        case ApiKey::kFetchCommittedOffset: {
            auto response = DecodeFetchCommittedOffsetResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<FetchCommittedOffsetResponse>,
                                std::move(response).value()};
        }
        case ApiKey::kJoinGroup: {
            auto response = DecodeJoinGroupResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<JoinGroupResponse>,
                                std::move(response).value()};
        }
        case ApiKey::kSyncGroup: {
            auto response = DecodeSyncGroupResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<SyncGroupResponse>,
                                std::move(response).value()};
        }
        case ApiKey::kHeartbeat: {
            auto response = DecodeHeartbeatResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<HeartbeatResponse>,
                                std::move(response).value()};
        }
        case ApiKey::kLeaveGroup: {
            auto response = DecodeLeaveGroupResponseBody(reader);
            if (!response.ok()) {
                return response.status();
            }
            return ResponseBody{std::in_place_type<LeaveGroupResponse>,
                                std::move(response).value()};
        }
    }
    return Status::InvalidArgument("unknown api key");
}

// Construct a RequestBody(std::variant<>) from the body byte stream accoding to the api_key.
Result<RequestBody> DecodeRequestBody(ApiKey api_key, std::span<const std::byte> body) {
    switch (api_key) {
        case ApiKey::kProduce: {
            auto request = DecodeProduceBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<ProduceRequest>, std::move(request).value()};
        }
        case ApiKey::kFetch: {
            auto request = DecodeFetchBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<FetchRequest>, std::move(request).value()};
        }
        case ApiKey::kMetadata: {
            auto request = DecodeMetadataBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<MetadataRequest>, std::move(request).value()};
        }
        case ApiKey::kCreateTopic: {
            auto request = DecodeCreateTopicBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<CreateTopicRequest>, std::move(request).value()};
        }
        case ApiKey::kCommitOffset: {
            auto request = DecodeCommitOffsetBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<CommitOffsetRequest>,
                               std::move(request).value()};
        }
        case ApiKey::kFetchCommittedOffset: {
            auto request = DecodeFetchCommittedOffsetBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<FetchCommittedOffsetRequest>,
                               std::move(request).value()};
        }
        case ApiKey::kJoinGroup: {
            auto request = DecodeJoinGroupBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<JoinGroupRequest>,
                               std::move(request).value()};
        }
        case ApiKey::kSyncGroup: {
            auto request = DecodeSyncGroupBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<SyncGroupRequest>,
                               std::move(request).value()};
        }
        case ApiKey::kHeartbeat: {
            auto request = DecodeHeartbeatBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<HeartbeatRequest>,
                               std::move(request).value()};
        }
        case ApiKey::kLeaveGroup: {
            auto request = DecodeLeaveGroupBody(body);
            if (!request.ok()) {
                return request.status();
            }
            return RequestBody{std::in_place_type<LeaveGroupRequest>,
                               std::move(request).value()};
        }
    }
    return Status::InvalidArgument("unknown api key");
}

Result<std::uint32_t> PeekFrameLength(std::span<const std::byte> data) {
    if (data.size() < sizeof(std::uint32_t)) {
        return Status::InvalidArgument("protocol frame length is incomplete");
    }
    return ReadBigEndian<std::uint32_t>(data, 0);
}

}  // namespace

std::string_view ApiKeyName(ApiKey api_key) {
    switch (api_key) {
        case ApiKey::kProduce:
            return "Produce";
        case ApiKey::kFetch:
            return "Fetch";
        case ApiKey::kMetadata:
            return "Metadata";
        case ApiKey::kCreateTopic:
            return "CreateTopic";
        case ApiKey::kCommitOffset:
            return "CommitOffset";
        case ApiKey::kFetchCommittedOffset:
            return "FetchCommittedOffset";
        case ApiKey::kJoinGroup:
            return "JoinGroup";
        case ApiKey::kSyncGroup:
            return "SyncGroup";
        case ApiKey::kHeartbeat:
            return "Heartbeat";
        case ApiKey::kLeaveGroup:
            return "LeaveGroup";
    }
    return "Unknown";
}

std::string_view ProtocolErrorCodeName(ProtocolErrorCode code) {
    switch (code) {
        case ProtocolErrorCode::kNone:
            return "None";
        case ProtocolErrorCode::kInvalidRequest:
            return "InvalidRequest";
        case ProtocolErrorCode::kTopicNotFound:
            return "TopicNotFound";
        case ProtocolErrorCode::kOffsetOutOfRange:
            return "OffsetOutOfRange";
        case ProtocolErrorCode::kInternal:
            return "Internal";
        case ProtocolErrorCode::kUnsupportedVersion:
            return "UnsupportedVersion";
        case ProtocolErrorCode::kUnknownMember:
            return "UnknownMember";
        case ProtocolErrorCode::kIllegalGeneration:
            return "IllegalGeneration";
    }
    return "Unknown";
}

Result<std::vector<std::byte>> EncodeRequest(const RequestEnvelope& request) {
    if (request.version != kProtocolVersion) {
        return Status::InvalidArgument("unsupported protocol version");
    }

    auto body = EncodeRequestBody(request);
    if (!body.ok()) {
        return body.status();
    }

    std::vector<std::byte> output;
    output.reserve(kFrameHeaderBytes + body.value().size());
    Status status =
        AppendFrameHeader(request.api_key, request.version, request.request_id, body.value(), output);
    if (!status.ok()) {
        return status;
    }
    AppendBytes(body.value(), output);
    return output;
}

// Decode the byte stream into request envelope.
// 00 01 00 02 00 03 00 04 00 05 -> RequestEnvelope structure.
Result<RequestEnvelope> DecodeRequest(std::span<const std::byte> frame) {
    Status status = ValidateFrame(frame);
    if (!status.ok()) {
        return status;
    }

    auto api_key_value = ReadBigEndian<std::uint16_t>(frame, sizeof(std::uint32_t));
    if (!api_key_value.ok()) {
        return api_key_value.status();
    }
    // Unpacking
    auto api_key = ParseApiKey(api_key_value.value());
    if (!api_key.ok()) {
        return api_key.status();
    }

    auto version = ReadBigEndian<std::uint16_t>(frame, sizeof(std::uint32_t) + sizeof(std::uint16_t));
    if (!version.ok()) {
        return version.status();
    }
    if (version.value() != kProtocolVersion) {
        return Status::InvalidArgument("unsupported protocol version");
    }

    auto request_id = ReadBigEndian<std::uint64_t>(frame, sizeof(std::uint32_t) + sizeof(std::uint16_t) + sizeof(std::uint16_t));
    if (!request_id.ok()) {
        return request_id.status();
    }

    std::span<const std::byte> body(frame.data() + kFrameHeaderBytes,
                                   frame.size() - kFrameHeaderBytes);
    auto request_body = DecodeRequestBody(api_key.value(), body);
    if (!request_body.ok()) {
        return request_body.status();
    }

    RequestEnvelope envelope;
    envelope.api_key = api_key.value();
    envelope.version = version.value();
    envelope.request_id = request_id.value();
    envelope.body = std::move(request_body).value();
    return envelope;
}

Result<std::vector<std::byte>> EncodeResponse(const ResponseEnvelope& response) {
    if (response.version != kProtocolVersion) {
        return Status::InvalidArgument("unsupported protocol version");
    }

    std::vector<std::byte> body;
    AppendBigEndian<std::uint16_t>(static_cast<std::uint16_t>(response.error.error_code), body);
    Status status = AppendString(response.error.message, body);
    if (!status.ok()) {
        return status;
    }

    // All responses start with the common error envelope. Only success appends
    // the api-specific typed payload.
    if (response.error.error_code == ProtocolErrorCode::kNone) {
        auto success_body = EncodeResponseBody(response);
        if (!success_body.ok()) {
            return success_body.status();
        }
        AppendBytes(success_body.value(), body);
    }

    std::vector<std::byte> output;
    output.reserve(kFrameHeaderBytes + body.size());
    status = AppendFrameHeader(response.api_key, response.version, response.request_id, body, output);
    if (!status.ok()) {
        return status;
    }
    AppendBytes(body, output);
    return output;
}

Result<std::vector<std::byte>> EncodeErrorResponse(ApiKey api_key, 
                                                   std::uint16_t version, 
                                                   std::uint64_t request_id, 
                                                   ProtocolErrorCode code,
                                                   std::string_view message) {
    if (version != kProtocolVersion) {
        return Status::InvalidArgument("unsupported protocol version");
    }

    std::vector<std::byte> body;
    AppendBigEndian<std::uint16_t>(static_cast<std::uint16_t>(code), body);
    Status status = AppendString(message, body);
    if (!status.ok()) {
        return status;
    }

    std::vector<std::byte> output;
    output.reserve(kFrameHeaderBytes + body.size());
    status = AppendFrameHeader(api_key, version, request_id, body, output);
    if (!status.ok()) {
        return status;
    }
    AppendBytes(body, output);
    return output;
}

Result<ResponseEnvelope> DecodeResponse(std::span<const std::byte> frame) {
    Status status = ValidateFrame(frame);
    if (!status.ok()) {
        return status;
    }

    auto api_key_value = ReadBigEndian<std::uint16_t>(frame, sizeof(std::uint32_t));
    if (!api_key_value.ok()) {
        return api_key_value.status();
    }
    auto api_key = ParseApiKey(api_key_value.value());
    if (!api_key.ok()) {
        return api_key.status();
    }

    auto version = ReadBigEndian<std::uint16_t>(
        frame, sizeof(std::uint32_t) + sizeof(std::uint16_t));
    if (!version.ok()) {
        return version.status();
    }
    if (version.value() != kProtocolVersion) {
        return Status::InvalidArgument("unsupported protocol version");
    }

    auto request_id = ReadBigEndian<std::uint64_t>(
        frame, sizeof(std::uint32_t) + sizeof(std::uint16_t) + sizeof(std::uint16_t));
    if (!request_id.ok()) {
        return request_id.status();
    }

    BodyReader reader(
        std::span<const std::byte>(frame.data() + kFrameHeaderBytes, frame.size() - kFrameHeaderBytes));
    auto error_code_value = reader.ReadUInt<std::uint16_t>();
    if (!error_code_value.ok()) {
        return error_code_value.status();
    }
    auto error_code = ParseProtocolErrorCode(error_code_value.value());
    if (!error_code.ok()) {
        return error_code.status();
    }
    auto message = reader.ReadString();
    if (!message.ok()) {
        return message.status();
    }

    ResponseEnvelope response;
    response.api_key = api_key.value();
    response.version = version.value();
    response.request_id = request_id.value();
    response.error.error_code = error_code.value();
    response.error.message = std::move(message).value();
    // The common error envelope has already been decoded. Success responses now
    // consume the remaining bytes as the api-specific payload.
    if (response.error.error_code == ProtocolErrorCode::kNone) {
        auto body = DecodeResponseBody(response.api_key, reader);
        if (!body.ok()) {
            return body.status();
        }
        response.body = std::move(body).value();
    } else {
        response.body = EmptyResponseBodyFor(response.api_key);
    }
    if (!reader.consumed()) {
        return Status::InvalidArgument("response has trailing bytes");
    }
    return response;
}

FrameDecoder::FrameDecoder(std::uint32_t max_frame_bytes)
    : max_frame_bytes_(std::max<std::uint32_t>(max_frame_bytes, kFrameHeaderBytes)) {}

Result<std::vector<RequestEnvelope>> FrameDecoder::Push(std::span<const std::byte> data) {
    buffer_.insert(buffer_.end(), data.begin(), data.end());

    std::vector<RequestEnvelope> requests;
    while (buffer_.size() >= sizeof(std::uint32_t)) {
        auto frame_len = PeekFrameLength(buffer_);
        if (!frame_len.ok()) {
            return frame_len.status();
        }
        if (frame_len.value() < kFrameHeaderBytes) {
            return Status::InvalidArgument("protocol frame length is smaller than header");
        }
        if (frame_len.value() > max_frame_bytes_) {
            return Status::InvalidArgument("protocol frame length exceeds max frame size");
        }
        if (buffer_.size() < frame_len.value()) {
            break;
        }

        std::span<const std::byte> frame(buffer_.data(), frame_len.value());
        auto request = DecodeRequest(frame);
        if (!request.ok()) {
            return request.status();
        }
        requests.push_back(std::move(request).value());
        buffer_.erase(buffer_.begin(), buffer_.begin() + frame_len.value());
    }

    return requests;
}

void FrameDecoder::Clear() { buffer_.clear(); }

std::size_t FrameDecoder::buffered_bytes() const { return buffer_.size(); }

}  // namespace logmq
