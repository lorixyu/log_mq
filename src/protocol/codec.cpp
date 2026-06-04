#include "logmq/protocol/codec.h"

#include <algorithm>
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
    }
    return Status::InvalidArgument("unknown protocol error code");
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
    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(request.partition_id), body);
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
    AppendBigEndian<std::uint32_t>(static_cast<std::uint32_t>(request.partition_id), body);
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
    request.partition_id = static_cast<PartitionId>(partition.value());
    request.records.reserve(record_count.value());

    for (std::uint32_t i = 0; i < record_count.value(); ++i) {
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
        request.records.push_back(std::move(record));
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
    request.partition_id = static_cast<PartitionId>(partition.value());
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
    if (!reader.consumed()) {
        return Status::InvalidArgument("response has trailing bytes");
    }

    ResponseEnvelope response;
    response.api_key = api_key.value();
    response.version = version.value();
    response.request_id = request_id.value();
    response.error.error_code = error_code.value();
    response.error.message = std::move(message).value();
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
