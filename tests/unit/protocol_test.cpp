#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "logmq/protocol/codec.h"

namespace logmq {
namespace {

std::span<const std::byte> AsBytes(const std::vector<std::byte>& bytes) {
    return {bytes.data(), bytes.size()};
}

RequestEnvelope MakeProduceEnvelope(std::uint64_t request_id) {
    ProduceRequest request;
    request.topic = "orders";
    request.partition_id = 2;
    request.records.push_back(ProtocolRecord{1'700'200'000, "key-1", "value-1"});
    request.records.push_back(ProtocolRecord{1'700'200'001, "key-2", "value-2"});

    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kProduce;
    envelope.version = kProtocolVersion;
    envelope.request_id = request_id;
    envelope.body = std::move(request);
    return envelope;
}

RequestEnvelope MakeFetchEnvelope(std::uint64_t request_id) {
    FetchRequest request;
    request.topic = "orders";
    request.partition_id = 2;
    request.offset = 42;
    request.max_bytes = 4096;

    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kFetch;
    envelope.version = kProtocolVersion;
    envelope.request_id = request_id;
    envelope.body = std::move(request);
    return envelope;
}

void SetBigEndian32(std::uint32_t value, std::vector<std::byte>& output) {
    output.push_back(static_cast<std::byte>((value >> 24U) & 0xFFU));
    output.push_back(static_cast<std::byte>((value >> 16U) & 0xFFU));
    output.push_back(static_cast<std::byte>((value >> 8U) & 0xFFU));
    output.push_back(static_cast<std::byte>(value & 0xFFU));
}

TEST(ProtocolCodecTest, EncodesAndDecodesSingleProduceFrame) {
    auto encoded = EncodeRequest(MakeProduceEnvelope(99));
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeRequest(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();

    EXPECT_EQ(decoded.value().api_key, ApiKey::kProduce);
    EXPECT_EQ(decoded.value().version, kProtocolVersion);
    EXPECT_EQ(decoded.value().request_id, 99U);

    ASSERT_TRUE(std::holds_alternative<ProduceRequest>(decoded.value().body));
    const auto& request = std::get<ProduceRequest>(decoded.value().body);
    EXPECT_EQ(request.topic, "orders");
    EXPECT_EQ(request.partition_id, 2);
    ASSERT_EQ(request.records.size(), 2U);
    EXPECT_EQ(request.records[0].key, "key-1");
    EXPECT_EQ(request.records[1].value, "value-2");
}

TEST(ProtocolCodecTest, EncodesAndDecodesAutoPartitionProduceFrame) {
    ProduceRequest request;
    request.topic = "orders";
    request.partition_id = kInvalidPartitionId;
    request.records.push_back(ProtocolRecord{1'700'200'000, "key-1", "value-1"});

    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kProduce;
    envelope.version = kProtocolVersion;
    envelope.request_id = 101;
    envelope.body = std::move(request);

    auto encoded = EncodeRequest(envelope);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeRequest(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_TRUE(std::holds_alternative<ProduceRequest>(decoded.value().body));
    const auto& decoded_request = std::get<ProduceRequest>(decoded.value().body);
    EXPECT_EQ(decoded_request.partition_id, kInvalidPartitionId);
}

TEST(ProtocolCodecTest, FrameDecoderKeepsPartialFrameBuffered) {
    auto encoded = EncodeRequest(MakeFetchEnvelope(100));
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    FrameDecoder decoder;
    auto first = decoder.Push(std::span<const std::byte>(encoded.value().data(), 7));
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_TRUE(first.value().empty());
    EXPECT_EQ(decoder.buffered_bytes(), 7U);

    auto second = decoder.Push(std::span<const std::byte>(
        encoded.value().data() + 7, encoded.value().size() - 7));
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    ASSERT_EQ(second.value().size(), 1U);

    const RequestEnvelope& envelope = second.value().front();
    EXPECT_EQ(envelope.api_key, ApiKey::kFetch);
    EXPECT_EQ(envelope.request_id, 100U);
    ASSERT_TRUE(std::holds_alternative<FetchRequest>(envelope.body));
    EXPECT_EQ(std::get<FetchRequest>(envelope.body).offset, 42);
    EXPECT_EQ(decoder.buffered_bytes(), 0U);
}

TEST(ProtocolCodecTest, FrameDecoderHandlesStickyFrames) {
    auto first = EncodeRequest(MakeFetchEnvelope(1));
    auto second = EncodeRequest(MakeProduceEnvelope(2));

    MetadataRequest metadata;
    metadata.topic = "orders";
    RequestEnvelope metadata_envelope;
    metadata_envelope.api_key = ApiKey::kMetadata;
    metadata_envelope.version = kProtocolVersion;
    metadata_envelope.request_id = 3;
    metadata_envelope.body = std::move(metadata);
    auto third = EncodeRequest(metadata_envelope);

    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    ASSERT_TRUE(third.ok()) << third.status().ToString();

    std::vector<std::byte> sticky;
    sticky.insert(sticky.end(), first.value().begin(), first.value().end());
    sticky.insert(sticky.end(), second.value().begin(), second.value().end());
    sticky.insert(sticky.end(), third.value().begin(), third.value().end());

    FrameDecoder decoder;
    auto decoded = decoder.Push(sticky);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_EQ(decoded.value().size(), 3U);
    EXPECT_EQ(decoded.value()[0].request_id, 1U);
    EXPECT_EQ(decoded.value()[1].request_id, 2U);
    EXPECT_EQ(decoded.value()[2].request_id, 3U);
    EXPECT_EQ(decoded.value()[2].api_key, ApiKey::kMetadata);
}

TEST(ProtocolCodecTest, FrameDecoderRejectsOversizedFrameLength) {
    std::vector<std::byte> bytes;
    SetBigEndian32(kMaxFrameBytes + 1, bytes);

    FrameDecoder decoder;
    auto decoded = decoder.Push(bytes);
    ASSERT_FALSE(decoded.ok());
    EXPECT_EQ(decoded.status().code(), ErrorCode::kInvalidArgument);
}

TEST(ProtocolCodecTest, DecodeRejectsUnsupportedVersion) {
    auto encoded = EncodeRequest(MakeFetchEnvelope(200));
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    encoded.value()[6] = std::byte{0x00};
    encoded.value()[7] = std::byte{0x02};

    auto decoded = DecodeRequest(AsBytes(encoded.value()));
    ASSERT_FALSE(decoded.ok());
    EXPECT_EQ(decoded.status().code(), ErrorCode::kInvalidArgument);
    EXPECT_EQ(decoded.status().message(), "unsupported protocol version");
}

TEST(ProtocolCodecTest, EncodesAndDecodesErrorResponse) {
    auto encoded = EncodeErrorResponse(ApiKey::kFetch, kProtocolVersion, 321,
                                       ProtocolErrorCode::kOffsetOutOfRange,
                                       "offset is out of range");
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeResponse(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();

    EXPECT_EQ(decoded.value().api_key, ApiKey::kFetch);
    EXPECT_EQ(decoded.value().request_id, 321U);
    EXPECT_EQ(decoded.value().error.error_code, ProtocolErrorCode::kOffsetOutOfRange);
    EXPECT_EQ(decoded.value().error.message, "offset is out of range");
    EXPECT_TRUE(std::holds_alternative<FetchResponse>(decoded.value().body));
}

TEST(ProtocolCodecTest, EncodesAndDecodesCreateTopicRequest) {
    CreateTopicRequest request;
    request.topic = "payments";
    request.partition_count = 6;

    RequestEnvelope envelope;
    envelope.api_key = ApiKey::kCreateTopic;
    envelope.version = kProtocolVersion;
    envelope.request_id = 44;
    envelope.body = std::move(request);

    auto encoded = EncodeRequest(envelope);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    auto decoded = DecodeRequest(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();

    ASSERT_TRUE(std::holds_alternative<CreateTopicRequest>(decoded.value().body));
    const auto& create_topic = std::get<CreateTopicRequest>(decoded.value().body);
    EXPECT_EQ(create_topic.topic, "payments");
    EXPECT_EQ(create_topic.partition_count, 6U);
}

TEST(ProtocolCodecTest, EncodesAndDecodesProduceSuccessResponse) {
    ResponseEnvelope response;
    response.api_key = ApiKey::kProduce;
    response.version = kProtocolVersion;
    response.request_id = 501;
    response.error = ErrorResponse{ProtocolErrorCode::kNone, ""};
    ProduceResponse produce;
    produce.partition_id = 2;
    produce.base_offset = 42;
    produce.record_count = 3;
    response.body = produce;

    auto encoded = EncodeResponse(response);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeResponse(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded.value().api_key, ApiKey::kProduce);
    EXPECT_EQ(decoded.value().request_id, 501U);
    EXPECT_EQ(decoded.value().error.error_code, ProtocolErrorCode::kNone);

    ASSERT_TRUE(std::holds_alternative<ProduceResponse>(decoded.value().body));
    const auto& body = std::get<ProduceResponse>(decoded.value().body);
    EXPECT_EQ(body.partition_id, 2);
    EXPECT_EQ(body.base_offset, 42);
    EXPECT_EQ(body.record_count, 3U);
}

TEST(ProtocolCodecTest, EncodesAndDecodesFetchSuccessResponse) {
    FetchResponse fetch;
    fetch.base_offset = 10;
    fetch.high_watermark = 12;
    fetch.records.push_back(ProtocolRecord{1000, "k1", "v1"});
    fetch.records.push_back(ProtocolRecord{1001, "k2", "v2"});

    ResponseEnvelope response;
    response.api_key = ApiKey::kFetch;
    response.version = kProtocolVersion;
    response.request_id = 502;
    response.error = ErrorResponse{ProtocolErrorCode::kNone, ""};
    response.body = std::move(fetch);

    auto encoded = EncodeResponse(response);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeResponse(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_TRUE(std::holds_alternative<FetchResponse>(decoded.value().body));
    const auto& body = std::get<FetchResponse>(decoded.value().body);
    EXPECT_EQ(body.base_offset, 10);
    EXPECT_EQ(body.high_watermark, 12);
    ASSERT_EQ(body.records.size(), 2U);
    EXPECT_EQ(body.records[0].key, "k1");
    EXPECT_EQ(body.records[1].value, "v2");
}

TEST(ProtocolCodecTest, EncodesAndDecodesMetadataSuccessResponse) {
    MetadataResponse metadata;
    metadata.topics.push_back(TopicMetadata{"orders", 3});
    metadata.topics.push_back(TopicMetadata{"payments", 1});

    ResponseEnvelope response;
    response.api_key = ApiKey::kMetadata;
    response.version = kProtocolVersion;
    response.request_id = 503;
    response.error = ErrorResponse{ProtocolErrorCode::kNone, ""};
    response.body = std::move(metadata);

    auto encoded = EncodeResponse(response);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeResponse(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_TRUE(std::holds_alternative<MetadataResponse>(decoded.value().body));
    const auto& body = std::get<MetadataResponse>(decoded.value().body);
    ASSERT_EQ(body.topics.size(), 2U);
    EXPECT_EQ(body.topics[0].topic, "orders");
    EXPECT_EQ(body.topics[0].partition_count, 3U);
    EXPECT_EQ(body.topics[1].topic, "payments");
}

TEST(ProtocolCodecTest, EncodesAndDecodesCreateTopicSuccessResponse) {
    ResponseEnvelope response;
    response.api_key = ApiKey::kCreateTopic;
    response.version = kProtocolVersion;
    response.request_id = 504;
    response.error = ErrorResponse{ProtocolErrorCode::kNone, ""};
    response.body = CreateTopicResponse{"orders", 4};

    auto encoded = EncodeResponse(response);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeResponse(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_TRUE(std::holds_alternative<CreateTopicResponse>(decoded.value().body));
    const auto& body = std::get<CreateTopicResponse>(decoded.value().body);
    EXPECT_EQ(body.topic, "orders");
    EXPECT_EQ(body.partition_count, 4U);
}

TEST(ProtocolCodecTest, DecodeRejectsSuccessfulResponseWithWrongBody) {
    ResponseEnvelope response;
    response.api_key = ApiKey::kProduce;
    response.version = kProtocolVersion;
    response.request_id = 505;
    response.error = ErrorResponse{ProtocolErrorCode::kNone, ""};
    response.body = MetadataResponse{};

    auto encoded = EncodeResponse(response);
    ASSERT_FALSE(encoded.ok());
    EXPECT_EQ(encoded.status().code(), ErrorCode::kInvalidArgument);
}

}  // namespace
}  // namespace logmq
