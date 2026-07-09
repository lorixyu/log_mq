#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "logmq/base/types.h"

namespace logmq {

// Stable request type identifiers stored in every protocol frame header.
enum class ApiKey : std::uint16_t {
    kProduce = 1,
    kFetch = 2,
    kMetadata = 3,
    kCreateTopic = 4,
    kCommitOffset = 5,
    kFetchCommittedOffset = 6,
    kJoinGroup = 7,
    kSyncGroup = 8,
    kHeartbeat = 9,
    kLeaveGroup = 10,
};

// Protocol-level errors carried by error responses.
enum class ProtocolErrorCode : std::uint16_t {
    kNone = 0,
    kInvalidRequest = 1,
    kTopicNotFound = 2,
    kOffsetOutOfRange = 3,
    kInternal = 4,
    kUnsupportedVersion = 5,
    kUnknownMember = 6,
    kIllegalGeneration = 7,
};

inline constexpr std::uint16_t kProtocolVersion = 1;

// total_len includes these 16 header bytes and the following body bytes.
inline constexpr std::size_t kFrameHeaderBytes =
    sizeof(std::uint32_t) + sizeof(std::uint16_t) + sizeof(std::uint16_t) +
    sizeof(std::uint64_t);
inline constexpr std::uint32_t kMaxFrameBytes = 16 * 1024 * 1024;

// Wire-level record payload for ProduceRequest.
// Storage RecordBatch encoding is deliberately separate from this format.
struct ProtocolRecord {
    std::uint64_t timestamp{0};
    std::string key;
    std::string value;
};

// Append one or more records to a topic partition.
struct ProduceRequest {
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    std::vector<ProtocolRecord> records;
};

// Fetch records starting at offset from a topic partition.
struct FetchRequest {
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    Offset offset{kInvalidOffset};
    std::uint32_t max_bytes{0};
};

// Query topic metadata. An empty topic can later mean "all topics".
struct MetadataRequest {
    TopicName topic;
};

// Create a topic with a fixed partition count.
struct CreateTopicRequest {
    TopicName topic;
    std::uint32_t partition_count{0};
};

// Commit the next offset a consumer group should read for one partition.
struct CommitOffsetRequest {
    std::string group_id;
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    Offset offset{kInvalidOffset};
};

// Fetch the committed offset for one consumer group/topic/partition.
struct FetchCommittedOffsetRequest {
    std::string group_id;
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
};

struct PartitionAssignment {
    TopicName topic;
    std::vector<PartitionId> partition_ids;
};

struct JoinGroupRequest {
    std::string group_id;
    std::string member_id;
    TopicName topic;
};

struct SyncGroupRequest {
    std::string group_id;
    std::string member_id;
    std::int32_t generation_id{0};
};

struct HeartbeatRequest {
    std::string group_id;
    std::string member_id;
    std::int32_t generation_id{0};
};

struct LeaveGroupRequest {
    std::string group_id;
    std::string member_id;
};

// Decoded request body. RequestEnvelope::api_key must match the active 
// variant alternative; EncodeRequest validates this before writing bytes.
using RequestBody = std::variant<ProduceRequest,
                                 FetchRequest,
                                 MetadataRequest,
                                 CreateTopicRequest,
                                 CommitOffsetRequest,
                                 FetchCommittedOffsetRequest,
                                 JoinGroupRequest,
                                 SyncGroupRequest,
                                 HeartbeatRequest,
                                 LeaveGroupRequest>;

// Complete decoded request frame. request_id is echoed by the response so a
// client can match responses to in-flight requests on the same connection.
struct RequestEnvelope {
    ApiKey api_key{ApiKey::kProduce};
    std::uint16_t version{kProtocolVersion};
    std::uint64_t request_id{0};
    RequestBody body{};
};

// Minimal error response payload for Week 3.
struct ErrorResponse {
    ProtocolErrorCode error_code{ProtocolErrorCode::kNone};
    std::string message;
};

// Produce returns the logical offset range assigned to the appended batch.
// Clients can derive [base_offset, base_offset + record_count).
struct ProduceResponse {
    PartitionId partition_id{kInvalidPartitionId};
    Offset base_offset{kInvalidOffset};
    std::uint32_t record_count{0};
};

// Fetch returns the batch found at or after the requested offset. The
// high_watermark is the visible end offset for this partition in the current broker.
struct FetchResponse {
    Offset base_offset{kInvalidOffset};
    Offset high_watermark{kInvalidOffset};
    std::vector<ProtocolRecord> records;
};

struct TopicMetadata {
    TopicName topic;
    std::uint32_t partition_count{0};
};

struct MetadataResponse {
    std::vector<TopicMetadata> topics;
};

struct CreateTopicResponse {
    TopicName topic;
    std::uint32_t partition_count{0};
};

struct CommitOffsetResponse {
    std::string group_id;
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    Offset offset{kInvalidOffset};
};

struct FetchCommittedOffsetResponse {
    std::string group_id;
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    bool committed{false};
    Offset offset{0};
};

struct JoinGroupResponse {
    std::string group_id;
    std::string member_id;
    std::int32_t generation_id{0};
    std::string leader_id;
};

struct SyncGroupResponse {
    std::string group_id;
    std::string member_id;
    std::int32_t generation_id{0};
    PartitionAssignment assignment;
};

struct HeartbeatResponse {
    std::string group_id;
    std::string member_id;
    std::int32_t generation_id{0};
};

struct LeaveGroupResponse {
    std::string group_id;
    std::string member_id;
    std::int32_t generation_id{0};
};

// Successful response payloads are typed by api_key, just like RequestBody.
// Error responses keep the right empty alternative so callers can still inspect api_key.
using ResponseBody =
    std::variant<ProduceResponse,
                 FetchResponse,
                 MetadataResponse,
                 CreateTopicResponse,
                 CommitOffsetResponse,
                 FetchCommittedOffsetResponse,
                 JoinGroupResponse,
                 SyncGroupResponse,
                 HeartbeatResponse,
                 LeaveGroupResponse>;

// Wire body layout is: error_code + message + optional success payload.
// The success payload is present only when error_code == kNone.
struct ResponseEnvelope {
    ApiKey api_key{ApiKey::kProduce};
    std::uint16_t version{kProtocolVersion};
    std::uint64_t request_id{0};
    ErrorResponse error;
    ResponseBody body{};
};

}  // namespace logmq
