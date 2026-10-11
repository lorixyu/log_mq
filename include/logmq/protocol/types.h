#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "logmq/base/types.h"

namespace logmq {

// 消息类型
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
    kNack = 11,
};

// 错误码
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
inline constexpr std::uint16_t kRetryProtocolVersion = 2;

inline constexpr bool SupportedProtocolVersion(std::uint16_t version) {
    return version == kProtocolVersion || version == kRetryProtocolVersion;
}

struct RetryMetadata {
    std::uint32_t retry_count{0};
    std::uint64_t next_visible_time{0};
    TopicName original_topic;
    std::string failure_reason;
    PartitionId original_partition{kInvalidPartitionId};
    Offset original_offset{kInvalidOffset};
};

// 客户端需要构造的帧头
inline constexpr std::size_t kFrameHeaderBytes =
    sizeof(std::uint32_t) +  // total_len
    sizeof(std::uint16_t) +  // api_key
    sizeof(std::uint16_t) +  // version
    sizeof(std::uint64_t);   // request_id
inline constexpr std::uint32_t kMaxFrameBytes = 16 * 1024 * 1024;

struct ProtocolRecord {
    std::uint64_t timestamp{0};
    std::string key;
    std::string value;
    std::optional<RetryMetadata> retry{};
};

// 创建消息请求
struct ProduceRequest {
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    std::vector<ProtocolRecord> records;
};

// 拉取消息请求
struct FetchRequest {
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    Offset offset{kInvalidOffset};
    std::uint32_t max_bytes{0};
};

// 查询topic的元数据(topic, partition)
// struct TopicMetadata;
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

// 加入消费组，注册Consumer
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

struct NackRequest {
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    Offset offset{kInvalidOffset};
    std::string reason;
};

using RequestBody = std::variant<ProduceRequest,
                                 FetchRequest,
                                 MetadataRequest,
                                 CreateTopicRequest,
                                 CommitOffsetRequest,
                                 FetchCommittedOffsetRequest,
                                 JoinGroupRequest,
                                 SyncGroupRequest,
                                 HeartbeatRequest,
                                 LeaveGroupRequest,
                                 NackRequest>;

// Complete decoded request frame. request_id is echoed by the response so a
// client can match responses to in-flight requests on the same connection.
struct RequestEnvelope {
    ApiKey api_key{ApiKey::kProduce};
    std::uint16_t version{kProtocolVersion};
    std::uint64_t request_id{0};
    RequestBody body{};
};

struct ErrorResponse {
    ProtocolErrorCode error_code{ProtocolErrorCode::kNone};
    std::string message;
};

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
    std::int32_t generation_id{0};  // 当前消费组的版本号
    std::string leader_id;  // 组内leader
};

// Join之后获取自己负责的partition
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

struct NackResponse {
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};
    Offset offset{kInvalidOffset};
    RetryMetadata retry;
    bool dead_letter{false};
};

// Successful response payloads are typed by api_key, just like RequestBody.
// Error responses keep the right empty alternative so callers can still inspect api_key.
using ResponseBody = std::variant<ProduceResponse,
                                 FetchResponse,
                                 MetadataResponse,
                                 CreateTopicResponse,
                                 CommitOffsetResponse,
                                 FetchCommittedOffsetResponse,
                                 JoinGroupResponse,
                                 SyncGroupResponse,
                                 HeartbeatResponse,
                                 LeaveGroupResponse,
                                 NackResponse>;

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
