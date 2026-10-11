#include "logmq/broker/broker_service.h"

#include <string>
#include <limits>
#include <chrono>
#include <utility>
#include <variant>
#include <vector>

#include "logmq/storage/record.h"
#include "logmq/storage/record_batch.h"

namespace logmq {
namespace {

Record ToStorageRecord(const ProtocolRecord& record) {
    // Keep wire records separate from storage records so protocol evolution does
    // not force the on-disk format to change.
    Record storage_record;
    storage_record.timestamp = record.timestamp;
    storage_record.key = record.key;
    storage_record.value = record.value;
    return storage_record;
}

ProtocolRecord ToProtocolRecord(const Record& record) {
    ProtocolRecord protocol_record;
    protocol_record.timestamp = record.timestamp;
    protocol_record.key = record.key;
    protocol_record.value = record.value;
    return protocol_record;
}

ProtocolErrorCode InvalidOrInternal(const Status& status) {
    // Storage/base Status is internal; clients see the smaller protocol error set.
    if (status.code() == ErrorCode::kInvalidArgument) {
        return ProtocolErrorCode::kInvalidRequest;
    }
    return ProtocolErrorCode::kInternal;
}

ProtocolErrorCode GroupErrorCode(const Status& status) {
    if (status.code() == ErrorCode::kNotFound && status.message() == "unknown member") {
        return ProtocolErrorCode::kUnknownMember;
    }
    if (status.code() == ErrorCode::kInvalidArgument &&
        status.message() == "illegal generation") {
        return ProtocolErrorCode::kIllegalGeneration;
    }
    return InvalidOrInternal(status);
}

}  // namespace

BrokerService::BrokerService(BrokerServiceOptions options)
    : options_(std::move(options)),
      topics_(TopicManagerOptionsFromConfig(options_.storage)),
      retries_(topics_),
      offsets_(OffsetStoreOptionsFromDataDir(options_.storage.data_dir)),
      groups_(GroupCoordinatorOptionsFromConfig(options_.consumer)) {}

BrokerService::~BrokerService() {
    (void)Close();
}

Status BrokerService::Start() {
    std::lock_guard lock(close_mutex_);
    if (started_) {
        return Status::Ok();
    }
    if (closed_) {
        return Status::InvalidArgument("broker service is closed");
    }

    if (options_.retry.delay.count() < 0 || options_.retry.max_retries == 0 ||
        options_.retry.max_retries == std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument("invalid retry options");
    }
    Status status = topics_.Load();
    if (!status.ok()) {
        return status;
    }
    status = offsets_.Load();
    if (!status.ok()) {
        return status;
    }
    status = retries_.Start();
    if (!status.ok()) return status;
    status = groups_.Start();
    if (!status.ok()) {
        return status;
    }

    if (options_.storage.flush_policy == FlushPolicy::kAsync &&
        options_.storage.flush_interval.count() > 0) {
        // Async flush
        flusher_ = std::thread([this] { RunAsyncFlusher(); });
    }
    started_ = true;
    return Status::Ok();
}

ResponseEnvelope BrokerService::Handle(const RequestEnvelope& request) {
    if (!SupportedProtocolVersion(request.version)) {
        return MakeError(request, ProtocolErrorCode::kUnsupportedVersion,
                         "unsupported protocol version");
    }

    switch (request.api_key) {
        case ApiKey::kNack:
            if (!std::holds_alternative<NackRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "nack request envelope has wrong body type");
            }
            return HandleNack(request, std::get<NackRequest>(request.body));
        case ApiKey::kProduce:
            if (!std::holds_alternative<ProduceRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "produce request envelope has wrong body type");
            }
            return HandleProduce(request, std::get<ProduceRequest>(request.body));
        case ApiKey::kFetch:
            if (!std::holds_alternative<FetchRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "fetch request envelope has wrong body type");
            }
            return HandleFetch(request, std::get<FetchRequest>(request.body));
        case ApiKey::kMetadata:
            if (!std::holds_alternative<MetadataRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "metadata request envelope has wrong body type");
            }
            return HandleMetadata(request, std::get<MetadataRequest>(request.body));
        case ApiKey::kCreateTopic:
            if (!std::holds_alternative<CreateTopicRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "create topic request envelope has wrong body type");
            }
            return HandleCreateTopic(request, std::get<CreateTopicRequest>(request.body));
        case ApiKey::kCommitOffset:
            if (!std::holds_alternative<CommitOffsetRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "commit offset request envelope has wrong body type");
            }
            return HandleCommitOffset(request, std::get<CommitOffsetRequest>(request.body));
        case ApiKey::kFetchCommittedOffset:
            if (!std::holds_alternative<FetchCommittedOffsetRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "fetch committed offset request envelope has wrong body type");
            }
            return HandleFetchCommittedOffset(
                request, std::get<FetchCommittedOffsetRequest>(request.body));
        case ApiKey::kJoinGroup:
            if (!std::holds_alternative<JoinGroupRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "join group request envelope has wrong body type");
            }
            return HandleJoinGroup(request, std::get<JoinGroupRequest>(request.body));
        case ApiKey::kSyncGroup:
            if (!std::holds_alternative<SyncGroupRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "sync group request envelope has wrong body type");
            }
            return HandleSyncGroup(request, std::get<SyncGroupRequest>(request.body));
        case ApiKey::kHeartbeat:
            if (!std::holds_alternative<HeartbeatRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "heartbeat request envelope has wrong body type");
            }
            return HandleHeartbeat(request, std::get<HeartbeatRequest>(request.body));
        case ApiKey::kLeaveGroup:
            if (!std::holds_alternative<LeaveGroupRequest>(request.body)) {
                return MakeError(request, ProtocolErrorCode::kInvalidRequest,
                                 "leave group request envelope has wrong body type");
            }
            return HandleLeaveGroup(request, std::get<LeaveGroupRequest>(request.body));
    }

    return MakeError(request, ProtocolErrorCode::kInvalidRequest, "unknown api key");
}

Status BrokerService::Close() {
    std::lock_guard lock(close_mutex_);
    if (closed_) {
        return Status::Ok();
    }
    closed_ = true;
    if (flusher_.joinable()) {
        stop_flusher_.store(true);
        flusher_condition_.notify_all();
        flusher_.join();
    }
    retries_.Close();
    Status group_status = groups_.Close();
    Status topics_status = topics_.Close();
    if (!group_status.ok()) {
        return group_status;
    }
    return topics_status;
}


ResponseEnvelope BrokerService::HandleNack(const RequestEnvelope& envelope,
                                           const NackRequest& request) {
    if (IsDlqTopic(request.topic)) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest, "DLQ messages cannot be retried");
    }
    if (request.offset < 0) {
        return MakeError(envelope, ProtocolErrorCode::kOffsetOutOfRange, "nack offset must not be negative");
    }
    std::lock_guard lock(nack_mutex_);
    auto source = topics_.GetPartition(request.topic, request.partition_id);
    if (!source.ok()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound, std::string(source.status().message()));
    }
    if (request.offset >= source.value()->log.next_offset()) {
        return MakeError(envelope, ProtocolErrorCode::kOffsetOutOfRange, "nack offset is beyond log end");
    }
    if (IsRetryTopic(request.topic) &&
        request.offset >= retries_.VisibleEnd(request.topic, request.partition_id)) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest, "retry message is not visible yet");
    }
    auto batch = source.value()->log.Read(request.offset, kMaxRetryBatchBytes);
    if (!batch.ok()) return MakeError(envelope, InvalidOrInternal(batch.status()), std::string(batch.status().message()));
    const auto index = static_cast<std::size_t>(request.offset - batch.value().base_offset);
    if (index >= batch.value().records.size()) {
        return MakeError(envelope, ProtocolErrorCode::kInternal, "nack offset not found in batch");
    }
    const Record& stored = batch.value().records[index];
    ProtocolRecord record = ToProtocolRecord(stored);
    RetryMetadata retry{0, 0, request.topic, request.reason, request.partition_id, request.offset};
    if (IsRetryTopic(request.topic)) {
        auto decoded = DecodeRetryRecord(stored);
        if (!decoded.ok()) return MakeError(envelope, ProtocolErrorCode::kInternal, std::string(decoded.status().message()));
        record = std::move(decoded).value();
        retry = *record.retry;
        if (request.topic != std::string(kRetryTopicPrefix) + retry.original_topic ||
            request.partition_id != retry.original_partition ||
            retry.retry_count == std::numeric_limits<std::uint32_t>::max()) {
            return MakeError(envelope, ProtocolErrorCode::kInternal, "invalid retry source metadata");
        }
    }
    ++retry.retry_count;
    retry.failure_reason = request.reason;
    const bool dead_letter = retry.retry_count > options_.retry.max_retries;
    retry.next_visible_time = dead_letter ? 0 : RetryNowMs() + static_cast<std::uint64_t>(options_.retry.delay.count());
    record.retry = retry;
    auto encoded = EncodeRetryRecord(record);
    if (!encoded.ok()) return MakeError(envelope, InvalidOrInternal(encoded.status()), std::string(encoded.status().message()));
    auto metadata = topics_.GetMetadata(retry.original_topic);
    if (!metadata.ok()) return MakeError(envelope, ProtocolErrorCode::kTopicNotFound, std::string(metadata.status().message()));
    const TopicName destination = std::string(dead_letter ? kDlqTopicPrefix : kRetryTopicPrefix) + retry.original_topic;
    Status status = topics_.CreateTopic(destination, metadata.value().front().partition_count);
    if (!status.ok()) return MakeError(envelope, InvalidOrInternal(status), std::string(status.message()));
    auto target = topics_.GetPartition(destination, request.partition_id);
    if (!target.ok()) return MakeError(envelope, ProtocolErrorCode::kInternal, std::string(target.status().message()));
    auto appended = target.value()->log.Append({std::move(encoded).value()});
    if (!appended.ok()) return MakeError(envelope, InvalidOrInternal(appended.status()), std::string(appended.status().message()));
    // Publish every successful append even if a subsequent fsync fails, so a
    // retried request cannot leave a permanent visibility hole in the log.
    if (!dead_letter) retries_.Schedule(destination, request.partition_id,
        appended.value().base_offset, retry.next_visible_time);
    if (options_.storage.flush_policy == FlushPolicy::kSync) {
        status = target.value()->log.Flush();
        if (!status.ok()) return MakeError(envelope, ProtocolErrorCode::kInternal, std::string(status.message()));
    }
    return MakeSuccess(envelope, NackResponse{destination, request.partition_id,
        appended.value().base_offset, std::move(retry), dead_letter});
}

ResponseEnvelope BrokerService::HandleProduce(const RequestEnvelope& envelope,
                                              const ProduceRequest& request) {
    if (IsInternalTopic(request.topic)) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest, "internal topics are reserved for Nack");
    }
    if (request.records.empty()) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest,
                         "produce request must contain at least one record");
    }

    const std::string_view key = request.records.front().key;
    // 显式指定partition或者由broker选择，key非空哈希选择，key空轮询选择
    auto partition = topics_.SelectPartition(request.topic, request.partition_id, key);
    if (!partition.ok()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound,
                         std::string(partition.status().message()));
    }

    std::vector<Record> records;
    records.reserve(request.records.size());
    for (const ProtocolRecord& record : request.records) {
        records.push_back(ToStorageRecord(record));
    }

    auto appended = partition.value().partition->log.Append(std::move(records));
    if (!appended.ok()) {
        return MakeError(envelope, InvalidOrInternal(appended.status()),
                         std::string(appended.status().message()));
    }

    if (options_.storage.flush_policy == FlushPolicy::kSync) {
        Status status = partition.value().partition->log.Flush();
        if (!status.ok()) {
            return MakeError(envelope, ProtocolErrorCode::kInternal,
                             std::string(status.message()));
        }
    }

    ProduceResponse response;
    // A batch append assigns one contiguous offset range.
    response.partition_id = partition.value().id;
    response.base_offset = appended.value().base_offset;
    response.record_count = static_cast<std::uint32_t>(request.records.size());
    return MakeSuccess(envelope, ResponseBody{std::in_place_type<ProduceResponse>, response});
}

ResponseEnvelope BrokerService::HandleFetch(const RequestEnvelope& envelope,
                                            const FetchRequest& request) {
    if (request.offset < 0) {
        return MakeError(envelope, ProtocolErrorCode::kOffsetOutOfRange,
                         "fetch offset must not be negative");
    }
    if (request.max_bytes == 0) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest,
                         "fetch max_bytes must be positive");
    }

    auto partition = topics_.GetPartition(request.topic, request.partition_id);
    if (!partition.ok()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound,
                         std::string(partition.status().message()));
    }

    const Offset log_end = partition.value()->log.next_offset();
    const Offset high_watermark = IsRetryTopic(request.topic)
        ? retries_.VisibleEnd(request.topic, request.partition_id) : log_end;
    if (request.offset > log_end) {
        return MakeError(envelope, ProtocolErrorCode::kOffsetOutOfRange,
                         "fetch offset is beyond log end");
    }
    if (request.offset >= high_watermark) {
        FetchResponse response;
        response.base_offset = request.offset;
        response.high_watermark = high_watermark;
        return MakeSuccess(envelope,
                           ResponseBody{std::in_place_type<FetchResponse>, std::move(response)});
    }
    // Internal envelopes duplicate the key and add metadata. Charge the Fetch
    // budget for the original payload, so a readable message remains retryable.
    auto batch = partition.value()->log.Read(request.offset,
        IsInternalTopic(request.topic) ? kMaxRetryBatchBytes : request.max_bytes);
    if (!batch.ok()) {
        ProtocolErrorCode code = batch.status().code() == ErrorCode::kNotFound
                                     ? ProtocolErrorCode::kOffsetOutOfRange
                                     : InvalidOrInternal(batch.status());
        return MakeError(envelope, code, std::string(batch.status().message()));
    }

    FetchResponse response;
    response.base_offset = batch.value().base_offset;
    // Retry partitions expose only the contiguous prefix released by the scheduler.
    response.high_watermark = high_watermark;
    response.records.reserve(batch.value().records.size());
    for (const Record& record : batch.value().records) {
        if (IsInternalTopic(request.topic)) {
            auto decoded = DecodeRetryRecord(record);
            if (!decoded.ok()) return MakeError(envelope, ProtocolErrorCode::kInternal,
                                                std::string(decoded.status().message()));
            auto value = std::move(decoded).value();
            const auto payload_bytes = kRecordBatchHeaderBytes + EncodedRecordSize(ToStorageRecord(value));
            if (payload_bytes > request.max_bytes) {
                return MakeError(envelope, ProtocolErrorCode::kInvalidRequest,
                                 "record batch is larger than max_bytes");
            }
            if (envelope.version == kProtocolVersion) value.retry.reset();
            response.records.push_back(std::move(value));
        } else {
            response.records.push_back(ToProtocolRecord(record));
        }
    }
    return MakeSuccess(envelope, ResponseBody{std::in_place_type<FetchResponse>, std::move(response)});
}

ResponseEnvelope BrokerService::HandleMetadata(const RequestEnvelope& envelope,
                                               const MetadataRequest& request) {
    auto metadata = topics_.GetMetadata(request.topic);
    if (!metadata.ok()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound,
                         std::string(metadata.status().message()));
    }

    MetadataResponse response;
    response.topics = std::move(metadata).value();
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<MetadataResponse>, std::move(response)});
}

ResponseEnvelope BrokerService::HandleCreateTopic(const RequestEnvelope& envelope,
                                                  const CreateTopicRequest& request) {
    if (IsInternalTopic(request.topic)) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest, "internal topics are reserved for Nack");
    }
    Status status = topics_.CreateTopic(request.topic, request.partition_count);
    if (!status.ok()) {
        return MakeError(envelope, InvalidOrInternal(status), std::string(status.message()));
    }

    CreateTopicResponse response;
    response.topic = request.topic;
    response.partition_count = request.partition_count;
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<CreateTopicResponse>, std::move(response)});
}

ResponseEnvelope BrokerService::HandleCommitOffset(const RequestEnvelope& envelope,
                                                   const CommitOffsetRequest& request) {
    if (request.offset < 0) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest,
                         "commit offset must not be negative");
    }

    auto partition = topics_.GetPartition(request.topic, request.partition_id);
    if (!partition.ok()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound,
                         std::string(partition.status().message()));
    }

    if (request.offset > partition.value()->log.next_offset()) {
        return MakeError(envelope, ProtocolErrorCode::kOffsetOutOfRange,
                         "commit offset is beyond high watermark");
    }

    Status status = offsets_.Commit(request.group_id, request.topic, request.partition_id,
                                    request.offset);
    if (!status.ok()) {
        return MakeError(envelope, InvalidOrInternal(status), std::string(status.message()));
    }

    CommitOffsetResponse response;
    response.group_id = request.group_id;
    response.topic = request.topic;
    response.partition_id = request.partition_id;
    response.offset = request.offset;
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<CommitOffsetResponse>,
                                    std::move(response)});
}

ResponseEnvelope BrokerService::HandleFetchCommittedOffset(
    const RequestEnvelope& envelope,
    const FetchCommittedOffsetRequest& request) {
    auto partition = topics_.GetPartition(request.topic, request.partition_id);
    if (!partition.ok()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound,
                         std::string(partition.status().message()));
    }

    auto offset = offsets_.Fetch(request.group_id, request.topic, request.partition_id);
    if (!offset.ok()) {
        return MakeError(envelope, InvalidOrInternal(offset.status()),
                         std::string(offset.status().message()));
    }

    FetchCommittedOffsetResponse response;
    response.group_id = request.group_id;
    response.topic = request.topic;
    response.partition_id = request.partition_id;
    response.committed = offset.value().has_value();
    response.offset = response.committed ? *offset.value() : 0;
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<FetchCommittedOffsetResponse>,
                                    std::move(response)});
}

ResponseEnvelope BrokerService::HandleJoinGroup(const RequestEnvelope& envelope,
                                                const JoinGroupRequest& request) {
    auto metadata = topics_.GetMetadata(request.topic);
    if (!metadata.ok()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound,
                         std::string(metadata.status().message()));
    }
    if (metadata.value().empty()) {
        return MakeError(envelope, ProtocolErrorCode::kTopicNotFound, "topic not found");
    }

    auto joined = groups_.JoinGroup(request, metadata.value().front().partition_count);
    if (!joined.ok()) {
        return MakeError(envelope, GroupErrorCode(joined.status()),
                         std::string(joined.status().message()));
    }
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<JoinGroupResponse>,
                                    std::move(joined).value()});
}

ResponseEnvelope BrokerService::HandleSyncGroup(const RequestEnvelope& envelope,
                                                const SyncGroupRequest& request) {
    auto synced = groups_.SyncGroup(request);
    if (!synced.ok()) {
        return MakeError(envelope, GroupErrorCode(synced.status()),
                         std::string(synced.status().message()));
    }
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<SyncGroupResponse>,
                                    std::move(synced).value()});
}

ResponseEnvelope BrokerService::HandleHeartbeat(const RequestEnvelope& envelope,
                                                const HeartbeatRequest& request) {
    auto heartbeat = groups_.Heartbeat(request);
    if (!heartbeat.ok()) {
        return MakeError(envelope, GroupErrorCode(heartbeat.status()),
                         std::string(heartbeat.status().message()));
    }
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<HeartbeatResponse>,
                                    std::move(heartbeat).value()});
}

ResponseEnvelope BrokerService::HandleLeaveGroup(const RequestEnvelope& envelope,
                                                 const LeaveGroupRequest& request) {
    auto left = groups_.LeaveGroup(request);
    if (!left.ok()) {
        return MakeError(envelope, GroupErrorCode(left.status()),
                         std::string(left.status().message()));
    }
    return MakeSuccess(envelope,
                       ResponseBody{std::in_place_type<LeaveGroupResponse>,
                                    std::move(left).value()});
}

ResponseEnvelope BrokerService::MakeError(const RequestEnvelope& envelope,
                                          ProtocolErrorCode code,
                                          std::string message) const {
    ResponseEnvelope response;
    response.api_key = envelope.api_key;
    response.version = SupportedProtocolVersion(envelope.version) ? envelope.version : kProtocolVersion;
    response.request_id = envelope.request_id;
    response.error = ErrorResponse{code, std::move(message)};
    return response;
}

ResponseEnvelope BrokerService::MakeSuccess(const RequestEnvelope& envelope,
                                            ResponseBody body) const {
    ResponseEnvelope response;
    response.api_key = envelope.api_key;
    response.version = SupportedProtocolVersion(envelope.version) ? envelope.version : kProtocolVersion;
    response.request_id = envelope.request_id;
    response.error = ErrorResponse{ProtocolErrorCode::kNone, ""};
    response.body = std::move(body);
    return response;
}

void BrokerService::RunAsyncFlusher() {
    std::unique_lock lock(flusher_mutex_);
    while (true) {
        // Close() wakes this condition so shutdown does not wait for the full
        // flush interval.
        /* 
        while (!pred()) {
            wait();    
        }
        */
        const bool stopped
             = flusher_condition_.wait_for(lock, 
                                           options_.storage.flush_interval,
                                           [this] { return stop_flusher_.load(); });
        if (stopped || stop_flusher_.load()) {
            break;
        }
        lock.unlock();
        (void)topics_.Flush();
        lock.lock();
    }
}

}  // namespace logmq
