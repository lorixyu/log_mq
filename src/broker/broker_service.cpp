#include "logmq/broker/broker_service.h"

#include <string>
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

}  // namespace

BrokerService::BrokerService(BrokerServiceOptions options)
    : options_(std::move(options)),
      topics_(TopicManagerOptionsFromConfig(options_.storage)),
      offsets_(OffsetStoreOptionsFromDataDir(options_.storage.data_dir)) {}

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

    Status status = topics_.Load();
    if (!status.ok()) {
        return status;
    }
    status = offsets_.Load();
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
    if (request.version != kProtocolVersion) {
        return MakeError(request, ProtocolErrorCode::kUnsupportedVersion,
                         "unsupported protocol version");
    }

    switch (request.api_key) {
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
    return topics_.Close();
}

ResponseEnvelope BrokerService::HandleProduce(const RequestEnvelope& envelope,
                                              const ProduceRequest& request) {
    if (request.records.empty()) {
        return MakeError(envelope, ProtocolErrorCode::kInvalidRequest,
                         "produce request must contain at least one record");
    }

    const std::string_view key = request.records.front().key;
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

    const Offset high_watermark = partition.value()->log.next_offset();
    if (request.offset == high_watermark) {
        FetchResponse response;
        response.base_offset = request.offset;
        response.high_watermark = high_watermark;
        return MakeSuccess(envelope,
                           ResponseBody{std::in_place_type<FetchResponse>, std::move(response)});
    }
    if (request.offset > high_watermark) {
        return MakeError(envelope, ProtocolErrorCode::kOffsetOutOfRange,
                         "fetch offset is beyond high watermark");
    }

    auto batch = partition.value()->log.Read(request.offset, request.max_bytes);
    if (!batch.ok()) {
        ProtocolErrorCode code = batch.status().code() == ErrorCode::kNotFound
                                     ? ProtocolErrorCode::kOffsetOutOfRange
                                     : InvalidOrInternal(batch.status());
        return MakeError(envelope, code, std::string(batch.status().message()));
    }

    FetchResponse response;
    response.base_offset = batch.value().base_offset;
    // In the single-broker version, all appended records are immediately visible.
    // Replication can later replace this with the replicated high watermark.
    response.high_watermark = high_watermark;
    response.records.reserve(batch.value().records.size());
    for (const Record& record : batch.value().records) {
        response.records.push_back(ToProtocolRecord(record));
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

ResponseEnvelope BrokerService::MakeError(const RequestEnvelope& envelope,
                                          ProtocolErrorCode code,
                                          std::string message) const {
    ResponseEnvelope response;
    response.api_key = envelope.api_key;
    response.version = kProtocolVersion;
    response.request_id = envelope.request_id;
    response.error = ErrorResponse{code, std::move(message)};
    return response;
}

ResponseEnvelope BrokerService::MakeSuccess(const RequestEnvelope& envelope,
                                            ResponseBody body) const {
    ResponseEnvelope response;
    response.api_key = envelope.api_key;
    response.version = kProtocolVersion;
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
