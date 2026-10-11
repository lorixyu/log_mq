#include "logmq/broker/retry_scheduler.h"

#include <algorithm>
#include <span>

#include "logmq/protocol/codec.h"

namespace logmq {
namespace {
constexpr std::string_view kEnvelopeMagic{"LMQR\x01", 5};
}

std::uint64_t RetryNowMs() {
    const auto time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return static_cast<std::uint64_t>(std::max<std::int64_t>(0, time));
}

Result<Record> EncodeRetryRecord(const ProtocolRecord& record) {
    if (!record.retry || IsInternalTopic(record.retry->original_topic)) {
        return Status::InvalidArgument("retry record needs original topic metadata");
    }
    ResponseEnvelope envelope;
    envelope.api_key = ApiKey::kFetch;
    envelope.version = kRetryProtocolVersion;
    envelope.body = FetchResponse{0, 1, {record}};
    auto bytes = EncodeResponse(envelope);
    if (!bytes.ok()) return bytes.status();
    Record stored;
    stored.timestamp = record.timestamp;
    stored.key = record.key;
    stored.value = kEnvelopeMagic;
    stored.value.append(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    return stored;
}

Result<ProtocolRecord> DecodeRetryRecord(const Record& record) {
    if (!record.value.starts_with(kEnvelopeMagic)) {
        return Status::Corruption("invalid retry envelope magic or version");
    }
    const auto* data = reinterpret_cast<const std::byte*>(record.value.data() + kEnvelopeMagic.size());
    auto decoded = DecodeResponse(std::span<const std::byte>(data,
        record.value.size() - kEnvelopeMagic.size()));
    if (!decoded.ok()) return Status::Corruption(std::string(decoded.status().message()));
    const auto& response = decoded.value();
    if (response.api_key != ApiKey::kFetch || response.version != kRetryProtocolVersion ||
        response.error.error_code != ProtocolErrorCode::kNone) {
        return Status::Corruption("invalid retry envelope type");
    }
    auto& fetch = std::get<FetchResponse>(decoded.value().body);
    if (fetch.base_offset != 0 || fetch.high_watermark != 1 || fetch.records.size() != 1 ||
        !fetch.records.front().retry || IsInternalTopic(fetch.records.front().retry->original_topic) ||
        fetch.records.front().key != record.key || fetch.records.front().timestamp != record.timestamp) {
        return Status::Corruption("invalid retry envelope contents");
    }
    return std::move(fetch.records.front());
}

Status RetryScheduler::Start() {
    std::lock_guard lock(mutex_);
    if (thread_.joinable()) return Status::Ok();
    if (stopped_) return Status::InvalidArgument("retry scheduler is closed");
    tasks_ = {};
    visibility_.clear();
    auto metadata = topics_.GetMetadata("");
    if (!metadata.ok()) return metadata.status();
    for (const auto& topic : metadata.value()) {
        if (!IsInternalTopic(topic.topic)) continue;
        const auto prefix = IsRetryTopic(topic.topic) ? kRetryTopicPrefix : kDlqTopicPrefix;
        const TopicName original = topic.topic.substr(prefix.size());
        auto source = topics_.GetMetadata(original);
        if (!source.ok() || source.value().front().partition_count != topic.partition_count) {
            return Status::Corruption("internal topic has invalid original topic");
        }
        for (std::uint32_t id = 0; id < topic.partition_count; ++id) {
            auto partition = topics_.GetPartition(topic.topic, static_cast<PartitionId>(id));
            if (!partition.ok()) return partition.status();
            const auto end = partition.value()->log.next_offset();
            for (Offset offset = 0; offset < end; ++offset) {
                auto batch = partition.value()->log.Read(offset, kMaxRetryBatchBytes);
                if (!batch.ok()) return batch.status();
                if (batch.value().base_offset != offset || batch.value().records.size() != 1) {
                    return Status::Corruption("retry log must contain single-record batches");
                }
                auto record = DecodeRetryRecord(batch.value().records.front());
                if (!record.ok()) return record.status();
                const auto& retry = *record.value().retry;
                if (retry.original_topic != original || retry.original_partition != static_cast<PartitionId>(id) ||
                    (IsRetryTopic(topic.topic) && retry.next_visible_time == 0) ||
                    (IsDlqTopic(topic.topic) && retry.next_visible_time != 0)) {
                    return Status::Corruption("retry metadata does not match internal topic");
                }
                if (IsRetryTopic(topic.topic)) {
                    tasks_.push(RetryTask{topic.topic, static_cast<PartitionId>(id), offset,
                        retry.next_visible_time});
                }
            }
        }
    }
    MakeDueVisibleLocked();
    thread_ = std::thread([this] { Run(); });
    return Status::Ok();
}

void RetryScheduler::Schedule(TopicName topic, PartitionId partition, Offset offset,
                              std::uint64_t time) {
    std::lock_guard lock(mutex_);
    tasks_.push(RetryTask{std::move(topic), partition, offset, time});
    MakeDueVisibleLocked();
    condition_.notify_all();
}

Offset RetryScheduler::VisibleEnd(const TopicName& topic, PartitionId partition) const {
    std::lock_guard lock(mutex_);
    auto found = visibility_.find({topic, partition});
    return found == visibility_.end() ? 0 : found->second.end;
}

void RetryScheduler::MakeDueVisibleLocked() {
    const auto now = RetryNowMs();
    while (!tasks_.empty() && tasks_.top().time <= now) {
        const auto task = tasks_.top();
        tasks_.pop();
        auto& state = visibility_[{task.topic, task.partition}];
        if (task.offset >= state.end) state.ready.insert(task.offset);
        while (state.ready.erase(state.end) != 0) ++state.end;
    }
}

void RetryScheduler::Run() {
    std::unique_lock lock(mutex_);
    while (!stopped_) {
        MakeDueVisibleLocked();
        if (tasks_.empty()) {
            condition_.wait(lock, [this] { return stopped_ || !tasks_.empty(); });
        } else {
            // Recheck the wall clock at least once per second, including after
            // clock adjustments. Notification also wakes for earlier tasks.
            const auto now = RetryNowMs();
            const auto delay = tasks_.top().time > now ? tasks_.top().time - now : 0;
            condition_.wait_for(lock, std::chrono::milliseconds(std::min<std::uint64_t>(delay, 1000)));
        }
    }
}

void RetryScheduler::Close() {
    {
        std::lock_guard lock(mutex_);
        stopped_ = true;
    }
    condition_.notify_all();
    if (thread_.joinable()) thread_.join();
}

}  // namespace logmq
