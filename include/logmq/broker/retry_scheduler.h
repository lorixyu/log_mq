#pragma once

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <string_view>
#include <thread>

#include "logmq/broker/topic_manager.h"

namespace logmq {

inline constexpr std::string_view kRetryTopicPrefix = "%RETRY%";
inline constexpr std::string_view kDlqTopicPrefix = "%DLQ%";

inline bool IsRetryTopic(std::string_view topic) { return topic.starts_with(kRetryTopicPrefix); }
inline bool IsDlqTopic(std::string_view topic) { return topic.starts_with(kDlqTopicPrefix); }
inline bool IsInternalTopic(std::string_view topic) { return IsRetryTopic(topic) || IsDlqTopic(topic); }

struct RetryOptions {
    std::chrono::milliseconds delay{5000};
    std::uint32_t max_retries{3};
};

// The internal envelope reuses the v2 Fetch codec. The key is also retained in
// the storage record, so recovery needs room for up to two frame-sized payloads.
inline constexpr std::size_t kMaxRetryBatchBytes = 2ULL * kMaxFrameBytes + 128;

[[nodiscard]] std::uint64_t RetryNowMs();
[[nodiscard]] Result<Record> EncodeRetryRecord(const ProtocolRecord& record);
[[nodiscard]] Result<ProtocolRecord> DecodeRetryRecord(const Record& record);

class RetryScheduler {
public:
    explicit RetryScheduler(TopicManager& topics) : topics_(topics) {}
    ~RetryScheduler() { Close(); }

    [[nodiscard]] Status Start();
    void Schedule(TopicName topic, PartitionId partition, Offset offset, std::uint64_t time);
    [[nodiscard]] Offset VisibleEnd(const TopicName& topic, PartitionId partition) const;
    void Close();

private:
    struct RetryTask {
        TopicName topic;
        PartitionId partition;
        Offset offset;
        std::uint64_t time;
    };
    struct CompareByVisibleTime {
        bool operator()(const RetryTask& left, const RetryTask& right) const {
            return left.time > right.time;
        }
    };
    struct Visibility {
        Offset end{0};
        std::set<Offset> ready;
    };
    void MakeDueVisibleLocked();
    void Run();

    TopicManager& topics_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::priority_queue<RetryTask, std::vector<RetryTask>, CompareByVisibleTime> tasks_;
    std::map<std::pair<TopicName, PartitionId>, Visibility> visibility_;
    std::thread thread_;
    bool stopped_{false};
};

}  // namespace logmq
