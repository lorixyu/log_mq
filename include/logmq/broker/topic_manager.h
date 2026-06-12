#pragma once

#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "logmq/base/config.h"
#include "logmq/base/result.h"
#include "logmq/base/types.h"
#include "logmq/protocol/types.h"
#include "logmq/storage/commit_log.h"

namespace logmq {

struct TopicManagerOptions {
    std::filesystem::path data_dir{"./data"};
    std::uint64_t segment_bytes{1024 * 1024 * 1024ULL};
    std::uint64_t index_interval_bytes{4 * 1024};
};

// One partition owns one CommitLog. CommitLog already serializes append/read
// internally, so TopicManager only protects the topic map and partition lookup.
struct Partition {
    PartitionId id{kInvalidPartitionId};
    CommitLog log;
};

struct Topic {
    TopicName name;
    std::vector<std::unique_ptr<Partition>> partitions;
};

class TopicManager {
public:
    explicit TopicManager(TopicManagerOptions options);

    [[nodiscard]] Status CreateTopic(const TopicName& topic, std::uint32_t partition_count);

    [[nodiscard]] Result<Partition*> GetPartition(const TopicName& topic,
                                                  PartitionId partition_id);

    [[nodiscard]] Result<std::vector<TopicMetadata>> GetMetadata(const TopicName& topic) const;

    [[nodiscard]] Status Flush() const;

    [[nodiscard]] Status Close();

private:
    // Creating partition path.   .data/topicname/2 001.log
    [[nodiscard]] std::filesystem::path PartitionPath(const TopicName& topic,
                                                      PartitionId partition_id) const;

    TopicManagerOptions options_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<TopicName, std::unique_ptr<Topic>> topics_;
};

[[nodiscard]] TopicManagerOptions TopicManagerOptionsFromConfig(const StorageConfig& config);

}  // namespace logmq
