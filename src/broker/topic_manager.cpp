#include "logmq/broker/topic_manager.h"

#include <algorithm>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace logmq {

TopicManager::TopicManager(TopicManagerOptions options) : options_(std::move(options)) {}

Status TopicManager::CreateTopic(const TopicName& topic, std::uint32_t partition_count) {
    if (topic.empty()) {
        return Status::InvalidArgument("topic name must not be empty");
    }
    if (partition_count == 0) {
        return Status::InvalidArgument("partition_count must be positive");
    }

    std::unique_lock lock(mutex_);
    auto existing = topics_.find(topic);
    if (existing != topics_.end()) {
        if (existing->second->partitions.size() == partition_count) {
            return Status::Ok();
        }
        return Status::InvalidArgument("topic already exists with different partition count");
    }

    auto created = std::make_unique<Topic>();
    created->name = topic;
    created->partitions.reserve(partition_count);

    for (std::uint32_t id = 0; id < partition_count; ++id) {
        CommitLogOptions log_options;
        log_options.data_dir = PartitionPath(topic, static_cast<PartitionId>(id));
        log_options.segment_bytes = options_.segment_bytes;
        log_options.index_interval_bytes = options_.index_interval_bytes;

        auto log = CommitLog::Open(std::move(log_options));
        if (!log.ok()) {
            return log.status();
        }

        auto partition = std::make_unique<Partition>(Partition{static_cast<PartitionId>(id), std::move(log).value()});
        created->partitions.push_back(std::move(partition));
    }

    topics_.emplace(topic, std::move(created));
    return Status::Ok();
}

Result<Partition*> TopicManager::GetPartition(const TopicName& topic, PartitionId partition_id) {
    std::shared_lock lock(mutex_);
    auto found = topics_.find(topic);
    if (found == topics_.end()) {
        return Status::NotFound("topic not found");
    }
    if (partition_id < 0 || static_cast<std::size_t>(partition_id) >= found->second->partitions.size()) {
        return Status::NotFound("partition not found");
    }
    return found->second->partitions[static_cast<std::size_t>(partition_id)].get();
}

Result<std::vector<TopicMetadata>> TopicManager::GetMetadata(const TopicName& topic) const {
    std::shared_lock lock(mutex_);

    std::vector<TopicMetadata> metadata;
    if (!topic.empty()) {
        auto found = topics_.find(topic);
        if (found == topics_.end()) {
            return Status::NotFound("topic not found");
        }
        metadata.push_back(TopicMetadata{found->second->name,
                                         static_cast<std::uint32_t>(
                                             found->second->partitions.size())});
        return metadata;
    }
    // Empty Topic means all topic
    metadata.reserve(topics_.size());
    for (const auto& [name, value] : topics_) {
        metadata.push_back(
            TopicMetadata{name, static_cast<std::uint32_t>(value->partitions.size())});
    }
    std::sort(metadata.begin(), metadata.end(),
              [](const TopicMetadata& left, const TopicMetadata& right) {
                  return left.topic < right.topic;
              });
    return metadata;
}

Status TopicManager::Flush() const {
    std::shared_lock lock(mutex_);
    for (const auto& [_, topic] : topics_) {
        for (const auto& partition : topic->partitions) {
            Status status = partition->log.Flush();
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::Ok();
}

Status TopicManager::Close() {
    std::unique_lock lock(mutex_);
    for (auto& [_, topic] : topics_) {
        for (auto& partition : topic->partitions) {
            Status status = partition->log.Close();
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::Ok();
}

std::filesystem::path TopicManager::PartitionPath(const TopicName& topic,
                                                  PartitionId partition_id) const {
    return options_.data_dir / topic / std::to_string(partition_id);
}

TopicManagerOptions TopicManagerOptionsFromConfig(const StorageConfig& config) {
    TopicManagerOptions options;
    options.data_dir = config.data_dir;
    options.segment_bytes = config.segment_bytes;
    return options;
}

}  // namespace logmq
