#include "logmq/broker/topic_manager.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

namespace logmq {
namespace {

constexpr std::string_view kTopicMetadataMagic = "logmq_topic_metadata_v1";
constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

Status ValidateTopicName(std::string_view topic) {
    if (topic.empty()) {
        return Status::InvalidArgument("topic name must not be empty");
    }
    if (topic == "." || topic == "..") {
        return Status::InvalidArgument("topic name must not be . or ..");
    }
    for (unsigned char ch : topic) {
        if (std::isalnum(ch) || ch == '.' || ch == '_' || ch == '-') {
            continue;
        }
        return Status::InvalidArgument(
            "topic name may only contain letters, digits, '.', '_' and '-'");
    }
    return Status::Ok();
}

std::uint64_t StableHash(std::string_view value) {
    std::uint64_t hash = kFnvOffsetBasis;
    for (unsigned char ch : value) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= kFnvPrime;
    }
    return hash;
}

Status FilesystemError(std::string_view operation, const std::error_code& error) {
    return Status::IoError(std::string(operation) + ": " + error.message());
}

}  // namespace

TopicManager::TopicManager(TopicManagerOptions options) : options_(std::move(options)) {}

Status TopicManager::Load() {
    std::error_code error;
    std::filesystem::create_directories(options_.data_dir, error);
    if (error) {
        return FilesystemError("create topic data directory", error);
    }

    const std::filesystem::path metadata_path = MetadataPath();
    if (!std::filesystem::exists(metadata_path, error)) {
        if (error) {
            return FilesystemError("stat topic metadata", error);
        }
        return Status::Ok();
    }

    std::ifstream input(metadata_path);
    if (!input.is_open()) {
        return Status::IoError("open topic metadata " + metadata_path.string());
    }

    std::string line;
    if (!std::getline(input, line)) {
        return Status::Corruption("topic metadata is empty");
    }
    if (line != kTopicMetadataMagic) {
        return Status::Corruption("topic metadata has unsupported format");
    }

    std::unordered_map<TopicName, std::unique_ptr<Topic>> loaded;
    std::uint64_t line_number = 1;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.empty()) {
            continue;
        }

        std::istringstream parser(line);
        TopicName topic;
        std::uint64_t partition_count = 0;
        std::string trailing;
        if (!(parser >> topic >> partition_count) || (parser >> trailing)) {
            return Status::Corruption("invalid topic metadata line " +
                                      std::to_string(line_number));
        }
        if (partition_count == 0 ||
            partition_count > std::numeric_limits<std::uint32_t>::max()) {
            return Status::Corruption("invalid partition count in topic metadata");
        }

        Status status = ValidateTopicName(topic);
        if (!status.ok()) {
            return status;
        }
        if (loaded.find(topic) != loaded.end()) {
            return Status::Corruption("duplicate topic in metadata: " + topic);
        }

        auto opened = OpenTopic(topic, static_cast<std::uint32_t>(partition_count));
        if (!opened.ok()) {
            return opened.status();
        }
        loaded.emplace(topic, std::move(opened).value());
    }
    if (!input.eof()) {
        return Status::IoError("read topic metadata " + metadata_path.string());
    }

    std::unique_lock lock(mutex_);
    topics_ = std::move(loaded);
    return Status::Ok();
}

Status TopicManager::CreateTopic(const TopicName& topic, std::uint32_t partition_count) {
    Status status = ValidateTopicName(topic);
    if (!status.ok()) {
        return status;
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

    auto opened = OpenTopic(topic, partition_count);
    if (!opened.ok()) {
        return opened.status();
    }

    topics_.emplace(topic, std::move(opened).value());
    status = PersistMetadataLocked();
    if (!status.ok()) {
        topics_.erase(topic);
        return status;
    }
    return Status::Ok();
}

Result<Partition*> TopicManager::GetPartition(const TopicName& topic, PartitionId partition_id) {
    std::shared_lock lock(mutex_);
    auto found = topics_.find(topic);
    if (found == topics_.end()) {
        return Status::NotFound("topic not found");
    }

    Topic& selected_topic = *found->second;
    if (partition_id < 0 ||
        static_cast<std::size_t>(partition_id) >= selected_topic.partitions.size()) {
        return Status::NotFound("partition not found");
    }
    return selected_topic.partitions[static_cast<std::size_t>(partition_id)].get();
}

Result<PartitionSelection> TopicManager::SelectPartition(const TopicName& topic,
                                                         PartitionId requested_partition,
                                                         std::string_view key) {
    std::shared_lock lock(mutex_);
    auto found = topics_.find(topic);
    if (found == topics_.end()) {
        return Status::NotFound("topic not found");
    }

    Topic& selected_topic = *found->second;
    if (selected_topic.partitions.empty()) {
        return Status::Internal("topic has no partitions");
    }

    PartitionId selected_id = requested_partition;
    if (selected_id == kInvalidPartitionId) {
        if (!key.empty()) {
            selected_id = static_cast<PartitionId>(
                StableHash(key) % selected_topic.partitions.size());
        } else {
            const std::uint32_t next =
                selected_topic.next_partition.fetch_add(1, std::memory_order_relaxed);
            selected_id = static_cast<PartitionId>(next % selected_topic.partitions.size());
        }
    }

    if (selected_id < 0 ||
        static_cast<std::size_t>(selected_id) >= selected_topic.partitions.size()) {
        return Status::NotFound("partition not found");
    }
    return PartitionSelection{
        selected_id,
        selected_topic.partitions[static_cast<std::size_t>(selected_id)].get()};
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

// data_dir/orders/0/
std::filesystem::path TopicManager::PartitionPath(const TopicName& topic,
                                                  PartitionId partition_id) const {
    return options_.data_dir / topic / std::to_string(partition_id);
}

std::filesystem::path TopicManager::MetadataPath() const {
    return options_.data_dir / "topics.meta";
}

std::filesystem::path TopicManager::MetadataTempPath() const {
    return options_.data_dir / "topics.meta.tmp";
}

// 打开所有 partition 的 CommitLog，构造尚未发布到 topics_ 的 Topic 对象。
Result<std::unique_ptr<Topic>> TopicManager::OpenTopic(const TopicName& topic,
                                                       std::uint32_t partition_count) const {
    auto created = std::make_unique<Topic>();
    created->name = topic;
    created->partitions.reserve(partition_count);

    for (std::uint32_t id = 0; id < partition_count; ++id) {
        CommitLogOptions log_options;
        log_options.data_dir = PartitionPath(topic, static_cast<PartitionId>(id));
        log_options.segment_bytes = options_.segment_bytes;
        log_options.index_interval_bytes = options_.index_interval_bytes;

        // data_dir/orders/0/      get a safety commitlog;
        auto log = CommitLog::Open(std::move(log_options));
        if (!log.ok()) {
            return log.status();
        }

        auto partition = std::make_unique<Partition>(Partition{static_cast<PartitionId>(id), std::move(log).value()});
        created->partitions.push_back(std::move(partition));
    }

    return std::move(created);
}

Status TopicManager::PersistMetadataLocked() const {
    std::error_code error;
    std::filesystem::create_directories(options_.data_dir, error);
    if (error) {
        return FilesystemError("create topic data directory", error);
    }

    const std::filesystem::path temp_path = MetadataTempPath();
    {
        std::ofstream output(temp_path, std::ios::trunc);
        if (!output.is_open()) {
            return Status::IoError("open topic metadata temp " + temp_path.string());
        }

        output << kTopicMetadataMagic << '\n';

        std::vector<TopicMetadata> metadata;
        metadata.reserve(topics_.size());
        for (const auto& [name, topic] : topics_) {
            metadata.push_back(
                TopicMetadata{name, static_cast<std::uint32_t>(topic->partitions.size())});
        }
        std::sort(metadata.begin(), metadata.end(),
                  [](const TopicMetadata& left, const TopicMetadata& right) {
                      return left.topic < right.topic;
                  });

        for (const TopicMetadata& topic : metadata) {
            output << topic.topic << ' ' << topic.partition_count << '\n';
        }
        output.flush();
        if (!output.good()) {
            return Status::IoError("write topic metadata temp " + temp_path.string());
        }
    }

    std::filesystem::rename(temp_path, MetadataPath(), error);
    if (error) {
        return FilesystemError("rename topic metadata", error);
    }
    return Status::Ok();
}

TopicManagerOptions TopicManagerOptionsFromConfig(const StorageConfig& config) {
    TopicManagerOptions options;
    options.data_dir = config.data_dir;
    options.segment_bytes = config.segment_bytes;
    return options;
}

}  // namespace logmq
