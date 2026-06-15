#include "logmq/broker/offset_store.h"

#include <cctype>
#include <fstream>
#include <functional>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>

namespace logmq {
namespace {

constexpr std::string_view kOffsetMetadataMagic = "logmq_consumer_offset_v1";

Status ValidateSafeName(std::string_view name, std::string_view field) {
    if (name.empty()) {
        return Status::InvalidArgument(std::string(field) + " must not be empty");
    }
    if (name == "." || name == "..") {
        return Status::InvalidArgument(std::string(field) + " must not be . or ..");
    }
    for (unsigned char ch : name) {
        if (std::isalnum(ch) || ch == '.' || ch == '_' || ch == '-') {
            continue;
        }
        return Status::InvalidArgument(
            std::string(field) + " may only contain letters, digits, '.', '_' and '-'");
    }
    return Status::Ok();
}

Status FilesystemError(std::string_view operation, const std::error_code& error) {
    return Status::IoError(std::string(operation) + ": " + error.message());
}

// 从 offset 文件名中解析 topic 和 partition   eg. order-0.offset
bool ParseOffsetFilename(const std::filesystem::path& path,
                         TopicName& topic,
                         PartitionId& partition_id) {
    if (path.extension() != ".offset") {
        return false;
    }

    const std::string stem = path.stem().string();
    const std::size_t delimiter = stem.rfind('-');
    if (delimiter == std::string::npos || delimiter == 0 || delimiter + 1 >= stem.size()) {
        return false;
    }

    const std::string partition = stem.substr(delimiter + 1);
    PartitionId parsed = 0;
    for (char ch : partition) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        const int digit = ch - '0';
        if (parsed > (std::numeric_limits<PartitionId>::max() - digit) / 10) {
            return false;
        }
        parsed = parsed * 10 + digit;
    }

    topic = stem.substr(0, delimiter);
    partition_id = parsed;
    return true;
}

}  // namespace

bool ConsumerOffsetKey::operator==(const ConsumerOffsetKey& other) const {
    return group_id == other.group_id && topic == other.topic &&
           partition_id == other.partition_id;
}

std::size_t ConsumerOffsetKeyHash::operator()(const ConsumerOffsetKey& key) const {
    std::size_t seed = std::hash<std::string>{}(key.group_id);
    seed ^= std::hash<std::string>{}(key.topic) + 0x9e3779b9U + (seed << 6U) + (seed >> 2U);
    seed ^= std::hash<PartitionId>{}(key.partition_id) + 0x9e3779b9U + (seed << 6U) +
            (seed >> 2U);
    return seed;
}

OffsetStore::OffsetStore(OffsetStoreOptions options) : options_(std::move(options)) {}

Status OffsetStore::Load() {
    std::error_code error;
    std::filesystem::create_directories(RootPath(), error);
    if (error) {
        return FilesystemError("create consumer offset directory", error);
    }

    std::unordered_map<ConsumerOffsetKey, Offset, ConsumerOffsetKeyHash> loaded;
    for (const auto& group_entry : std::filesystem::directory_iterator(RootPath(), error)) {
        if (error) {
            return FilesystemError("scan consumer offset directory", error);
        }
        if (!group_entry.is_directory()) {
            continue;
        }

        const std::string group_id = group_entry.path().filename().string();
        Status status = ValidateSafeName(group_id, "group_id");
        if (!status.ok()) {
            return status;
        }

        for (const auto& offset_entry :
             std::filesystem::directory_iterator(group_entry.path(), error)) {
            if (error) {
                return FilesystemError("scan consumer offset group directory", error);
            }
            if (!offset_entry.is_regular_file()) {
                continue;
            }

            TopicName topic;
            PartitionId partition_id = kInvalidPartitionId;
            if (!ParseOffsetFilename(offset_entry.path(), topic, partition_id)) {
                continue;
            }
            status = ValidateSafeName(topic, "topic");
            if (!status.ok()) {
                return status;
            }

            std::ifstream input(offset_entry.path());
            if (!input.is_open()) {
                return Status::IoError("open consumer offset " + offset_entry.path().string());
            }

            std::string magic;
            if (!std::getline(input, magic)) {
                return Status::Corruption("consumer offset file is empty");
            }
            if (magic != kOffsetMetadataMagic) {
                return Status::Corruption("consumer offset file has unsupported format");
            }

            std::string file_group;
            TopicName file_topic;
            std::int64_t file_partition = kInvalidPartitionId;
            std::int64_t file_offset = kInvalidOffset;
            std::string trailing;
            if (!(input >> file_group >> file_topic >> file_partition >> file_offset) ||
                (input >> trailing)) {
                return Status::Corruption("invalid consumer offset file body");
            }
            if (file_group != group_id || file_topic != topic ||
                file_partition != partition_id) {
                return Status::Corruption("consumer offset file does not match path");
            }
            status = ValidateSafeName(file_topic, "topic");
            if (!status.ok()) {
                return status;
            }
            if (file_offset < 0) {
                return Status::Corruption("consumer offset must not be negative");
            }

            loaded.emplace(ConsumerOffsetKey{file_group, file_topic,
                                             static_cast<PartitionId>(file_partition)},
                           static_cast<Offset>(file_offset));
        }
        if (error) {
            return FilesystemError("scan consumer offset group directory", error);
        }
    }
    if (error) {
        return FilesystemError("scan consumer offset directory", error);
    }

    std::unique_lock lock(mutex_);
    offsets_ = std::move(loaded);
    return Status::Ok();
}

Status OffsetStore::Commit(std::string group_id,
                           TopicName topic,
                           PartitionId partition_id,
                           Offset offset) {
    Status status = ValidateSafeName(group_id, "group_id");
    if (!status.ok()) {
        return status;
    }
    status = ValidateSafeName(topic, "topic");
    if (!status.ok()) {
        return status;
    }
    if (partition_id < 0) {
        return Status::InvalidArgument("partition_id must not be negative");
    }
    if (offset < 0) {
        return Status::InvalidArgument("offset must not be negative");
    }

    ConsumerOffsetKey key{std::move(group_id), std::move(topic), partition_id};

    std::unique_lock lock(mutex_);
    status = PersistLocked(key, offset);
    if (!status.ok()) {
        return status;
    }
    offsets_[std::move(key)] = offset;
    return Status::Ok();
}

Result<std::optional<Offset>> OffsetStore::Fetch(std::string_view group_id,
                                                 std::string_view topic,
                                                 PartitionId partition_id) const {
    Status status = ValidateSafeName(group_id, "group_id");
    if (!status.ok()) {
        return status;
    }
    status = ValidateSafeName(topic, "topic");
    if (!status.ok()) {
        return status;
    }
    if (partition_id < 0) {
        return Status::InvalidArgument("partition_id must not be negative");
    }

    ConsumerOffsetKey key{std::string(group_id), TopicName(topic), partition_id};
    std::shared_lock lock(mutex_);
    auto found = offsets_.find(key);
    if (found == offsets_.end()) {
        return std::optional<Offset>{};
    }
    return std::optional<Offset>{found->second};
}

std::filesystem::path OffsetStore::RootPath() const {
    return options_.data_dir / "__consumer_offsets";
}

std::filesystem::path OffsetStore::GroupPath(std::string_view group_id) const {
    return RootPath() / std::string(group_id);
}

std::filesystem::path OffsetStore::OffsetPath(std::string_view group_id,
                                              std::string_view topic,
                                              PartitionId partition_id) const {
    return GroupPath(group_id) /
           (std::string(topic) + "-" + std::to_string(partition_id) + ".offset");
}

std::filesystem::path OffsetStore::OffsetTempPath(std::string_view group_id,
                                                  std::string_view topic,
                                                  PartitionId partition_id) const {
    return GroupPath(group_id) /
           (std::string(topic) + "-" + std::to_string(partition_id) + ".offset.tmp");
}

Status OffsetStore::PersistLocked(const ConsumerOffsetKey& key, Offset offset) const {
    std::error_code error;
    std::filesystem::create_directories(GroupPath(key.group_id), error);
    if (error) {
        return FilesystemError("create consumer offset group directory", error);
    }

    const std::filesystem::path temp_path =
        OffsetTempPath(key.group_id, key.topic, key.partition_id);
    {
        std::ofstream output(temp_path, std::ios::trunc);
        if (!output.is_open()) {
            return Status::IoError("open consumer offset temp " + temp_path.string());
        }
        output << kOffsetMetadataMagic << '\n';
        output << key.group_id << ' ' << key.topic << ' ' << key.partition_id << ' '
               << offset << '\n';
        output.flush();
        if (!output.good()) {
            return Status::IoError("write consumer offset temp " + temp_path.string());
        }
    }

    std::filesystem::rename(temp_path, OffsetPath(key.group_id, key.topic, key.partition_id),
                            error);
    if (error) {
        return FilesystemError("rename consumer offset", error);
    }
    return Status::Ok();
}

OffsetStoreOptions OffsetStoreOptionsFromDataDir(const std::filesystem::path& data_dir) {
    OffsetStoreOptions options;
    options.data_dir = data_dir;
    return options;
}

}  // namespace logmq
