#pragma once

#include <filesystem>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "logmq/base/result.h"
#include "logmq/base/types.h"

namespace logmq {

struct OffsetStoreOptions {
    std::filesystem::path data_dir{"./data"};
};

// 某个 group 在指定partition_id上的消费进度
struct ConsumerOffsetKey {
    std::string group_id;
    TopicName topic;
    PartitionId partition_id{kInvalidPartitionId};

    [[nodiscard]] bool operator==(const ConsumerOffsetKey& other) const;
};

struct ConsumerOffsetKeyHash {
    [[nodiscard]] std::size_t operator()(const ConsumerOffsetKey& key) const;
};

class OffsetStore {
public:
    explicit OffsetStore(OffsetStoreOptions options);

    [[nodiscard]] Status Load();

    [[nodiscard]] Status Commit(std::string group_id,
                                TopicName topic,
                                PartitionId partition_id,
                                Offset offset);

    [[nodiscard]] Result<std::optional<Offset>> Fetch(std::string_view group_id,
                                                       std::string_view topic,
                                                       PartitionId partition_id) const;

private:
    [[nodiscard]] std::filesystem::path RootPath() const;
    [[nodiscard]] std::filesystem::path GroupPath(std::string_view group_id) const;
    [[nodiscard]] std::filesystem::path OffsetPath(std::string_view group_id,
                                                   std::string_view topic,
                                                   PartitionId partition_id) const;
    [[nodiscard]] std::filesystem::path OffsetTempPath(std::string_view group_id,
                                                       std::string_view topic,
                                                       PartitionId partition_id) const;
    [[nodiscard]] Status PersistLocked(const ConsumerOffsetKey& key, Offset offset) const;

    OffsetStoreOptions options_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<ConsumerOffsetKey, Offset, ConsumerOffsetKeyHash> offsets_;
};

[[nodiscard]] OffsetStoreOptions OffsetStoreOptionsFromDataDir(
    const std::filesystem::path& data_dir);

}  // namespace logmq
