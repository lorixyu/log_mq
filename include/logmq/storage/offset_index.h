#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <vector>

#include "logmq/base/result.h"
#include "logmq/base/types.h"

namespace logmq {

struct IndexEntry {
    Offset offset{kInvalidOffset};
    std::uint64_t position{0};
};

class OffsetIndex {
public:
    OffsetIndex() = default;

    OffsetIndex(std::filesystem::path path, Offset base_offset, std::uint64_t interval_bytes);

    [[nodiscard]] Result<IndexEntry> FindFloor(Offset offset) const;

    Status MaybeAppend(Offset offset, std::uint64_t position);

    void Clear();

    Status Rewrite() const;

    [[nodiscard]] std::size_t size() const;

    [[nodiscard]] const std::filesystem::path& path() const;

private:
    std::filesystem::path path_;
    Offset base_offset_{kInvalidOffset};
    std::uint64_t interval_bytes_{4096};
    std::vector<IndexEntry> entries_;
};

}  // namespace logmq
