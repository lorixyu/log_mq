#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "logmq/base/result.h"
#include "logmq/base/types.h"
#include "logmq/storage/file.h"
#include "logmq/storage/record_batch.h"

namespace logmq {

class Segment {
public:
    Segment() = default;

    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;

    Segment(Segment&&) noexcept = default;
    Segment& operator=(Segment&&) noexcept = default;

    static Result<Segment> Open(const std::filesystem::path& path, Offset base_offset);

    // Appends the decoded batch into the segment,
    // returning new write position.
    [[nodiscard]] Result<std::uint64_t> Append(const RecordBatch& batch);

    // Reads a RecordBatch by a specified position.
    [[nodiscard]] Result<RecordBatch> Read(std::uint64_t position, std::size_t max_bytes) const;

    Status Flush() const;

    Status Truncate(std::uint64_t size);

    Status Close();

    [[nodiscard]] Offset base_offset() const;

    [[nodiscard]] std::uint64_t write_position() const;

    [[nodiscard]] const std::filesystem::path& path() const;

private:
    Segment(std::filesystem::path path, Offset base_offset, File file,
            std::uint64_t write_position);

    std::filesystem::path path_;
    Offset base_offset_{kInvalidOffset};
    std::uint64_t write_position_{0};
    File file_;
    bool closed_{true};
};

}  // namespace logmq
