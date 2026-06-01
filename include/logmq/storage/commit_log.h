#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <vector>

#include "logmq/base/result.h"
#include "logmq/base/types.h"
#include "logmq/storage/offset_index.h"
#include "logmq/storage/record.h"
#include "logmq/storage/record_batch.h"
#include "logmq/storage/segment.h"

namespace logmq {

// Information from the segment file.
// Including direction, size, and index structure.
struct CommitLogOptions {
    std::filesystem::path data_dir;                  // Data directory
    std::uint64_t segment_bytes{128 * 1024 * 1024};  // The max size of a segment file
    std::uint64_t index_interval_bytes{4 * 1024};    // The interval of index. Single segment file (local).
};

struct AppendResult {
    Offset base_offset{kInvalidOffset};  // segment order
    std::uint64_t position{0};           // Single segment file
    std::filesystem::path segment_path;  // segment path
};

class CommitLog {
public:
    CommitLog(const CommitLog&) = delete;
    CommitLog& operator=(const CommitLog&) = delete;

    CommitLog(CommitLog&& other) noexcept;
    CommitLog& operator=(CommitLog&& other) noexcept;

    static Result<CommitLog> Open(CommitLogOptions options);

    [[nodiscard]] Result<AppendResult> Append(std::vector<Record> records);

    [[nodiscard]] Result<RecordBatch> Read(Offset offset, std::size_t max_bytes) const;

    Status Flush() const;

    Status Close();

    [[nodiscard]] Offset next_offset() const;

    [[nodiscard]] std::size_t segment_count() const;

private:
    struct RecoveryResult {
        Offset next_offset{kInvalidOffset};
    };

    explicit CommitLog(CommitLogOptions options);

    Status LoadOrCreateSegments();

    Result<RecoveryResult> RecoverSegment(Segment& segment, OffsetIndex& index);

    Result<std::size_t> FindSegmentIndex(Offset offset) const;

    Status RollSegment(Offset base_offset);

    [[nodiscard]] std::filesystem::path SegmentPath(Offset base_offset) const;

    [[nodiscard]] std::filesystem::path IndexPath(Offset base_offset) const;

    CommitLogOptions options_;
    std::vector<Segment> segments_;
    std::vector<OffsetIndex> indexes_;
    Offset next_offset_{0};
    bool closed_{true};
    mutable std::mutex mutex_;
};

}  // namespace logmq
