#include "logmq/storage/commit_log.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <utility>

namespace logmq {
namespace {

// Parse offset from file name.
bool ParseBaseOffset(const std::filesystem::path& path, Offset& offset) {
    const std::string name = path.stem().string();
    if (name.empty()) {
        return false;
    }

    Offset parsed = 0;
    for (const char ch : name) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        const int digit = ch - '0';
        if (parsed > (std::numeric_limits<Offset>::max() - digit) / 10) {
            return false;
        }
        parsed = parsed * 10 + digit;
    }

    offset = parsed;
    return true;
}

bool BatchContainsOffset(const RecordBatch& batch, Offset offset) {
    if (batch.base_offset < 0) {
        return false;
    }

    const auto count = static_cast<Offset>(batch.record_count);
    return batch.base_offset <= offset && offset < batch.base_offset + count;
}

}  // namespace

CommitLog::CommitLog(CommitLogOptions options) : options_(std::move(options)) {}

CommitLog::CommitLog(CommitLog&& other) noexcept {
    std::lock_guard<std::mutex> lock(other.mutex_);
    options_ = std::move(other.options_);
    segments_ = std::move(other.segments_);
    indexes_ = std::move(other.indexes_);
    next_offset_ = other.next_offset_;
    closed_ = other.closed_;
    other.closed_ = true;
}

CommitLog& CommitLog::operator=(CommitLog&& other) noexcept {
    if (this != &other) {
        std::scoped_lock lock(mutex_, other.mutex_);
        options_ = std::move(other.options_);
        segments_ = std::move(other.segments_);
        indexes_ = std::move(other.indexes_);
        next_offset_ = other.next_offset_;
        closed_ = other.closed_;
        other.closed_ = true;
    }
    return *this;
}

// 重启/恢复 一个 Partition 里的 Segemnts;
Result<CommitLog> CommitLog::Open(CommitLogOptions options) {
    if (options.data_dir.empty()) {
        return Status::InvalidArgument("commit log data_dir must not be empty");
    }
    if (options.segment_bytes < kRecordBatchHeaderBytes) {
        return Status::InvalidArgument("commit log segment_bytes is too small");
    }
    if (options.index_interval_bytes == 0) {
        options.index_interval_bytes = 1;
    }

    std::error_code error;
    std::filesystem::create_directories(options.data_dir, error);
    if (error) {
        return Status::IoError("create commit log directory: " + error.message());
    }

    CommitLog log(std::move(options));
    Status status = log.LoadOrCreateSegments();
    if (!status.ok()) {
        return status;
    }

    log.closed_ = false;
    return std::move(log);
}

Result<AppendResult> CommitLog::Append(std::vector<Record> records) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (closed_) {
        return Status::InvalidArgument("append on closed commit log");
    }
    if (records.empty()) {
        return Status::InvalidArgument("commit log append requires at least one record");
    }
    if (records.size() >
        static_cast<std::size_t>(std::numeric_limits<Offset>::max() - next_offset_)) {
        return Status::InvalidArgument("commit log append would overflow offset");
    }

    auto batch = MakeRecordBatch(next_offset_, std::move(records));
    if (!batch.ok()) {
        return batch.status();
    }

    Segment& active = segments_.back();
    if (active.write_position() > 0 &&
        active.write_position() + batch.value().batch_bytes > options_.segment_bytes) {
        Status status = RollSegment(batch.value().base_offset);
        if (!status.ok()) {
            return status;
        }
    }

    Segment& target = segments_.back();
    OffsetIndex& index = indexes_.back();

    auto position = target.Append(batch.value());
    if (!position.ok()) {
        return position.status();
    }

    Status status = index.MaybeAppend(batch.value().base_offset, position.value());
    if (!status.ok()) {
        return status;
    }

    next_offset_ += static_cast<Offset>(batch.value().record_count);
    return AppendResult{batch.value().base_offset, position.value(), target.path()};
}

Result<RecordBatch> CommitLog::Read(Offset offset, std::size_t max_bytes) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (closed_) {
        return Status::InvalidArgument("read on closed commit log");
    }
    if (offset < 0) {
        return Status::InvalidArgument("commit log read offset must not be negative");
    }
    if (offset >= next_offset_) {
        return Status::NotFound("commit log offset is not readable");
    }

    auto segment_index = FindSegmentIndex(offset);
    if (!segment_index.ok()) {
        return segment_index.status();
    }

    const Segment& segment = segments_[segment_index.value()];
    const OffsetIndex& index = indexes_[segment_index.value()];

    std::uint64_t position = 0;
    auto floor_entry = index.FindFloor(offset);
    if (floor_entry.ok()) {
        position = floor_entry.value().position;
    } else if (floor_entry.status().code() != ErrorCode::kNotFound) {
        return floor_entry.status();
    }

    while (position < segment.write_position()) {
        auto batch = segment.Read(position, max_bytes);
        if (!batch.ok()) {
            return batch.status();
        }
        if (BatchContainsOffset(batch.value(), offset)) {
            return batch.value();
        }
        position += batch.value().batch_bytes;
    }

    return Status::NotFound("commit log offset was not found in segment");
}

Status CommitLog::Flush() const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (closed_) {
        return Status::InvalidArgument("flush on closed commit log");
    }

    for (const Segment& segment : segments_) {
        Status status = segment.Flush();
        if (!status.ok()) {
            return status;
        }
    }
    for (const OffsetIndex& index : indexes_) {
        Status status = index.Rewrite();
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}

Status CommitLog::Close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (closed_) {
        return Status::Ok();
    }

    for (OffsetIndex& index : indexes_) {
        Status status = index.Rewrite();
        if (!status.ok()) {
            return status;
        }
    }
    for (Segment& segment : segments_) {
        Status status = segment.Close();
        if (!status.ok()) {
            return status;
        }
    }
    closed_ = true;
    return Status::Ok();
}

Offset CommitLog::next_offset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return next_offset_;
}

std::size_t CommitLog::segment_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return segments_.size();
}

Status CommitLog::LoadOrCreateSegments() {
    // Segment file order and path pair list.
    std::vector<std::pair<Offset, std::filesystem::path>> logs;
    // for(): Scan directory files to build logs. (Disorder)
    for (const auto& entry : std::filesystem::directory_iterator(options_.data_dir)) {
        // Only 
        if (!entry.is_regular_file() || entry.path().extension() != ".log") {
            continue;
        }

        Offset base_offset = kInvalidOffset;
        if (!ParseBaseOffset(entry.path(), base_offset)) {
            continue;
        }
        logs.emplace_back(base_offset, entry.path());
    }
    // Order logs.
    std::sort(logs.begin(), logs.end(),
              [](const auto& left, const auto& right) { return left.first < right.first; });

    if (logs.empty()) {
        return RollSegment(0);
    }

    for (const auto& [base_offset, path] : logs) {
        auto segment = Segment::Open(path, base_offset);
        if (!segment.ok()) {
            return segment.status();
        }

        OffsetIndex index(IndexPath(base_offset), base_offset, options_.index_interval_bytes);
        auto recovery = RecoverSegment(segment.value(), index);
        if (!recovery.ok()) {
            return recovery.status();
        }

        next_offset_ = std::max(next_offset_, recovery.value().next_offset);
        segments_.push_back(std::move(segment).value());
        indexes_.push_back(std::move(index));
    }

    if (segments_.empty()) {
        return RollSegment(0);
    }
    return Status::Ok();
}

Result<CommitLog::RecoveryResult> CommitLog::RecoverSegment(Segment& segment,
                                                            OffsetIndex& index) {
    index.Clear();

    std::uint64_t position = 0;
    Offset recovered_next_offset = segment.base_offset();
    const std::uint64_t file_size = segment.write_position();

    while (position < file_size) {
        const std::uint64_t remaining = file_size - position;
        if (remaining > std::numeric_limits<std::size_t>::max()) {
            return Status::InvalidArgument("segment is too large to recover on this platform");
        }

        auto batch = segment.Read(position, static_cast<std::size_t>(remaining));
        if (!batch.ok()) {
            break;
        }

        Status status = index.MaybeAppend(batch.value().base_offset, position);
        if (!status.ok()) {
            return status;
        }

        position += batch.value().batch_bytes;
        recovered_next_offset = std::max(
            recovered_next_offset,
            batch.value().base_offset + static_cast<Offset>(batch.value().record_count));
    }

    if (position != file_size) {
        Status status = segment.Truncate(position);
        if (!status.ok()) {
            return status;
        }
    }

    Status status = index.Rewrite();
    if (!status.ok()) {
        return status;
    }

    return RecoveryResult{recovered_next_offset};
}

Result<std::size_t> CommitLog::FindSegmentIndex(Offset offset) const {
    if (segments_.empty()) {
        return Status::NotFound("commit log has no segments");
    }

    auto it = std::upper_bound(
        segments_.begin(), segments_.end(), offset,
        [](Offset value, const Segment& segment) { return value < segment.base_offset(); });

    if (it == segments_.begin()) {
        return Status::NotFound("commit log offset is before first segment");
    }

    return static_cast<std::size_t>((it - 1) - segments_.begin());
}

Status CommitLog::RollSegment(Offset base_offset) {
    auto segment = Segment::Open(SegmentPath(base_offset), base_offset);
    if (!segment.ok()) {
        return segment.status();
    }

    segments_.push_back(std::move(segment).value());
    indexes_.emplace_back(IndexPath(base_offset), base_offset, options_.index_interval_bytes);
    return indexes_.back().Rewrite();
}

// Generates a fixed format index file name based on offset.
// example: /lorixyu/data/0000000000000000521.log
std::filesystem::path CommitLog::SegmentPath(Offset base_offset) const {
    std::ostringstream name;
    name << std::setw(20) << std::setfill('0') << base_offset << ".log";
    return options_.data_dir / name.str();
}

// Generates a fixed format index file name based on offset.
// example: /lorixyu/data/0000000000000000521.index
std::filesystem::path CommitLog::IndexPath(Offset base_offset) const {
    std::ostringstream name;
    name << std::setw(20) << std::setfill('0') << base_offset << ".index";
    return options_.data_dir / name.str();
}

}  // namespace logmq
