#include "logmq/storage/segment.h"

#include <array>

namespace logmq {

Segment::Segment(std::filesystem::path path, 
                 Offset base_offset, File file,
                 std::uint64_t write_position)
    : path_(std::move(path)),
      base_offset_(base_offset),
      write_position_(write_position),
      file_(std::move(file)),
      closed_(false) {}

Result<Segment> Segment::Open(const std::filesystem::path& path, Offset base_offset) {
    if (base_offset < 0) {
        return Status::InvalidArgument("segment base_offset must not be negative");
    }

    auto file = File::Open(path);
    if (!file.ok()) {
        return file.status();
    }

    File opened_file = std::move(file).value();
    auto file_size = opened_file.Size();
    if (!file_size.ok()) {
        return file_size.status();
    }

    return Segment(path, base_offset, std::move(opened_file), file_size.value());
}

Result<std::uint64_t> Segment::Append(const RecordBatch& batch) {
    if (closed_) {
        return Status::InvalidArgument("append on closed segment");
    }

    auto encoded = EncodeRecordBatch(batch);
    if (!encoded.ok()) {
        return encoded.status();
    }

    const std::uint64_t position = write_position_;
    Status status = file_.PWrite(encoded.value(), position);
    if (!status.ok()) {
        return status;
    }

    write_position_ += encoded.value().size();
    return position;
}

Result<RecordBatch> Segment::Read(std::uint64_t position, std::size_t max_bytes) const {
    if (closed_) {
        return Status::InvalidArgument("read on closed segment");
    }
    if (max_bytes < kRecordBatchHeaderBytes) {
        return Status::InvalidArgument("max_bytes is smaller than record batch header");
    }

    std::array<std::byte, kRecordBatchHeaderBytes> header{};
    auto header_bytes = file_.PRead(header, position);
    if (!header_bytes.ok()) {
        return header_bytes.status();
    }
    if (header_bytes.value() == 0) {
        return Status::NotFound("record batch position is not readable");
    }
    if (header_bytes.value() != header.size()) {
        return Status::Corruption("record batch header is truncated");
    }

    auto batch_bytes = PeekRecordBatchBytes(header);
    if (!batch_bytes.ok()) {
        return batch_bytes.status();
    }
    if (batch_bytes.value() > max_bytes) {
        return Status::InvalidArgument("record batch is larger than max_bytes");
    }

    std::vector<std::byte> encoded(batch_bytes.value());
    auto bytes_read = file_.PRead(encoded, position);
    if (!bytes_read.ok()) {
        return bytes_read.status();
    }
    if (bytes_read.value() != encoded.size()) {
        return Status::Corruption("record batch payload is truncated");
    }

    return DecodeRecordBatch(encoded);
}

Status Segment::Flush() const { return file_.Fsync(); }

Status Segment::Close() {
    closed_ = true;
    return file_.Close();
}

Offset Segment::base_offset() const { return base_offset_; }

std::uint64_t Segment::write_position() const { return write_position_; }

const std::filesystem::path& Segment::path() const { return path_; }

}  // namespace logmq
