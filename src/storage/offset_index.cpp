#include "logmq/storage/offset_index.h"

#include <fcntl.h>

#include <algorithm>
#include <type_traits>

#include "logmq/storage/file.h"

namespace logmq {
namespace {

inline constexpr std::size_t kIndexEntryBytes = sizeof(std::uint64_t) + sizeof(std::uint64_t);

template <typename UInt>
void AppendLittleEndian(UInt value, std::vector<std::byte>& output) {
    static_assert(std::is_unsigned_v<UInt>);

    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        output.push_back(static_cast<std::byte>((value >> (i * 8U)) & 0xFFU));
    }
}

std::vector<std::byte> EncodeEntry(const IndexEntry& entry) {
    std::vector<std::byte> bytes;
    bytes.reserve(kIndexEntryBytes);
    AppendLittleEndian<std::uint64_t>(static_cast<std::uint64_t>(entry.offset), bytes);
    AppendLittleEndian<std::uint64_t>(entry.position, bytes);
    return bytes;
}

}  // namespace

OffsetIndex::OffsetIndex(std::filesystem::path path, Offset base_offset,
                         std::uint64_t interval_bytes)
    : path_(std::move(path)),
      base_offset_(base_offset),
      interval_bytes_(interval_bytes == 0 ? 1 : interval_bytes) {}

Result<IndexEntry> OffsetIndex::FindFloor(Offset offset) const {
    if (entries_.empty() || offset < entries_.front().offset) {
        return Status::NotFound("offset index has no floor entry");
    }

    const auto it = std::upper_bound(
        entries_.begin(), entries_.end(), offset,
        [](Offset value, const IndexEntry& entry) { return value < entry.offset; });

    return *(it - 1);
}

Status OffsetIndex::MaybeAppend(Offset offset, std::uint64_t position) {
    if (base_offset_ < 0) {
        return Status::InvalidArgument("offset index base_offset must not be negative");
    }
    if (offset < base_offset_) {
        return Status::InvalidArgument("offset index entry is before base_offset");
    }
    if (!entries_.empty()) {
        const IndexEntry& last = entries_.back();
        if (offset <= last.offset) {
            return Status::InvalidArgument("offset index entries must be increasing");
        }
        if (position < last.position) {
            return Status::InvalidArgument("offset index positions must be increasing");
        }
        if (position - last.position < interval_bytes_) {
            return Status::Ok();
        }
    }

    entries_.push_back(IndexEntry{offset, position});
    return Status::Ok();
}

void OffsetIndex::Clear() { entries_.clear(); }

Status OffsetIndex::Rewrite() const {
    auto file = File::Open(path_, O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (!file.ok()) {
        return file.status();
    }

    std::vector<std::byte> bytes;
    bytes.reserve(entries_.size() * kIndexEntryBytes);
    for (const IndexEntry& entry : entries_) {
        std::vector<std::byte> encoded = EncodeEntry(entry);
        bytes.insert(bytes.end(), encoded.begin(), encoded.end());
    }

    Status status = file.value().PWrite(bytes, 0);
    if (!status.ok()) {
        return status;
    }

    return file.value().Fsync();
}

std::size_t OffsetIndex::size() const { return entries_.size(); }

const std::filesystem::path& OffsetIndex::path() const { return path_; }

}  // namespace logmq
