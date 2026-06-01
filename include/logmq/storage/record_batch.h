#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "logmq/base/result.h"
#include "logmq/base/types.h"
#include "logmq/storage/record.h"

namespace logmq {

// A batch of records. 
struct RecordBatch {
    Offset base_offset{kInvalidOffset};
    std::uint64_t record_count{0};
    std::uint64_t batch_bytes{0};
    std::uint32_t batch_crc32{0};
    std::vector<Record> records;
};

// Magic number identifying a RecordBatch.
inline constexpr std::uint32_t kRecordBatchMagic = 0x4C4D5142;
// Size of the fixed record batch header in bytes,
// including magic, RecordBatch.
inline constexpr std::size_t kRecordBatchHeaderBytes =
    sizeof(std::uint32_t) + sizeof(std::uint64_t) + sizeof(std::uint64_t) + sizeof(std::uint64_t) +
    sizeof(std::uint32_t);

// Construct a RecordBatch from the base_offset and records.
[[nodiscard]] Result<RecordBatch> MakeRecordBatch(Offset base_offset, std::vector<Record> records);

// Encode the batch into a raw byte buffer. 
[[nodiscard]] Result<std::vector<std::byte>> EncodeRecordBatch(const RecordBatch& batch);

// Read the total size of the batch.
[[nodiscard]] Result<std::uint64_t> PeekRecordBatchBytes(std::span<const std::byte> data);

// Decode raw bytes into a RecordBatch.
[[nodiscard]] Result<RecordBatch> DecodeRecordBatch(std::span<const std::byte> data);

}  // namespace logmq
