#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "logmq/base/result.h"

namespace logmq {

// A key-value record    -[timestamp key value crc]
struct Record {
    std::uint64_t timestamp{0};
    std::string key;
    std::string value;
    std::uint32_t crc32{0};
};

struct DecodedRecord {
    Record record;
    std::size_t bytes_consumed{0};
};

// Size of the fixed header in bytes.
inline constexpr std::size_t kRecordHeaderBytes =
    sizeof(std::uint64_t) + sizeof(std::uint32_t) + sizeof(std::uint32_t) + sizeof(std::uint32_t);

// Returns a total encoded size of a record in bytes,
// including the fixed header and key/value payloads.
[[nodiscard]] std::size_t EncodedRecordSize(const Record& record);

// Compute a CRC checksum of a record,
// including timestamp, the sizes and contents of key and value.
[[nodiscard]] std::uint32_t ComputeRecordCrc(const Record& record);

// Append the encoded record to the buffer as raw bytes.
Status AppendEncodedRecord(const Record& record, std::vector<std::byte>& output);

// Decode a record and check CRC.
[[nodiscard]] Result<DecodedRecord> DecodeRecord(std::span<const std::byte> data);

}  // namespace logmq
