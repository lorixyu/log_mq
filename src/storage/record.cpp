#include "logmq/storage/record.h"

#include <limits>
#include <type_traits>

#include "logmq/storage/crc32.h"

namespace logmq {
namespace {

// Serialize value using little_endian order
template <typename UInt>
void AppendLittleEndian(UInt value, std::vector<std::byte>& output) {
    static_assert(std::is_unsigned_v<UInt>);

    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        output.push_back(static_cast<std::byte>((value >> (i * 8U)) & 0xFFU));
    }
}

template <typename UInt>
Result<UInt> ReadLittleEndian(std::span<const std::byte> data, std::size_t offset) {
    static_assert(std::is_unsigned_v<UInt>);

    if (data.size() < offset + sizeof(UInt)) {
        return Status::Corruption("record header is truncated");
    }

    UInt value = 0;
    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        value |= static_cast<UInt>(std::to_integer<std::uint8_t>(data[offset + i])) << (i * 8U);
    }
    return value;
}

// Append the string data to the output buffer as raw bytes.
void AppendStringBytes(const std::string& value, std::vector<std::byte>& output) {
    const auto* begin = reinterpret_cast<const std::byte*>(value.data());
    output.insert(output.end(), begin, begin + value.size());
}

Status ValidateStringSize(const std::string& name, const std::string& value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument(name + " is too large");
    }
    return Status::Ok();
}

}  // namespace

std::size_t EncodedRecordSize(const Record& record) {
    return kRecordHeaderBytes + record.key.size() + record.value.size();
}

std::uint32_t ComputeRecordCrc(const Record& record) {
    std::vector<std::byte> bytes;
    bytes.reserve(sizeof(std::uint64_t) + sizeof(std::uint32_t) + sizeof(std::uint32_t) +
                  record.key.size() + record.value.size());

    AppendLittleEndian<std::uint64_t>(record.timestamp, bytes);
    AppendLittleEndian<std::uint32_t>(static_cast<std::uint32_t>(record.key.size()), bytes);
    AppendLittleEndian<std::uint32_t>(static_cast<std::uint32_t>(record.value.size()), bytes);
    AppendStringBytes(record.key, bytes);
    AppendStringBytes(record.value, bytes);

    return Crc32(bytes);
}

Status AppendEncodedRecord(const Record& record, std::vector<std::byte>& output) {
    Status status = ValidateStringSize("record key", record.key);
    if (!status.ok()) {
        return status;
    }

    status = ValidateStringSize("record value", record.value);
    if (!status.ok()) {
        return status;
    }

    const std::uint32_t crc = ComputeRecordCrc(record);

    AppendLittleEndian<std::uint64_t>(record.timestamp, output);
    AppendLittleEndian<std::uint32_t>(static_cast<std::uint32_t>(record.key.size()), output);
    AppendLittleEndian<std::uint32_t>(static_cast<std::uint32_t>(record.value.size()), output);
    AppendLittleEndian<std::uint32_t>(crc, output);
    AppendStringBytes(record.key, output);
    AppendStringBytes(record.value, output);

    return Status::Ok();
}

Result<DecodedRecord> DecodeRecord(std::span<const std::byte> data) {
    if (data.size() < kRecordHeaderBytes) {
        return Status::Corruption("record header is truncated");
    }

    auto timestamp = ReadLittleEndian<std::uint64_t>(data, 0);
    if (!timestamp.ok()) {
        return timestamp.status();
    }
    auto key_size = ReadLittleEndian<std::uint32_t>(data, sizeof(std::uint64_t));
    if (!key_size.ok()) {
        return key_size.status();
    }
    auto value_size = ReadLittleEndian<std::uint32_t>(data, sizeof(std::uint64_t) + sizeof(std::uint32_t));
    if (!value_size.ok()) {
        return value_size.status();
    }
    auto crc = ReadLittleEndian<std::uint32_t>( data, 
                                                sizeof(std::uint64_t) + 
                                                sizeof(std::uint32_t) + 
                                                sizeof(std::uint32_t));
    if (!crc.ok()) {
        return crc.status();
    }

    const std::size_t payload_size = static_cast<std::size_t>(key_size.value()) + static_cast<std::size_t>(value_size.value());
    if (payload_size > data.size() - kRecordHeaderBytes) {
        return Status::Corruption("record payload is truncated");
    }

    const std::byte* key_begin = data.data() + kRecordHeaderBytes;
    const std::byte* value_begin = key_begin + key_size.value();

    Record record;
    record.timestamp = timestamp.value();
    record.key.assign(reinterpret_cast<const char*>(key_begin), key_size.value());
    record.value.assign(reinterpret_cast<const char*>(value_begin), value_size.value());
    record.crc32 = crc.value();

    if (ComputeRecordCrc(record) != record.crc32) {
        return Status::Corruption("record crc mismatch");
    }

    return DecodedRecord{std::move(record), kRecordHeaderBytes + payload_size};
}

}  // namespace logmq
