#include "logmq/storage/record_batch.h"

#include <limits>
#include <type_traits>

#include "logmq/storage/crc32.h"

namespace logmq {
namespace {

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
        return Status::Corruption("record batch header is truncated");
    }

    UInt value = 0;
    for (std::size_t i = 0; i < sizeof(UInt); ++i) {
        value |= static_cast<UInt>(std::to_integer<std::uint8_t>(data[offset + i])) << (i * 8U);
    }
    return value;
}

// Encode records into a raw bytes payload.
Result<std::vector<std::byte>> EncodeRecordsPayload(const std::vector<Record>& records) {
    std::vector<std::byte> payload;

    std::size_t payload_bytes = 0;
    for (const Record& record : records) {
        const std::size_t record_bytes = EncodedRecordSize(record);
        if (record_bytes > std::numeric_limits<std::size_t>::max() - payload_bytes) {
            return Status::InvalidArgument("record batch is too large");
        }
        payload_bytes += record_bytes;
    }
    payload.reserve(payload_bytes);

    for (const Record& record : records) {
        Status status = AppendEncodedRecord(record, payload);
        if (!status.ok()) {
            return status;
        }
    }

    return payload;
}

}  // namespace

Result<RecordBatch> MakeRecordBatch(Offset base_offset, std::vector<Record> records) {
    if (base_offset < 0) {
        return Status::InvalidArgument("record batch base_offset must not be negative");
    }
    if (records.empty()) {
        return Status::InvalidArgument("record batch must contain at least one record");
    }
    if (records.size() > std::numeric_limits<std::uint64_t>::max()) {
        return Status::InvalidArgument("record batch contains too many records");
    }

    for (Record& record : records) {
        record.crc32 = ComputeRecordCrc(record);
    }

    auto payload = EncodeRecordsPayload(records);
    if (!payload.ok()) {
        return payload.status();
    }

    RecordBatch batch;
    batch.base_offset = base_offset;
    batch.record_count = static_cast<std::uint64_t>(records.size());
    batch.batch_bytes = kRecordBatchHeaderBytes + payload.value().size();
    batch.batch_crc32 = Crc32(payload.value());
    batch.records = std::move(records);
    return batch;
}

Result<std::vector<std::byte>> EncodeRecordBatch(const RecordBatch& batch) {
    if (batch.base_offset < 0) {
        return Status::InvalidArgument("record batch base_offset must not be negative");
    }
    if (batch.records.empty()) {
        return Status::InvalidArgument("record batch must contain at least one record");
    }
    if (batch.records.size() > std::numeric_limits<std::uint64_t>::max()) {
        return Status::InvalidArgument("record batch contains too many records");
    }

    auto payload = EncodeRecordsPayload(batch.records);
    if (!payload.ok()) {
        return payload.status();
    }

    const std::uint64_t batch_bytes = kRecordBatchHeaderBytes + payload.value().size();
    const std::uint32_t batch_crc = Crc32(payload.value());

    std::vector<std::byte> output;
    output.reserve(batch_bytes);
    AppendLittleEndian<std::uint32_t>(kRecordBatchMagic, output);
    AppendLittleEndian<std::uint64_t>(static_cast<std::uint64_t>(batch.base_offset), output);
    AppendLittleEndian<std::uint64_t>(static_cast<std::uint64_t>(batch.records.size()), output);
    AppendLittleEndian<std::uint64_t>(batch_bytes, output);
    AppendLittleEndian<std::uint32_t>(batch_crc, output);
    output.insert(output.end(), payload.value().begin(), payload.value().end());

    return output;
}

// Read the total size of the batch.
Result<std::uint64_t> PeekRecordBatchBytes(std::span<const std::byte> data) {
    if (data.size() < kRecordBatchHeaderBytes) {
        return Status::Corruption("record batch header is truncated");
    }

    auto magic = ReadLittleEndian<std::uint32_t>(data, 0);
    if (!magic.ok()) {
        return magic.status();
    }
    if (magic.value() != kRecordBatchMagic) {
        return Status::Corruption("record batch magic mismatch");
    }

    auto batch_bytes = ReadLittleEndian<std::uint64_t>(
        data, sizeof(std::uint32_t) + sizeof(std::uint64_t) + sizeof(std::uint64_t));
    if (!batch_bytes.ok()) {
        return batch_bytes.status();
    }
    if (batch_bytes.value() < kRecordBatchHeaderBytes) {
        return Status::Corruption("record batch size is smaller than header");
    }

    return batch_bytes.value();
}

Result<RecordBatch> DecodeRecordBatch(std::span<const std::byte> data) {
    auto batch_bytes = PeekRecordBatchBytes(data);
    if (!batch_bytes.ok()) {
        return batch_bytes.status();
    }
    if (batch_bytes.value() > data.size()) {
        return Status::Corruption("record batch payload is truncated");
    }

    auto base_offset = ReadLittleEndian<std::uint64_t>(data, sizeof(std::uint32_t));
    if (!base_offset.ok()) {
        return base_offset.status();
    }
    auto record_count =
        ReadLittleEndian<std::uint64_t>(data, sizeof(std::uint32_t) + sizeof(std::uint64_t));
    if (!record_count.ok()) {
        return record_count.status();
    }
    auto batch_crc =
        ReadLittleEndian<std::uint32_t>(data, sizeof(std::uint32_t) + sizeof(std::uint64_t) +
                                                  sizeof(std::uint64_t) + sizeof(std::uint64_t));
    if (!batch_crc.ok()) {
        return batch_crc.status();
    }

    const std::span<const std::byte> payload(data.data() + kRecordBatchHeaderBytes,
                                             batch_bytes.value() - kRecordBatchHeaderBytes);
    if (Crc32(payload) != batch_crc.value()) {
        return Status::Corruption("record batch crc mismatch");
    }
    if (record_count.value() == 0) {
        return Status::Corruption("record batch record_count is zero");
    }
    if (record_count.value() > payload.size() / kRecordHeaderBytes) {
        return Status::Corruption("record batch record_count exceeds payload capacity");
    }

    std::vector<Record> records;
    records.reserve(static_cast<std::size_t>(record_count.value()));

    std::size_t consumed = 0;
    for (std::uint64_t i = 0; i < record_count.value(); ++i) {
        auto decoded = DecodeRecord(payload.subspan(consumed));
        if (!decoded.ok()) {
            return decoded.status();
        }
        consumed += decoded.value().bytes_consumed;
        records.push_back(std::move(decoded.value().record));
    }

    if (consumed != payload.size()) {
        return Status::Corruption("record batch has trailing bytes");
    }

    RecordBatch batch;
    batch.base_offset = static_cast<Offset>(base_offset.value());
    batch.record_count = record_count.value();
    batch.batch_bytes = batch_bytes.value();
    batch.batch_crc32 = batch_crc.value();
    batch.records = std::move(records);
    return batch;
}

}  // namespace logmq
