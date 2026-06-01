#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include "logmq/storage/file.h"
#include "logmq/storage/record_batch.h"
#include "logmq/storage/segment.h"

namespace logmq {
namespace {

class TempDir {
public:
    TempDir() {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() / ("logmq_week1_" + std::to_string(now));
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    [[nodiscard]] std::filesystem::path FilePath(std::string name) const {
        return path_ / std::move(name);
    }

private:
    std::filesystem::path path_;
};

Record MakeRecord(std::uint64_t index) {
    Record record;
    record.key = "key-" + std::to_string(index);
    record.value = "value-" + std::to_string(index);
    record.timestamp = 1'700'000'000 + index;
    return record;
}

std::span<const std::byte> AsBytes(const std::vector<std::byte>& bytes) {
    return {bytes.data(), bytes.size()};
}

TEST(RecordBatchTest, EncodesAndDecodesRecords) {
    std::vector<Record> records;
    for (std::uint64_t i = 0; i < 8; ++i) {
        records.push_back(MakeRecord(i));
    }

    auto batch = MakeRecordBatch(12, std::move(records));
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();

    auto encoded = EncodeRecordBatch(batch.value());
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto decoded = DecodeRecordBatch(AsBytes(encoded.value()));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();

    EXPECT_EQ(decoded.value().base_offset, 12);
    EXPECT_EQ(decoded.value().record_count, 8);
    ASSERT_EQ(decoded.value().records.size(), 8);

    for (std::size_t i = 0; i < decoded.value().records.size(); ++i) {
        EXPECT_EQ(decoded.value().records[i].key, "key-" + std::to_string(i));
        EXPECT_EQ(decoded.value().records[i].value, "value-" + std::to_string(i));
        EXPECT_EQ(decoded.value().records[i].crc32, ComputeRecordCrc(decoded.value().records[i]));
    }
}

TEST(RecordBatchTest, RejectsCorruptedPayload) {
    auto batch = MakeRecordBatch(0, std::vector<Record>{MakeRecord(1)});
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();

    auto encoded = EncodeRecordBatch(batch.value());
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    encoded.value().back() ^= std::byte{0x01};

    auto decoded = DecodeRecordBatch(AsBytes(encoded.value()));
    ASSERT_FALSE(decoded.ok());
    EXPECT_EQ(decoded.status().code(), ErrorCode::kCorruption);
}

TEST(FileTest, WriteAllRetriesPartialWrites) {
    std::vector<std::byte> bytes(17, std::byte{0x7F});
    std::vector<std::uint64_t> offsets;
    std::size_t copied = 0;

    Status status =
        WriteAll(AsBytes(bytes), 42,
                 [&](const std::byte*, std::size_t size, std::uint64_t offset) -> ssize_t {
                     const std::size_t n = std::min<std::size_t>(3, size);
                     offsets.push_back(offset);
                     copied += n;
                     return static_cast<ssize_t>(n);
                 });

    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(copied, bytes.size());
    EXPECT_GT(offsets.size(), 1U);
    EXPECT_EQ(offsets.front(), 42U);
    EXPECT_EQ(offsets.back(), 57U);
}

TEST(SegmentTest, AppendsAndReadsTenThousandBatches) {
    TempDir temp_dir;
    auto segment = Segment::Open(temp_dir.FilePath("00000000000000000000.log"), 0);
    ASSERT_TRUE(segment.ok()) << segment.status().ToString();

    std::vector<std::uint64_t> positions;
    positions.reserve(10000);

    for (Offset offset = 0; offset < 10000; ++offset) {
        auto batch = MakeRecordBatch(offset, std::vector<Record>{MakeRecord(offset)});
        ASSERT_TRUE(batch.ok()) << batch.status().ToString();

        auto position = segment.value().Append(batch.value());
        ASSERT_TRUE(position.ok()) << position.status().ToString();
        if (!positions.empty()) {
            EXPECT_GT(position.value(), positions.back());
        }
        positions.push_back(position.value());
    }

    ASSERT_TRUE(segment.value().Flush().ok());

    for (Offset offset = 0; offset < 10000; ++offset) {
        auto batch = segment.value().Read(positions[offset], 4096);
        ASSERT_TRUE(batch.ok()) << batch.status().ToString();
        EXPECT_EQ(batch.value().base_offset, offset);
        ASSERT_EQ(batch.value().records.size(), 1U);
        EXPECT_EQ(batch.value().records[0].key, "key-" + std::to_string(offset));
    }
}

TEST(SegmentTest, HandlesLargeRecord) {
    TempDir temp_dir;
    auto segment = Segment::Open(temp_dir.FilePath("00000000000000000000.log"), 0);
    ASSERT_TRUE(segment.ok()) << segment.status().ToString();

    Record record;
    record.key = "large";
    record.value.assign(1024 * 1024, 'x');
    record.timestamp = 1'700'000'001;

    auto batch = MakeRecordBatch(0, std::vector<Record>{std::move(record)});
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();

    auto position = segment.value().Append(batch.value());
    ASSERT_TRUE(position.ok()) << position.status().ToString();

    auto decoded = segment.value().Read(position.value(), 2 * 1024 * 1024);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_EQ(decoded.value().records.size(), 1U);
    EXPECT_EQ(decoded.value().records[0].key, "large");
    EXPECT_EQ(decoded.value().records[0].value.size(), 1024U * 1024U);
}

}  // namespace
}  // namespace logmq
