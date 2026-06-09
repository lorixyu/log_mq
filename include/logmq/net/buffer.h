#pragma once

#include <cstddef>
#include <span>
#include <vector>

#include "logmq/base/result.h"

namespace logmq {

class Buffer {
public:
    // Returns readable bytes;
    [[nodiscard]] std::size_t readable_bytes() const;

    // Returns readable bytes span;
    [[nodiscard]] std::span<const std::byte> ReadableSpan() const;

    // Push back raw data to data_;
    void Append(std::span<const std::byte> data);

    void Append(const std::vector<std::byte>& data);

    // Consuming bytes
    void Consume(std::size_t bytes);

    void Clear();

    [[nodiscard]] Result<std::vector<std::byte>> ReadFromFd(int fd, std::size_t max_bytes);

    [[nodiscard]] Status WriteToFd(int fd);

private:
    // Only retain readable part;
    void CompactIfNeeded();

    std::vector<std::byte> data_;
    std::size_t reader_index_{0};  // Readable index    readable [data_.begin() + reader_index_, data_.end()];
};

}  // namespace logmq
