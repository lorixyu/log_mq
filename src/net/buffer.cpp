#include "logmq/net/buffer.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <unistd.h>

namespace logmq {

std::size_t Buffer::readable_bytes() const {
    return data_.size() - reader_index_;
}

std::span<const std::byte> Buffer::ReadableSpan() const {
    return {data_.data() + reader_index_, readable_bytes()};
}

void Buffer::Append(std::span<const std::byte> data) {
    data_.insert(data_.end(), data.begin(), data.end());
}

void Buffer::Append(const std::vector<std::byte>& data) {
    Append(std::span<const std::byte>(data.data(), data.size()));
}

void Buffer::Consume(std::size_t bytes) {
    reader_index_ += std::min(bytes, readable_bytes());
    CompactIfNeeded();
}

void Buffer::Clear() {
    data_.clear();
    reader_index_ = 0;
}

Result<std::vector<std::byte>> Buffer::ReadFromFd(int fd, std::size_t max_bytes) {
    std::vector<std::byte> chunk(max_bytes);
    const ssize_t n = ::read(fd, chunk.data(), chunk.size());
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return std::vector<std::byte>{};
        }
        return Status::IoError("read: " + std::string(std::strerror(errno)));
    }
    if (n == 0) {
        return Status::NotFound("peer closed connection");
    }

    chunk.resize(static_cast<std::size_t>(n));
    Append(chunk);
    return chunk;
}

Status Buffer::WriteToFd(int fd) {
    while (readable_bytes() > 0) {
        const auto readable = ReadableSpan();
        const ssize_t n = ::write(fd, readable.data(), readable.size());
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return Status::Ok();
            }
            return Status::IoError("write: " + std::string(std::strerror(errno)));
        }
        if (n == 0) {
            return Status::Ok();
        }
        Consume(static_cast<std::size_t>(n));
    }
    return Status::Ok();
}

void Buffer::CompactIfNeeded() {
    if (reader_index_ == 0) {
        return;
    }
    if (reader_index_ == data_.size()) {
        Clear();
        return;
    }
    if (reader_index_ > data_.size() / 2) {
        data_.erase(data_.begin(), data_.begin() + static_cast<std::ptrdiff_t>(reader_index_));
        reader_index_ = 0;
    }
}

}  // namespace logmq
