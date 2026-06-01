#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>

#include "logmq/base/result.h"

namespace logmq {

// 循环调用 writer 直到 data 全部写完；用于处理 pwrite 只写入部分字节的情况。
template <typename Writer>
Status WriteAll(std::span<const std::byte> data, std::uint64_t offset, Writer&& writer) {
    std::size_t written = 0;

    while (written < data.size()) {
        errno = 0;
        const ssize_t n = writer(data.data() + written, data.size() - written, offset + written);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::IoError(std::string("pwrite failed: ") + std::strerror(errno));
        }
        if (n == 0) {
            return Status::IoError("pwrite made no progress");
        }
        if (static_cast<std::size_t>(n) > data.size() - written) {
            return Status::Internal("pwrite returned more bytes than requested");
        }

        written += static_cast<std::size_t>(n);
    }

    return Status::Ok();
}

class File {
public:
    File() = default;
    ~File();

    File(const File&) = delete;
    File& operator=(const File&) = delete;

    // Move constructor & move assignment operator.
    File(File&& other) noexcept;
    File& operator=(File&& other) noexcept;

    // Opens the file pointed to by the path,
    // On sucess, returns a File object containing a file descriptor.
    // On failure, returns an IoError.
    static Result<File> Open(const std::filesystem::path& path, int flags = O_CREAT | O_RDWR,
                             mode_t mode = 0644);

    // Returns true if the File object owns a valid file descriptor.
    [[nodiscard]] bool is_open() const;

    // Returns a underlying file descriptor.
    // Ownership of the fd remains with the file object; the caller must not close it.
    [[nodiscard]] int fd() const;

    // Returns the path of the file.
    [[nodiscard]] const std::filesystem::path& path() const;

    // Reads data from the file starting at the specified offset.
    [[nodiscard]] Result<std::size_t> PRead(std::span<std::byte> data, std::uint64_t offset) const;

    // Writes data into the file starting at the specified offset.
    Status PWrite(std::span<const std::byte> data, std::uint64_t offset) const;

    Status Fsync() const;

    Status Close();

    // Returns the size of the file.
    [[nodiscard]] Result<std::uint64_t> Size() const;

private:
    // 使用已经打开的 fd 构造 File，并接管 fd 所有权。
    File(int fd, std::filesystem::path path);

    int fd_ = -1;
    std::filesystem::path path_;
};

}  // namespace logmq
