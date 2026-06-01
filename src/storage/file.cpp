#include "logmq/storage/file.h"

#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

namespace logmq {
namespace {

Status IoErrorFromErrno(std::string_view operation, const std::filesystem::path& path) {
    std::string message(operation);
    if (!path.empty()) {
        message += " ";
        message += path.string();
    }
    message += ": ";
    message += std::strerror(errno);
    return Status::IoError(std::move(message));
}

}  // namespace

File::File(int fd, std::filesystem::path path) : fd_(fd), path_(std::move(path)) {}

File::~File() {
    if (fd_ >= 0) {
        static_cast<void>(Close());
    }
}

File::File(File&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)), path_(std::move(other.path_)) {}

File& File::operator=(File&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            static_cast<void>(Close());
        }
        fd_ = std::exchange(other.fd_, -1);
        path_ = std::move(other.path_);
    }
    return *this;
}

Result<File> File::Open(const std::filesystem::path& path, int flags, mode_t mode) {
    const std::string path_string = path.string();
    const int fd = ::open(path_string.c_str(), flags, mode);
    if (fd < 0) {
        return IoErrorFromErrno("open", path);
    }
    return File(fd, path);
}

bool File::is_open() const { return fd_ >= 0; }

int File::fd() const { return fd_; }

const std::filesystem::path& File::path() const { return path_; }

Result<std::size_t> File::PRead(std::span<std::byte> data, std::uint64_t offset) const {
    if (!is_open()) {
        return Status::InvalidArgument("pread on closed file");
    }

    std::size_t total = 0;
    while (total < data.size()) {
        errno = 0;
        const ssize_t n = ::pread(fd_, data.data() + total, data.size() - total, offset + total);
        if (n < 0) {
            if (errno == EINTR) {  // System call interrupted by a signal(EINTR).
                continue;  // Retry.
            }
            return IoErrorFromErrno("pread", path_);
        }
        if (n == 0) {
            break;
        }
        total += static_cast<std::size_t>(n);
    }

    return total;
}

Status File::PWrite(std::span<const std::byte> data, std::uint64_t offset) const {
    if (!is_open()) {
        return Status::InvalidArgument("pwrite on closed file");
    }

    return WriteAll(
        data, offset,
        [this](const std::byte* bytes, std::size_t size, std::uint64_t write_offset) -> ssize_t {
            return ::pwrite(fd_, bytes, size, write_offset);
        });
}

Status File::Fsync() const {
    if (!is_open()) {
        return Status::InvalidArgument("fsync on closed file");
    }

    while (true) {
        errno = 0;
        if (::fsync(fd_) == 0) {
            return Status::Ok();
        }
        if (errno != EINTR) {
            return IoErrorFromErrno("fsync", path_);
        }
    }
}

Status File::Truncate(std::uint64_t size) const {
    if (!is_open()) {
        return Status::InvalidArgument("truncate on closed file");
    }

    if (size > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        return Status::InvalidArgument("truncate size is too large");
    }

    while (true) {
        errno = 0;
        if (::ftruncate(fd_, static_cast<off_t>(size)) == 0) {
            return Status::Ok();
        }
        if (errno != EINTR) {
            return IoErrorFromErrno("ftruncate", path_);
        }
    }
}

Status File::Close() {
    if (!is_open()) {
        return Status::Ok();
    }

    const int fd = fd_;
    fd_ = -1;
    if (::close(fd) != 0) {
        return IoErrorFromErrno("close", path_);
    }
    return Status::Ok();
}

Result<std::uint64_t> File::Size() const {
    if (!is_open()) {
        return Status::InvalidArgument("fstat on closed file");
    }

    struct stat stat_buffer{};
    if (::fstat(fd_, &stat_buffer) != 0) {
        return IoErrorFromErrno("fstat", path_);
    }
    if (stat_buffer.st_size < 0) {
        return Status::IoError("file size is negative");
    }
    return static_cast<std::uint64_t>(stat_buffer.st_size);
}

}  // namespace logmq
