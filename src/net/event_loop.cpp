#include "logmq/net/event_loop.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <utility>

#if defined(__linux__)
#include <sys/eventfd.h>
#endif

#include "logmq/net/channel.h"
#include "logmq/net/poll/poller.h"

namespace logmq {
namespace {

Status SetNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return Status::IoError("fcntl(F_GETFL): " + std::string(std::strerror(errno)));
    }
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return Status::IoError("fcntl(F_SETFL): " + std::string(std::strerror(errno)));
    }
    return Status::Ok();
}

void CloseFd(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

void CloseWakeupFds(int& read_fd, int& write_fd) {
    if (read_fd == write_fd) {
        CloseFd(read_fd);
        write_fd = -1;
        return;
    }
    CloseFd(read_fd);
    CloseFd(write_fd);
}

Status CreateWakeupFds(int& read_fd, int& write_fd) {
#if defined(__linux__)
    const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) {
        return Status::IoError("eventfd: " + std::string(std::strerror(errno)));
    }
    read_fd = fd;
    write_fd = fd;
    return Status::Ok();
#else
    int fds[2]{-1, -1};
    if (::pipe(fds) < 0) {
        return Status::IoError("pipe: " + std::string(std::strerror(errno)));
    }

    Status status = SetNonBlocking(fds[0]);
    if (!status.ok()) {
        CloseFd(fds[0]);
        CloseFd(fds[1]);
        return status;
    }
    status = SetNonBlocking(fds[1]);
    if (!status.ok()) {
        CloseFd(fds[0]);
        CloseFd(fds[1]);
        return status;
    }

    read_fd = fds[0];
    write_fd = fds[1];
    return Status::Ok();
#endif
}

}  // namespace

EventLoop::EventLoop() : thread_id_(std::this_thread::get_id()) {}

EventLoop::~EventLoop() {
    Quit();
    if (wakeup_channel_) {
        wakeup_channel_->DisableAll();
        wakeup_channel_->Remove();
        wakeup_channel_.reset();
    }
    CloseWakeupFds(wakeup_read_fd_, wakeup_write_fd_);
}

Status EventLoop::Init() {
    if (poller_ != nullptr) {
        return Status::Ok();
    }

    poller_ = Poller::CreatePlatformPoller();
    Status status = poller_->Init();
    if (!status.ok()) {
        poller_.reset();
        return status;
    }

    status = CreateWakeupFds(wakeup_read_fd_, wakeup_write_fd_);
    if (!status.ok()) {
        poller_.reset();
        return status;
    }

    wakeup_channel_ = std::make_unique<Channel>(this, wakeup_read_fd_);
    wakeup_channel_->SetReadCallback([this] { HandleWakeup(); });
    wakeup_channel_->EnableReading();
    return Status::Ok();
}

void EventLoop::Loop() {
    thread_id_ = std::this_thread::get_id();
    std::vector<PollEvent> active_events;

    while (!quit_.load()) {
        if (poller_ == nullptr) {
            break;
        }

        active_events.clear();
        // 捕获监听到的事件列表
        Status status = poller_->Poll(&active_events);
        if (!status.ok()) {
            continue;
        }
        // 分发
        for (const auto& event : active_events) {
            if (event.channel == nullptr) {
                continue;
            }
            event.channel->HandleEvent(event.readable, event.writable, event.closed, event.error);
        }
        DoPendingFunctors();
    }
    DoPendingFunctors();
}

void EventLoop::Quit() {
    quit_.store(true);
    Wakeup();
}

void EventLoop::RunInLoop(Functor functor) {
    if (IsInLoopThread()) {
        functor();
    } else {
        QueueInLoop(std::move(functor));
    }
}

void EventLoop::QueueInLoop(Functor functor) {
    {
        std::lock_guard lock(pending_mutex_);
        pending_functors_.push_back(std::move(functor));
    }
    // 任务去排队后叫醒eventloop
    Wakeup();
}

bool EventLoop::IsInLoopThread() const {
    return std::this_thread::get_id() == thread_id_;
}

void EventLoop::UpdateChannel(Channel* channel) {
    if (poller_ != nullptr && channel != nullptr) {
        (void)poller_->UpdateChannel(channel);
    }
}

void EventLoop::RemoveChannel(Channel* channel) {
    if (poller_ != nullptr && channel != nullptr) {
        (void)poller_->RemoveChannel(channel);
    }
}

void EventLoop::Wakeup() {
    if (wakeup_write_fd_ < 0) {
        return;
    }

#if defined(__linux__)
    const std::uint64_t value{1};
    const ssize_t n = ::write(wakeup_write_fd_, &value, sizeof(value));
#else
    const std::byte byte{0x01};
    const ssize_t n = ::write(wakeup_write_fd_, &byte, sizeof(byte));
#endif
    (void)n;
}

void EventLoop::HandleWakeup() {
#if defined(__linux__)
    std::uint64_t value{0};
    while (true) {
        const ssize_t n = ::read(wakeup_read_fd_, &value, sizeof(value));
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            return;
        }
        if (n == 0) {
            return;
        }
    }
#else
    std::byte buffer[64];
    while (true) {
        const ssize_t n = ::read(wakeup_read_fd_, buffer, sizeof(buffer));
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            return;
        }
        if (n == 0 || static_cast<std::size_t>(n) < sizeof(buffer)) {
            return;
        }
    }
#endif
}

void EventLoop::DoPendingFunctors() {
    std::vector<Functor> functors;
    {
        std::lock_guard lock(pending_mutex_);
        // Swap out the queue so callbacks can enqueue more callbacks without
        // deadlocking or extending this locked section.
        functors.swap(pending_functors_);
    }
    for (auto& functor : functors) {
        functor();
    }
}

}  // namespace logmq
