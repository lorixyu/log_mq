#include "logmq/net/poll/epoll_poller.h"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <unistd.h>

#include "logmq/net/channel.h"

namespace logmq {
namespace {

#if !defined(__linux__)
Status Unsupported() {
    return Status::Internal("epoll poller is only supported on Linux");
}
#endif

#if defined(__linux__)
Status EpollError(const char* operation) {
    return Status::IoError(std::string(operation) + ": " + std::strerror(errno));
}

// 根据参数 channel 返回channel的事件
std::uint32_t EventsForChannel(const Channel& channel) {
    std::uint32_t events = 0;
    if (channel.reading()) {
        events |= EPOLLIN | EPOLLPRI | EPOLLRDHUP;
    }
    if (channel.writing()) {
        events |= EPOLLOUT;
    }
    return events;
}

PollEvent ToPollEvent(const epoll_event& event) {
    const std::uint32_t flags = event.events;
    PollEvent poll_event;
    poll_event.channel = static_cast<Channel*>(event.data.ptr);
    poll_event.readable = (flags & (EPOLLIN | EPOLLPRI)) != 0;
    poll_event.writable = (flags & EPOLLOUT) != 0;
    poll_event.closed = (flags & (EPOLLHUP | EPOLLRDHUP)) != 0;
    poll_event.error = (flags & EPOLLERR) != 0;
    return poll_event;
}
#endif

}  // namespace

EpollPoller::EpollPoller() {
#if defined(__linux__)
    events_.resize(kInitialEventCapacity);
#endif
}

EpollPoller::~EpollPoller() {
#if defined(__linux__)
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
#endif
}

Status EpollPoller::Init() {
#if defined(__linux__)
    if (epoll_fd_ >= 0) {
        return Status::Ok();
    }

    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        return EpollError("epoll_create1");
    }
    return Status::Ok();
#else
    return Unsupported();
#endif
}

Status EpollPoller::Poll(std::vector<PollEvent>* active_events) {
    if (active_events == nullptr) {
        return Status::InvalidArgument("active_events must not be null");
    }
    active_events->clear();

#if defined(__linux__)
    if (epoll_fd_ < 0) {
        return Status::Internal("epoll poller is not initialized");
    }

    const int n = ::epoll_wait(epoll_fd_, events_.data(),
                              static_cast<int>(events_.size()), -1);
    if (n < 0) {
        if (errno == EINTR) {
            return Status::Ok();
        }
        return EpollError("epoll_wait");
    }

    active_events->reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        active_events->push_back(ToPollEvent(events_[static_cast<std::size_t>(i)]));
    }

    if (static_cast<std::size_t>(n) == events_.size()) {
        events_.resize(events_.size() * 2);
    }
    return Status::Ok();
#else
    return Unsupported();
#endif
}

Status EpollPoller::UpdateChannel(Channel* channel) {
    if (channel == nullptr) {
        return Status::InvalidArgument("channel must not be null");
    }

#if defined(__linux__)
    if (epoll_fd_ < 0) {
        return Status::Internal("epoll poller is not initialized");
    }
    if (channel->fd() < 0) {
        return Status::InvalidArgument("channel fd must be valid");
    }

    const std::uint32_t events = EventsForChannel(*channel);
    const bool registered = registered_fds_.find(channel->fd()) != registered_fds_.end();
    if (events == 0) {
        if (!registered) {
            return Status::Ok();
        }
        return RemoveChannel(channel);
    }

    epoll_event event{};
    event.events = events;
    event.data.ptr = channel;

    const int operation = registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    if (::epoll_ctl(epoll_fd_, operation, channel->fd(), &event) < 0) {
        return EpollError(registered ? "epoll_ctl MOD" : "epoll_ctl ADD");
    }

    if (!registered) {
        registered_fds_.insert(channel->fd());
    }
    return Status::Ok();
#else
    return Unsupported();
#endif
}

Status EpollPoller::RemoveChannel(Channel* channel) {
    if (channel == nullptr) {
        return Status::InvalidArgument("channel must not be null");
    }

#if defined(__linux__)
    if (epoll_fd_ < 0 || channel->fd() < 0) {
        return Status::Ok();
    }

    const auto it = registered_fds_.find(channel->fd());
    if (it == registered_fds_.end()) {
        return Status::Ok();
    }

    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, channel->fd(), nullptr) < 0 &&
        errno != ENOENT) {
        return EpollError("epoll_ctl DEL");
    }
    registered_fds_.erase(it);
    return Status::Ok();
#else
    return Unsupported();
#endif
}

}  // namespace logmq
