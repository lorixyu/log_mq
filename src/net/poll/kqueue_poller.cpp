#include "logmq/net/poll/kqueue_poller.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <string>
#include <unistd.h>

#include "logmq/net/channel.h"

namespace logmq {
namespace {

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
Status KqueueError(const char* operation) {
    return Status::IoError(std::string(operation) + ": " + std::strerror(errno));
}

PollEvent ToPollEvent(const struct kevent& event) {
    PollEvent poll_event;
    poll_event.channel = static_cast<Channel*>(event.udata);
    poll_event.readable = event.filter == EVFILT_READ;
    poll_event.writable = event.filter == EVFILT_WRITE;
    poll_event.closed = (event.flags & EV_EOF) != 0;
    poll_event.error = (event.flags & EV_ERROR) != 0;
    return poll_event;
}

Status ApplyEvent(int kqueue_fd, const struct kevent& event, bool ignore_missing) {
    if (::kevent(kqueue_fd, &event, 1, nullptr, 0, nullptr) < 0 &&
        !(ignore_missing && errno == ENOENT)) {
        return KqueueError("kevent change");
    }
    return Status::Ok();
}
#else
Status Unsupported() {
    return Status::Internal("kqueue poller is only supported on BSD-style platforms");
}
#endif

}  // namespace

KqueuePoller::KqueuePoller() {
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    events_.resize(kInitialEventCapacity);
#endif
}

KqueuePoller::~KqueuePoller() {
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    if (kqueue_fd_ >= 0) {
        ::close(kqueue_fd_);
        kqueue_fd_ = -1;
    }
#endif
}

Status KqueuePoller::Init() {
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    if (kqueue_fd_ >= 0) {
        return Status::Ok();
    }

    kqueue_fd_ = ::kqueue();
    if (kqueue_fd_ < 0) {
        return KqueueError("kqueue");
    }
    return Status::Ok();
#else
    return Unsupported();
#endif
}

Status KqueuePoller::Poll(std::vector<PollEvent>* active_events) {
    if (active_events == nullptr) {
        return Status::InvalidArgument("active_events must not be null");
    }
    active_events->clear();

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    if (kqueue_fd_ < 0) {
        return Status::Internal("kqueue poller is not initialized");
    }

    const int n = ::kevent(kqueue_fd_, nullptr, 0, events_.data(),
                           static_cast<int>(events_.size()), nullptr);
    if (n < 0) {
        if (errno == EINTR) {
            return Status::Ok();
        }
        return KqueueError("kevent wait");
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

Status KqueuePoller::UpdateChannel(Channel* channel) {
    if (channel == nullptr) {
        return Status::InvalidArgument("channel must not be null");
    }

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    if (kqueue_fd_ < 0) {
        return Status::Internal("kqueue poller is not initialized");
    }
    if (channel->fd() < 0) {
        return Status::InvalidArgument("channel fd must be valid");
    }

    struct kevent read_event;
    EV_SET(&read_event, channel->fd(), EVFILT_READ,
           channel->reading() ? EV_ADD | EV_ENABLE : EV_DELETE, 0, 0, channel);
    Status status = ApplyEvent(kqueue_fd_, read_event, !channel->reading());
    if (!status.ok()) {
        return status;
    }

    struct kevent write_event;
    EV_SET(&write_event, channel->fd(), EVFILT_WRITE,
           channel->writing() ? EV_ADD | EV_ENABLE : EV_DELETE, 0, 0, channel);
    return ApplyEvent(kqueue_fd_, write_event, !channel->writing());
#else
    return Unsupported();
#endif
}

Status KqueuePoller::RemoveChannel(Channel* channel) {
    if (channel == nullptr) {
        return Status::InvalidArgument("channel must not be null");
    }

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    if (kqueue_fd_ < 0 || channel->fd() < 0) {
        return Status::Ok();
    }

    struct kevent read_event;
    EV_SET(&read_event, channel->fd(), EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    Status status = ApplyEvent(kqueue_fd_, read_event, true);
    if (!status.ok()) {
        return status;
    }

    struct kevent write_event;
    EV_SET(&write_event, channel->fd(), EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    return ApplyEvent(kqueue_fd_, write_event, true);
#else
    return Unsupported();
#endif
}

}  // namespace logmq
