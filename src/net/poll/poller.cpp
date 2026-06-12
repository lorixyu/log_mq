#include "logmq/net/poll/poller.h"

#include "logmq/net/poll/epoll_poller.h"
#include "logmq/net/poll/kqueue_poller.h"

namespace logmq {

std::unique_ptr<Poller> Poller::CreatePlatformPoller() {
#if defined(__linux__)
    return std::make_unique<EpollPoller>();
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return std::make_unique<KqueuePoller>();
#else
#error "Unsupported platform"
#endif
}

}  // namespace logmq
