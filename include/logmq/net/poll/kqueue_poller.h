#pragma once

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#include <sys/event.h>
#include <vector>
#endif

#include "logmq/net/poll/poller.h"

namespace logmq {

class KqueuePoller : public Poller {
public:
    KqueuePoller();
    ~KqueuePoller() override;

    [[nodiscard]] Status Init() override;

    [[nodiscard]] Status Poll(std::vector<PollEvent>* active_events) override;

    [[nodiscard]] Status UpdateChannel(Channel* channel) override;

    [[nodiscard]] Status RemoveChannel(Channel* channel) override;

private:
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    static constexpr int kInitialEventCapacity = 64;

    int kqueue_fd_{-1};
    std::vector<struct kevent> events_;
#endif
};

}  // namespace logmq
