#pragma once

#if defined(__linux__)
#include <sys/epoll.h>
#include <unordered_set>
#include <vector>
#endif

#include "logmq/net/poll/poller.h"

namespace logmq {

class EpollPoller : public Poller {
public:
    EpollPoller();
    ~EpollPoller() override;

    [[nodiscard]] Status Init() override;

    [[nodiscard]] Status Poll(std::vector<PollEvent>* active_events) override;

    [[nodiscard]] Status UpdateChannel(Channel* channel) override;

    [[nodiscard]] Status RemoveChannel(Channel* channel) override;

private:
#if defined(__linux__)
    static constexpr int kInitialEventCapacity = 64;

    int epoll_fd_{-1};
    std::vector<epoll_event> events_;
    std::unordered_set<int> registered_fds_;
#endif
};

}  // namespace logmq
