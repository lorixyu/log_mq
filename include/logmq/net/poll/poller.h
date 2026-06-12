#pragma once

#include <memory>
#include <vector>

#include "logmq/base/status.h"

namespace logmq {

class Channel;

struct PollEvent {
    Channel* channel{nullptr};
    bool readable{false};
    bool writable{false};
    bool closed{false};
    bool error{false};
};

class Poller {
public:
    Poller() = default;
    virtual ~Poller() = default;

    virtual Status Init() = 0;
    virtual Status Poll(std::vector<PollEvent>* active_events) = 0;
    virtual Status UpdateChannel(Channel* channel) = 0;
    virtual Status RemoveChannel(Channel* channel) = 0;

    [[nodiscard]] static std::unique_ptr<Poller> CreatePlatformPoller();
};

}  // namespace logmq
