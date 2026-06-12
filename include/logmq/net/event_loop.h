#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "logmq/base/result.h"

namespace logmq {

class Channel;
class Poller;

class EventLoop {
public:
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    [[nodiscard]] Status Init();

    void Loop();

    void Quit();

    void RunInLoop(Functor functor);

    void QueueInLoop(Functor functor);

    [[nodiscard]] bool IsInLoopThread() const;

    void UpdateChannel(Channel* channel);

    void RemoveChannel(Channel* channel);

private:
    void Wakeup();
    void HandleWakeup();
    void DoPendingFunctors();

    std::unique_ptr<Poller> poller_;
    int wakeup_read_fd_{-1};
    int wakeup_write_fd_{-1};
    std::unique_ptr<Channel> wakeup_channel_;
    std::atomic<bool> quit_{false};
    std::thread::id thread_id_;
    std::mutex pending_mutex_;
    std::vector<Functor> pending_functors_;
};

}  // namespace logmq
