#pragma once

#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

#include "logmq/base/result.h"
#include "logmq/net/event_loop.h"

namespace logmq {

class EventLoopThreadPool {
public:
    explicit EventLoopThreadPool(std::size_t thread_count);
    ~EventLoopThreadPool();

    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    [[nodiscard]] Status Start();

    [[nodiscard]] EventLoop* NextLoop();

    void Stop();

private:
    std::size_t thread_count_{0};
    std::vector<std::unique_ptr<EventLoop>> loops_;
    std::vector<std::thread> threads_;
    std::size_t next_{0};
};

}  // namespace logmq
