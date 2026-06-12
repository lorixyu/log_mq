#include "logmq/net/event_loop_thread_pool.h"

#include <algorithm>

namespace logmq {

EventLoopThreadPool::EventLoopThreadPool(std::size_t thread_count)
    : thread_count_(std::max<std::size_t>(1, thread_count)) {}

EventLoopThreadPool::~EventLoopThreadPool() { Stop(); }

Status EventLoopThreadPool::Start() {
    // Started;
    if (!loops_.empty()) {
        return Status::Ok();
    }

    loops_.reserve(thread_count_);
    threads_.reserve(thread_count_);
    for (std::size_t i = 0; i < thread_count_; ++i) {
        auto loop = std::make_unique<EventLoop>();
        Status status = loop->Init();
        if (!status.ok()) {
            return status;
        }
        EventLoop* raw_loop = loop.get();
        loops_.push_back(std::move(loop));
        threads_.emplace_back([raw_loop] { raw_loop->Loop(); });
    }
    return Status::Ok();
}

EventLoop* EventLoopThreadPool::NextLoop() {
    if (loops_.empty()) {
        return nullptr;
    }
    EventLoop* loop = loops_[next_].get();
    next_ = (next_ + 1) % loops_.size();
    return loop;
}

void EventLoopThreadPool::Stop() {
    for (auto& loop : loops_) {
        loop->Quit();
    }
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads_.clear();
    loops_.clear();
}

}  // namespace logmq
