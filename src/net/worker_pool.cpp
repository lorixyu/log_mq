#include "logmq/net/worker_pool.h"

#include <algorithm>
#include <utility>

namespace logmq {

WorkerPool::WorkerPool(std::size_t thread_count, std::size_t max_queue_size)
    : max_queue_size_(std::max<std::size_t>(1, max_queue_size)) {
    const std::size_t count = std::max<std::size_t>(1, thread_count);
    threads_.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        threads_.emplace_back([this] { Run(); });
    }
}

WorkerPool::~WorkerPool() { Stop(); }

bool WorkerPool::Submit(Task task) {
    {
        std::lock_guard lock(mutex_);
        if (stopping_ || tasks_.size() >= max_queue_size_) {
            return false;
        }
        tasks_.push(std::move(task));
    }
    condition_.notify_one();
    return true;
}

void WorkerPool::Stop() {
    {
        std::lock_guard lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
    }
    condition_.notify_all();
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void WorkerPool::Run() {
    while (true) {
        Task task;
        {
            std::unique_lock lock(mutex_);
            condition_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
            // If this pool is stopped and task queue is empty, return;
            if (stopping_ && tasks_.empty()) {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
    }
}

}  // namespace logmq
