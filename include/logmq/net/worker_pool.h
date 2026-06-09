#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace logmq {

class WorkerPool {
public:
    using Task = std::function<void()>;

    WorkerPool(std::size_t thread_count, std::size_t max_queue_size);
    ~WorkerPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    [[nodiscard]] bool Submit(Task task);

    void Stop();

private:
    void Run();

    std::mutex mutex_;
    std::condition_variable condition_;
    std::queue<Task> tasks_;
    std::vector<std::thread> threads_;
    // Bounded queue is the first backpressure point before request handling.
    std::size_t max_queue_size_{0};
    bool stopping_{false};
};

}  // namespace logmq
