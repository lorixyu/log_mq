#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <vector>

#include "logmq/net/buffer.h"
#include "logmq/net/worker_pool.h"

namespace logmq {
namespace {

TEST(BufferTest, AppendsConsumesAndCompactsReadableBytes) {
    Buffer buffer;
    std::vector<std::byte> first{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    std::vector<std::byte> second{std::byte{'d'}, std::byte{'e'}};

    buffer.Append(first);
    EXPECT_EQ(buffer.readable_bytes(), 3U);
    buffer.Consume(2);
    EXPECT_EQ(buffer.readable_bytes(), 1U);
    buffer.Append(second);

    const auto readable = buffer.ReadableSpan();
    ASSERT_EQ(readable.size(), 3U);
    EXPECT_EQ(readable[0], std::byte{'c'});
    EXPECT_EQ(readable[1], std::byte{'d'});
    EXPECT_EQ(readable[2], std::byte{'e'});
}

TEST(WorkerPoolTest, ExecutesSubmittedTasks) {
    WorkerPool pool(2, 8);
    std::mutex mutex;
    std::condition_variable condition;
    int count = 0;

    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(pool.Submit([&] {
            {
                std::lock_guard lock(mutex);
                ++count;
            }
            condition.notify_one();
        }));
    }

    std::unique_lock lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return count == 4; }));
    pool.Stop();
}

}  // namespace
}  // namespace logmq
