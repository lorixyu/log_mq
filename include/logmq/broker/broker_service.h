#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "logmq/base/config.h"
#include "logmq/broker/group_coordinator.h"
#include "logmq/broker/offset_store.h"
#include "logmq/broker/retry_scheduler.h"
#include "logmq/broker/topic_manager.h"
#include "logmq/protocol/types.h"

namespace logmq {

struct BrokerServiceOptions {
    StorageConfig storage;
    ConsumerConfig consumer;
    RetryOptions retry;
};

class BrokerService {
public:
    explicit BrokerService(BrokerServiceOptions options);
    ~BrokerService();

    [[nodiscard]] Status Start();

    [[nodiscard]] ResponseEnvelope Handle(const RequestEnvelope& request);

    [[nodiscard]] Status Close();

private:
    [[nodiscard]] ResponseEnvelope HandleNack(const RequestEnvelope& envelope,
                                             const NackRequest& request);
    [[nodiscard]] ResponseEnvelope HandleProduce(const RequestEnvelope& envelope,
                                                 const ProduceRequest& request);
    [[nodiscard]] ResponseEnvelope HandleFetch(const RequestEnvelope& envelope,
                                               const FetchRequest& request);
    [[nodiscard]] ResponseEnvelope HandleMetadata(const RequestEnvelope& envelope,
                                                  const MetadataRequest& request);
    [[nodiscard]] ResponseEnvelope HandleCreateTopic(const RequestEnvelope& envelope,
                                                     const CreateTopicRequest& request);
    [[nodiscard]] ResponseEnvelope HandleCommitOffset(const RequestEnvelope& envelope,
                                                      const CommitOffsetRequest& request);
    [[nodiscard]] ResponseEnvelope HandleFetchCommittedOffset(
        const RequestEnvelope& envelope,
        const FetchCommittedOffsetRequest& request);
    [[nodiscard]] ResponseEnvelope HandleJoinGroup(const RequestEnvelope& envelope,
                                                   const JoinGroupRequest& request);
    [[nodiscard]] ResponseEnvelope HandleSyncGroup(const RequestEnvelope& envelope,
                                                   const SyncGroupRequest& request);
    [[nodiscard]] ResponseEnvelope HandleHeartbeat(const RequestEnvelope& envelope,
                                                   const HeartbeatRequest& request);
    [[nodiscard]] ResponseEnvelope HandleLeaveGroup(const RequestEnvelope& envelope,
                                                    const LeaveGroupRequest& request);

    [[nodiscard]] ResponseEnvelope MakeError(const RequestEnvelope& envelope,
                                             ProtocolErrorCode code,
                                             std::string message) const;
    [[nodiscard]] ResponseEnvelope MakeSuccess(const RequestEnvelope& envelope,
                                               ResponseBody body) const;
    void RunAsyncFlusher();

    BrokerServiceOptions options_;
    TopicManager topics_;
    RetryScheduler retries_;
    // Serialize append + scheduler publication so no retry offset is lost.
    // ponytail: global Nack lock; use per-partition locks if failures dominate traffic.
    std::mutex nack_mutex_;
    OffsetStore offsets_;
    GroupCoordinator groups_;
    // 异步模式在 `Produce` 之外批量处理持久化操作；同步模式则在 `HandleProduce` 内部执行刷盘操作，随后才返回成功
    std::thread flusher_;
    std::atomic<bool> stop_flusher_{false};
    std::mutex flusher_mutex_;  // To ensure that releasing the lock and entering sleep are atomic.
    std::condition_variable flusher_condition_;
    std::mutex close_mutex_;
    bool started_{false};
    bool closed_{false};
};

}  // namespace logmq
