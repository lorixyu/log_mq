#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "logmq/base/config.h"
#include "logmq/base/result.h"
#include "logmq/broker/offset_store.h"
#include "logmq/broker/topic_manager.h"
#include "logmq/protocol/types.h"

namespace logmq {

struct BrokerServiceOptions {
    StorageConfig storage;
};

class BrokerService {
public:
    explicit BrokerService(BrokerServiceOptions options);
    ~BrokerService();

    [[nodiscard]] Status Start();

    [[nodiscard]] ResponseEnvelope Handle(const RequestEnvelope& request);

    [[nodiscard]] Status Close();

private:
    // BrokerService is the protocol/business boundary: it validates typed
    // requests, calls TopicManager/CommitLog, then maps Status into protocol errors.
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

    [[nodiscard]] ResponseEnvelope MakeError(const RequestEnvelope& envelope,
                                             ProtocolErrorCode code,
                                             std::string message) const;
    [[nodiscard]] ResponseEnvelope MakeSuccess(const RequestEnvelope& envelope,
                                               ResponseBody body) const;
    void RunAsyncFlusher();

    BrokerServiceOptions options_;
    TopicManager topics_;
    OffsetStore offsets_;
    // Async mode batches durability work outside Produce; sync mode flushes
    // inside HandleProduce before returning success.
    std::thread flusher_;
    std::atomic<bool> stop_flusher_{false};
    std::mutex flusher_mutex_;  // To ensure that releasing the lock and entering sleep are atomic.
    std::condition_variable flusher_condition_;
    std::mutex close_mutex_;
    bool started_{false};
    bool closed_{false};
};

}  // namespace logmq
