#pragma once

#include <deque>
#include <functional>
#include <memory>

#include "logmq/broker/broker_service.h"
#include "logmq/net/buffer.h"
#include "logmq/net/channel.h"
#include "logmq/net/worker_pool.h"
#include "logmq/protocol/codec.h"

namespace logmq {

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    using CloseCallback = std::function<void(int)>;

    TcpConnection(EventLoop* loop, int fd, BrokerService* service, WorkerPool* workers,
                  std::size_t read_buffer_bytes);
    ~TcpConnection();

    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;

    void Start();

    void Shutdown();

    [[nodiscard]] int fd() const;

    void SetCloseCallback(CloseCallback callback);
    
    void Tie();

private:
    void HandleRead();
    void HandleWrite();
    void HandleClose();
    void SubmitNext();
    void OnResponse(std::vector<std::byte> response);
    void QueueErrorAndClose(ProtocolErrorCode code, std::string message);


    EventLoop* loop_{nullptr};
    int fd_{-1};
    BrokerService* service_{nullptr};  // Main entrance;
    WorkerPool* workers_{nullptr};
    std::size_t read_buffer_bytes_{64 * 1024};  // The size of the temporary(lin shi) buffer;
    Channel channel_;
    Buffer output_;
    FrameDecoder decoder_;
    // Requests are processed one at a time per connection. This preserves
    // response order even though different connections use the same worker pool.
    std::deque<RequestEnvelope> requests_;
    bool processing_{false};  // In worker;
    bool closing_{false};
    CloseCallback close_callback_;  //To remove this connection from TcpServer::connections_;
};

}  // namespace logmq
