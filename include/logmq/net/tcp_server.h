#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "logmq/base/config.h"
#include "logmq/base/result.h"
#include "logmq/broker/broker_service.h"
#include "logmq/net/channel.h"
#include "logmq/net/event_loop.h"
#include "logmq/net/event_loop_thread_pool.h"
#include "logmq/net/tcp_connection.h"
#include "logmq/net/worker_pool.h"

namespace logmq {

struct TcpServerOptions {
    std::string host{"0.0.0.0"};
    std::uint16_t port{9092};
    std::size_t io_threads{1};
    std::size_t worker_threads{4};
    std::size_t max_connections{10000};
    std::size_t read_buffer_bytes{64 * 1024};
    std::size_t worker_queue_size{10000};
};

class TcpServer {
public:
    TcpServer(TcpServerOptions options, BrokerService* service);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    [[nodiscard]] Status Start();

    void Loop();

    void Stop();

    [[nodiscard]] std::uint16_t bound_port() const;

private:
    [[nodiscard]] Status CreateListenSocket();
    void HandleAccept();
    void RemoveConnection(int fd);

    TcpServerOptions options_;
    BrokerService* service_{nullptr};
    EventLoop main_loop_;
    EventLoopThreadPool io_loops_;
    WorkerPool workers_;
    int listen_fd_{-1};
    std::uint16_t bound_port_{0};
    std::unique_ptr<Channel> accept_channel_;
    std::mutex connections_mutex_;
    std::unordered_map<int, std::shared_ptr<TcpConnection>> connections_;
};

[[nodiscard]] TcpServerOptions TcpServerOptionsFromConfig(const Config& config);

}  // namespace logmq
