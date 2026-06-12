#include "logmq/net/tcp_server.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace logmq {
namespace {

Status SetNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return Status::IoError("fcntl(F_GETFL): " + std::string(std::strerror(errno)));
    }
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return Status::IoError("fcntl(F_SETFL): " + std::string(std::strerror(errno)));
    }
    return Status::Ok();
}

void CloseFd(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

void SetNoSigPipe(int fd) {
#ifdef SO_NOSIGPIPE
    // macOS sends SIGPIPE on writes to closed sockets unless this is disabled.
    int enabled = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#else
    (void)fd;
#endif
}

}  // namespace

TcpServer::TcpServer(TcpServerOptions options, BrokerService* service)
    : options_(std::move(options)),
      service_(service),
      io_loops_(options_.io_threads),
      workers_(options_.worker_threads, options_.worker_queue_size) {}

TcpServer::~TcpServer() { Stop(); }

Status TcpServer::Start() {
    Status status = main_loop_.Init();
    if (!status.ok()) {
        return status;
    }
    status = io_loops_.Start();
    if (!status.ok()) {
        return status;
    }
    status = CreateListenSocket();
    if (!status.ok()) {
        return status;
    }

    accept_channel_ = std::make_unique<Channel>(&main_loop_, listen_fd_);
    accept_channel_->SetReadCallback([this] { HandleAccept(); });
    accept_channel_->EnableReading();
    return Status::Ok();
}

void TcpServer::Loop() { main_loop_.Loop(); }

void TcpServer::Stop() {
    main_loop_.RunInLoop([this] {
        if (accept_channel_) {
            accept_channel_->DisableAll();
            accept_channel_->Remove();
            accept_channel_.reset();
        }
        std::vector<std::shared_ptr<TcpConnection>> connections;
        {
            std::lock_guard lock(connections_mutex_);
            // Copy shared_ptrs out of the map before calling Shutdown(); close
            // callbacks may erase entries from the same map.
            connections.reserve(connections_.size());
            for (auto& [_, connection] : connections_) {
                connections.push_back(connection);
            }
            connections_.clear();
        }
        for (auto& connection : connections) {
            connection->Shutdown();
        }
        CloseFd(listen_fd_);
    });
    main_loop_.Quit();
    io_loops_.Stop();
    workers_.Stop();
}

std::uint16_t TcpServer::bound_port() const { return bound_port_; }

Status TcpServer::CreateListenSocket() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return Status::IoError("socket: " + std::string(std::strerror(errno)));
    }
    SetNoSigPipe(listen_fd_);

    int enabled = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
        return Status::IoError("setsockopt(SO_REUSEADDR): " + std::string(std::strerror(errno)));
    }

    Status status = SetNonBlocking(listen_fd_);
    if (!status.ok()) {
        return status;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options_.port);
    if (options_.host.empty() || options_.host == "0.0.0.0") {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (::inet_pton(AF_INET, options_.host.c_str(), &addr.sin_addr) != 1) {
        return Status::InvalidArgument("tcp server host must be an IPv4 address");
    }

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        return Status::IoError("bind: " + std::string(std::strerror(errno)));
    }
    if (::listen(listen_fd_, SOMAXCONN) < 0) {
        return Status::IoError("listen: " + std::string(std::strerror(errno)));
    }

    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0) {
        bound_port_ = ntohs(bound.sin_port);
    } else {
        bound_port_ = options_.port;
    }
    return Status::Ok();
}

void TcpServer::HandleAccept() {
    while (true) {
        sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);
        const int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            return;
        }

        if (connections_.size() >= options_.max_connections) {
            ::close(fd);
            continue;
        }

        Status status = SetNonBlocking(fd);
        if (!status.ok()) {
            ::close(fd);
            continue;
        }
        SetNoSigPipe(fd);

        EventLoop* io_loop = io_loops_.NextLoop();
        if (io_loop == nullptr) {
            ::close(fd);
            continue;
        }

        // MainReactor accepts; a SubReactor owns the connection from creation on.
        io_loop->QueueInLoop([this, io_loop, fd] {
            auto connection = std::make_shared<TcpConnection>(
                io_loop, fd, service_, &workers_, options_.read_buffer_bytes);
            connection->SetCloseCallback([this](int closed_fd) { RemoveConnection(closed_fd); });
            connection->Tie();
            {
                std::lock_guard lock(connections_mutex_);
                connections_.emplace(fd, connection);
            }
            connection->Start();
        });
    }
}

void TcpServer::RemoveConnection(int fd) {
    std::lock_guard lock(connections_mutex_);
    connections_.erase(fd);
}

TcpServerOptions TcpServerOptionsFromConfig(const Config& config) {
    TcpServerOptions options;
    options.host = config.broker.host;
    options.port = config.broker.port;
    options.io_threads = static_cast<std::size_t>(config.network.io_threads);
    options.worker_threads = static_cast<std::size_t>(config.network.worker_threads);
    options.max_connections = config.network.max_connections;
    options.read_buffer_bytes = config.network.read_buffer_bytes;
    options.worker_queue_size = config.network.max_connections;
    return options;
}

}  // namespace logmq
