#include "logmq/net/tcp_connection.h"

#include <cerrno>
#include <cstring>
#include <span>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "logmq/net/event_loop.h"

namespace logmq {

TcpConnection::TcpConnection(EventLoop* loop, int fd, BrokerService* service,
                             WorkerPool* workers, std::size_t read_buffer_bytes)
    : loop_(loop),
      fd_(fd),
      service_(service),
      workers_(workers),
      read_buffer_bytes_(read_buffer_bytes),
      channel_(loop, fd) {
    channel_.SetReadCallback([this] { HandleRead(); });
    channel_.SetWriteCallback([this] { HandleWrite(); });
    channel_.SetCloseCallback([this] { HandleClose(); });
    channel_.SetErrorCallback([this] { HandleClose(); });
}

TcpConnection::~TcpConnection() {
    channel_.DisableAll();
    channel_.Remove();
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

// 该连接的 fd 可读
void TcpConnection::Start() { channel_.EnableReading(); }

void TcpConnection::Shutdown() {
    auto self = shared_from_this();
    // Shutdown can be requested from TcpServer/main thread; mutate connection
    // state only on the owning IO loop.
    loop_->QueueInLoop([self] {
        self->closing_ = true;
        if (self->output_.readable_bytes() == 0) {
            self->HandleClose();
        }
    });
}

int TcpConnection::fd() const { return fd_; }

void TcpConnection::SetCloseCallback(CloseCallback callback) {
    close_callback_ = std::move(callback);
}

void TcpConnection::Tie() {
    channel_.Tie(shared_from_this());
}

void TcpConnection::HandleRead() {
    while (true) {
        std::vector<std::byte> chunk(read_buffer_bytes_);
        const ssize_t n = ::read(fd_, chunk.data(), chunk.size());
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            HandleClose();
            return;
        }
        if (n == 0) {
            HandleClose();
            return;
        }

        chunk.resize(static_cast<std::size_t>(n));
        // FrameDecoder owns partial frame state, so every read chunk can be
        // pushed directly even when requests are split or stuck together.
        auto decoded = decoder_.Push(std::span<const std::byte>(chunk.data(), chunk.size()));
        if (!decoded.ok()) {
            QueueErrorAndClose(ProtocolErrorCode::kInvalidRequest,
                               std::string(decoded.status().message()));
            return;
        }
        for (auto& request : decoded.value()) {
            requests_.push_back(std::move(request));
        }
    }
    SubmitNext();
}

void TcpConnection::HandleWrite() {
    Status status = output_.WriteToFd(fd_);
    if (!status.ok()) {
        HandleClose();
        return;
    }
    if (output_.readable_bytes() == 0) {
        channel_.DisableWriting();
        if (closing_) {
            HandleClose();
        }
    }
}

void TcpConnection::HandleClose() {
    if (fd_ < 0) {
        return;
    }
    auto self = shared_from_this();
    channel_.DisableAll();
    channel_.Remove();
    const int closed_fd = fd_;
    fd_ = -1;
    ::close(closed_fd);
    if (close_callback_) {
        close_callback_(closed_fd);
    }
}

void TcpConnection::SubmitNext() {
    if (processing_ || closing_ || requests_.empty()) {
        return;
    }

    // Keep only one in-flight worker task for this connection. That avoids
    // reordering Produce/Fetch responses sent over the same TCP stream.
    processing_ = true;
    RequestEnvelope request = std::move(requests_.front());
    requests_.pop_front();

    auto self = shared_from_this();
    RequestEnvelope request_for_worker = request;
    const bool accepted = workers_->Submit([self, request = std::move(request_for_worker)]() mutable {
        ResponseEnvelope response = self->service_->Handle(request);
        auto encoded = EncodeResponse(response);
        std::vector<std::byte> bytes;
        if (encoded.ok()) {
            bytes = std::move(encoded).value();
        } else {
            // Encoding a success response should be deterministic; if it fails,
            // return a protocol-level internal error instead of dropping the reply.
            auto fallback = EncodeErrorResponse(request.api_key, kProtocolVersion, request.request_id,
                                                ProtocolErrorCode::kInternal,
                                                encoded.status().message());
            if (fallback.ok()) {
                bytes = std::move(fallback).value();
            }
        }
        // Worker threads must not write the socket directly. Hand the bytes back
        // to the owning IO loop, where the fd and output buffer live.
        self->loop_->QueueInLoop([self, bytes = std::move(bytes)]() mutable {
            self->OnResponse(std::move(bytes));
        });
    });

    if (!accepted) {
        processing_ = false;
        auto encoded = EncodeErrorResponse(request.api_key, kProtocolVersion, request.request_id,
                                           ProtocolErrorCode::kInternal,
                                           "worker queue is full");
        if (encoded.ok()) {
            output_.Append(encoded.value());
            channel_.EnableWriting();
        }
        closing_ = true;
    }
}

void TcpConnection::OnResponse(std::vector<std::byte> response) {
    processing_ = false;
    if (!response.empty()) {
        output_.Append(response);
        channel_.EnableWriting();
    }
    SubmitNext();
}

void TcpConnection::QueueErrorAndClose(ProtocolErrorCode code, std::string message) {
    // A malformed frame may not have a trustworthy api_key/request_id yet, so
    // use a neutral envelope and close after the error is flushed.
    auto encoded = EncodeErrorResponse(ApiKey::kProduce, kProtocolVersion, 0, code, message);
    if (encoded.ok()) {
        output_.Append(encoded.value());
        channel_.EnableWriting();
    }
    closing_ = true;
    decoder_.Clear();
    requests_.clear();
}

}  // namespace logmq
