#include "logmq/net/channel.h"

#include <utility>

#include "logmq/net/event_loop.h"

namespace logmq {

Channel::Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd) {}

Channel::~Channel() { Remove(); }

int Channel::fd() const { return fd_; }

bool Channel::reading() const { return reading_; }

bool Channel::writing() const { return writing_; }

void Channel::SetReadCallback(EventCallback callback) {
    read_callback_ = std::move(callback);
}

void Channel::SetWriteCallback(EventCallback callback) {
    write_callback_ = std::move(callback);
}

void Channel::SetCloseCallback(EventCallback callback) {
    close_callback_ = std::move(callback);
}

void Channel::SetErrorCallback(EventCallback callback) {
    error_callback_ = std::move(callback);
}

void Channel::EnableReading() {
    reading_ = true;
    Update();
}

void Channel::EnableWriting() {
    writing_ = true;
    Update();
}

void Channel::DisableWriting() {
    writing_ = false;
    Update();
}

void Channel::DisableAll() {
    reading_ = false;
    writing_ = false;
    Update();
}

void Channel::Remove() {
    if (!removed_ && loop_ != nullptr && fd_ >= 0) {
        removed_ = true;
        loop_->RemoveChannel(this);
    }
}
void Channel::HandleEvent(bool readable, bool writable, bool closed, bool error) {
    if (tied_) {
        auto guard = tie_.lock();
        if (!guard) {
            return;
        }
        HandleEventWithGuard(readable, writable, closed, error);
        return;
    }
    HandleEventWithGuard(readable, writable, closed, error);
}

void Channel::HandleEventWithGuard(bool readable, bool writable, bool closed, bool error) {
    if (error && error_callback_) {
        error_callback_();
    }
    if (closed && close_callback_) {
        close_callback_();
        return;
    }
    if (readable && read_callback_) {
        read_callback_();
    }
    if (writable && write_callback_) {
        write_callback_();
    }
}

void Channel::Update() {
    if (loop_ != nullptr && !removed_) {
        loop_->UpdateChannel(this);
    }
}

void Channel::Tie(const std::shared_ptr<void>& owner) {
    tie_ = owner;
    tied_ = true;
}

}  // namespace logmq
