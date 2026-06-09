#pragma once

#include <functional>
#include <memory>

namespace logmq {

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel();

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    [[nodiscard]] int fd() const;
    [[nodiscard]] bool reading() const;
    [[nodiscard]] bool writing() const;

    void SetReadCallback(EventCallback callback);
    void SetWriteCallback(EventCallback callback);
    void SetCloseCallback(EventCallback callback);
    void SetErrorCallback(EventCallback callback);

    void EnableReading();
    void EnableWriting();
    void DisableWriting();
    void DisableAll();
    void Remove();
    
    void HandleEvent(bool readable, bool writable, bool closed, bool error);

    void Tie(const std::shared_ptr<void>& owner);

private:
    void HandleEventWithGuard(bool readable, bool writable, bool closed, bool error);
    void Update();

    EventLoop* loop_{nullptr};
    int fd_{-1};
    bool reading_{false};
    bool writing_{false};
    bool removed_{false};
    EventCallback read_callback_;
    EventCallback write_callback_;
    EventCallback close_callback_;
    EventCallback error_callback_;

    bool tied_{false};
    std::weak_ptr<void> tie_;
};

}  // namespace logmq
