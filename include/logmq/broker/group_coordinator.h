#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "logmq/base/config.h"
#include "logmq/base/result.h"
#include "logmq/protocol/types.h"

namespace logmq {

struct GroupCoordinatorOptions {
    ConsumerConfig consumer;
};

class GroupCoordinator {
public:
    explicit GroupCoordinator(GroupCoordinatorOptions options);
    ~GroupCoordinator();

    [[nodiscard]] Status Start();
    [[nodiscard]] Status Close();

    [[nodiscard]] Result<JoinGroupResponse> JoinGroup(const JoinGroupRequest& request,
                                                      std::uint32_t partition_count);
    [[nodiscard]] Result<SyncGroupResponse> SyncGroup(const SyncGroupRequest& request);
    [[nodiscard]] Result<HeartbeatResponse> Heartbeat(const HeartbeatRequest& request);
    [[nodiscard]] Result<LeaveGroupResponse> LeaveGroup(const LeaveGroupRequest& request);

private:
    struct MemberState {
        std::string member_id;
        std::chrono::steady_clock::time_point last_heartbeat;
        PartitionAssignment assignment;
    };

    struct GroupState {
        std::string group_id;
        TopicName topic;
        std::uint32_t partition_count{0};
        std::int32_t generation_id{0};
        std::uint64_t next_member_sequence{1};
        std::string leader_id;
        std::map<std::string, MemberState> members;
    };

    [[nodiscard]] Status ValidateGroupId(std::string_view group_id) const;
    [[nodiscard]] Status ValidateMemberId(std::string_view member_id) const;
    [[nodiscard]] Status ValidateTopic(std::string_view topic) const;
    [[nodiscard]] std::chrono::milliseconds ScanInterval() const;
    void AssignLocked(GroupState& group);
    void ExpireTimedOutMembers();
    void RunTimeoutScanner();

    GroupCoordinatorOptions options_;
    mutable std::mutex mutex_;
    std::map<std::string, GroupState> groups_;

    std::thread scanner_;
    std::mutex scanner_mutex_;
    std::condition_variable scanner_condition_;
    bool started_{false};
    bool closed_{false};
    bool stop_scanner_{false};
};

[[nodiscard]] GroupCoordinatorOptions GroupCoordinatorOptionsFromConfig(
    const ConsumerConfig& config);

}  // namespace logmq
