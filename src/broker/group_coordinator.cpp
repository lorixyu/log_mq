#include "logmq/broker/group_coordinator.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace logmq {
namespace {

Status ValidateSafeName(std::string_view name, std::string_view field) {
    if (name.empty()) {
        return Status::InvalidArgument(std::string(field) + " must not be empty");
    }
    if (name == "." || name == "..") {
        return Status::InvalidArgument(std::string(field) + " must not be . or ..");
    }
    for (unsigned char ch : name) {
        if (std::isalnum(ch) || ch == '.' || ch == '_' || ch == '-') {
            continue;
        }
        return Status::InvalidArgument(
            std::string(field) + " may only contain letters, digits, '.', '_' and '-'");
    }
    return Status::Ok();
}

Status UnknownMember() {
    return Status::NotFound("unknown member");
}

Status IllegalGeneration() {
    return Status::InvalidArgument("illegal generation");
}

}  // namespace

GroupCoordinator::GroupCoordinator(GroupCoordinatorOptions options)
    : options_(std::move(options)) {}

GroupCoordinator::~GroupCoordinator() {
    (void)Close();
}

Status GroupCoordinator::Start() {
    std::lock_guard lock(scanner_mutex_);
    if (started_) {
        return Status::Ok();
    }
    if (closed_) {
        return Status::InvalidArgument("group coordinator is closed");
    }

    stop_scanner_ = false;
    scanner_ = std::thread([this] { RunTimeoutScanner(); });
    started_ = true;
    return Status::Ok();
}

Status GroupCoordinator::Close() {
    {
        std::lock_guard lock(scanner_mutex_);
        if (closed_) {
            return Status::Ok();
        }
        closed_ = true;
        stop_scanner_ = true;
    }
    scanner_condition_.notify_all();
    if (scanner_.joinable()) {
        scanner_.join();
    }
    return Status::Ok();
}

Result<JoinGroupResponse> GroupCoordinator::JoinGroup(const JoinGroupRequest& request,
                                                      std::uint32_t partition_count) {
    Status status = ValidateGroupId(request.group_id);
    if (!status.ok()) {
        return status;
    }
    if (!request.member_id.empty()) {
        status = ValidateMemberId(request.member_id);
        if (!status.ok()) {
            return status;
        }
    }
    status = ValidateTopic(request.topic);
    if (!status.ok()) {
        return status;
    }
    if (partition_count == 0) {
        return Status::InvalidArgument("partition_count must be positive");
    }

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(mutex_);
    auto [group_it, inserted] = groups_.try_emplace(request.group_id);
    GroupState& group = group_it->second;
    if (inserted) {
        group.group_id = request.group_id;
        group.topic = request.topic;
        group.partition_count = partition_count;
    } else {
        if (group.topic != request.topic) {
            return Status::InvalidArgument("group is already subscribed to another topic");
        }
        if (group.partition_count != partition_count) {
            return Status::InvalidArgument("group topic partition count changed");
        }
    }

    std::string member_id = request.member_id;
    bool membership_changed = false;
    if (member_id.empty()) {
        member_id = "member-" + std::to_string(group.next_member_sequence++);
        membership_changed = true;
        group.members.emplace(member_id,
                              MemberState{member_id, now, PartitionAssignment{request.topic, {}}});
    } else {
        auto member = group.members.find(member_id);
        if (member == group.members.end()) {
            return UnknownMember();
        }
        member->second.last_heartbeat = now;
    }

    if (membership_changed || group.generation_id == 0) {
        if (group.generation_id == std::numeric_limits<std::int32_t>::max()) {
            return Status::Internal("group generation overflow");
        }
        ++group.generation_id;
        AssignLocked(group);
    }

    JoinGroupResponse response;
    response.group_id = request.group_id;
    response.member_id = member_id;
    response.generation_id = group.generation_id;
    response.leader_id = group.leader_id;
    return response;
}

Result<SyncGroupResponse> GroupCoordinator::SyncGroup(const SyncGroupRequest& request) {
    Status status = ValidateGroupId(request.group_id);
    if (!status.ok()) {
        return status;
    }
    status = ValidateMemberId(request.member_id);
    if (!status.ok()) {
        return status;
    }
    if (request.generation_id <= 0) {
        return IllegalGeneration();
    }

    std::lock_guard lock(mutex_);
    auto group_it = groups_.find(request.group_id);
    if (group_it == groups_.end()) {
        return UnknownMember();
    }
    GroupState& group = group_it->second;
    auto member = group.members.find(request.member_id);
    if (member == group.members.end()) {
        return UnknownMember();
    }
    if (request.generation_id != group.generation_id) {
        return IllegalGeneration();
    }
    member->second.last_heartbeat = std::chrono::steady_clock::now();

    SyncGroupResponse response;
    response.group_id = request.group_id;
    response.member_id = request.member_id;
    response.generation_id = group.generation_id;
    response.assignment = member->second.assignment;
    return response;
}

Result<HeartbeatResponse> GroupCoordinator::Heartbeat(const HeartbeatRequest& request) {
    Status status = ValidateGroupId(request.group_id);
    if (!status.ok()) {
        return status;
    }
    status = ValidateMemberId(request.member_id);
    if (!status.ok()) {
        return status;
    }
    if (request.generation_id <= 0) {
        return IllegalGeneration();
    }

    std::lock_guard lock(mutex_);
    auto group_it = groups_.find(request.group_id);
    if (group_it == groups_.end()) {
        return UnknownMember();
    }
    GroupState& group = group_it->second;
    auto member = group.members.find(request.member_id);
    if (member == group.members.end()) {
        return UnknownMember();
    }
    if (request.generation_id != group.generation_id) {
        return IllegalGeneration();
    }
    member->second.last_heartbeat = std::chrono::steady_clock::now();

    HeartbeatResponse response;
    response.group_id = request.group_id;
    response.member_id = request.member_id;
    response.generation_id = group.generation_id;
    return response;
}

Result<LeaveGroupResponse> GroupCoordinator::LeaveGroup(const LeaveGroupRequest& request) {
    Status status = ValidateGroupId(request.group_id);
    if (!status.ok()) {
        return status;
    }
    status = ValidateMemberId(request.member_id);
    if (!status.ok()) {
        return status;
    }

    std::lock_guard lock(mutex_);
    auto group_it = groups_.find(request.group_id);
    if (group_it == groups_.end()) {
        return UnknownMember();
    }
    GroupState& group = group_it->second;
    auto member = group.members.find(request.member_id);
    if (member == group.members.end()) {
        return UnknownMember();
    }

    group.members.erase(member);
    if (group.generation_id == std::numeric_limits<std::int32_t>::max()) {
        return Status::Internal("group generation overflow");
    }
    ++group.generation_id;
    const std::int32_t generation_id = group.generation_id;
    if (group.members.empty()) {
        groups_.erase(group_it);
    } else {
        AssignLocked(group);
    }

    LeaveGroupResponse response;
    response.group_id = request.group_id;
    response.member_id = request.member_id;
    response.generation_id = generation_id;
    return response;
}

Status GroupCoordinator::ValidateGroupId(std::string_view group_id) const {
    return ValidateSafeName(group_id, "group_id");
}

Status GroupCoordinator::ValidateMemberId(std::string_view member_id) const {
    return ValidateSafeName(member_id, "member_id");
}

Status GroupCoordinator::ValidateTopic(std::string_view topic) const {
    return ValidateSafeName(topic, "topic");
}

std::chrono::milliseconds GroupCoordinator::ScanInterval() const {
    const auto timeout = options_.consumer.session_timeout;
    const auto quarter = timeout / 4;
    if (quarter <= std::chrono::milliseconds(1)) {
        return std::chrono::milliseconds(1);
    }
    return std::min(quarter, std::chrono::milliseconds(100));
}

void GroupCoordinator::AssignLocked(GroupState& group) {
    group.leader_id.clear();
    if (group.members.empty()) {
        return;
    }
    group.leader_id = group.members.begin()->first;

    const std::size_t member_count = group.members.size();
    const std::uint32_t base = group.partition_count / static_cast<std::uint32_t>(member_count);
    const std::uint32_t extra = group.partition_count % static_cast<std::uint32_t>(member_count);

    std::uint32_t member_index = 0;
    for (auto& [member_id, member] : group.members) {
        (void)member_id;
        const std::uint32_t count = base + (member_index < extra ? 1U : 0U);
        const std::uint32_t start = member_index * base + std::min(member_index, extra);

        member.assignment.topic = group.topic;
        member.assignment.partition_ids.clear();
        member.assignment.partition_ids.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            member.assignment.partition_ids.push_back(static_cast<PartitionId>(start + i));
        }
        ++member_index;
    }
}

void GroupCoordinator::ExpireTimedOutMembers() {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(mutex_);
    for (auto group_it = groups_.begin(); group_it != groups_.end();) {
        GroupState& group = group_it->second;
        bool changed = false;
        for (auto member_it = group.members.begin(); member_it != group.members.end();) {
            if (now - member_it->second.last_heartbeat > options_.consumer.session_timeout) {
                member_it = group.members.erase(member_it);
                changed = true;
            } else {
                ++member_it;
            }
        }

        if (changed) {
            if (group.generation_id < std::numeric_limits<std::int32_t>::max()) {
                ++group.generation_id;
            }
            if (group.members.empty()) {
                group_it = groups_.erase(group_it);
                continue;
            }
            AssignLocked(group);
        }
        ++group_it;
    }
}

void GroupCoordinator::RunTimeoutScanner() {
    std::unique_lock lock(scanner_mutex_);
    while (true) {
        const bool stopped =
            scanner_condition_.wait_for(lock, ScanInterval(), [this] { return stop_scanner_; });
        if (stopped || stop_scanner_) {
            break;
        }
        lock.unlock();
        ExpireTimedOutMembers();
        lock.lock();
    }
}

GroupCoordinatorOptions GroupCoordinatorOptionsFromConfig(const ConsumerConfig& config) {
    GroupCoordinatorOptions options;
    options.consumer = config;
    return options;
}

}  // namespace logmq
