#include "logmq/broker/group_coordinator.h"
#include "logmq/broker/retry_scheduler.h"

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
    if (field == "topic" && IsInternalTopic(name)) {
        name.remove_prefix(IsRetryTopic(name) ? kRetryTopicPrefix.size() : kDlqTopicPrefix.size());
    }
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
    // group_id 必须是可安全保存和查找的非空名称。
    Status status = ValidateGroupId(request.group_id);
    if (!status.ok()) {
        return status;
    }
    // member_id 为空表示首次加入，由协调器生成；非空则表示已有成员重新加入。
    if (!request.member_id.empty()) {
        status = ValidateMemberId(request.member_id);
        if (!status.ok()) {
            return status;
        }
    }
    // 一个消费组只允许订阅一个合法 Topic。
    status = ValidateTopic(request.topic);
    if (!status.ok()) {
        return status;
    }
    // 没有 Partition 时无法为成员生成有效分配结果。
    if (partition_count == 0) {
        return Status::InvalidArgument("partition_count must be positive");
    }

    // steady_clock 不受系统时间调整影响，适合记录成员存活时间。
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(mutex_);
    // 消费组不存在时创建一个空的 GroupState，存在时返回已有状态。
    auto [group_it, inserted] = groups_.try_emplace(request.group_id);
    GroupState& group = group_it->second;
    if (inserted) {
        // 首个成员加入时固定消费组的 Topic 和 Partition 数量。
        group.group_id = request.group_id;
        group.topic = request.topic;
        group.partition_count = partition_count;
    } else {
        // 当前实现不允许同一个消费组切换到其他 Topic。
        if (group.topic != request.topic) {
            return Status::InvalidArgument("group is already subscribed to another topic");
        }
        // Partition 数量变化需要更完整的重平衡流程，当前直接拒绝。
        if (group.partition_count != partition_count) {
            return Status::InvalidArgument("group topic partition count changed");
        }
    }

    std::string member_id = request.member_id;
    bool membership_changed = false;
    if (member_id.empty()) {
        // 首次加入时生成组内唯一且递增的成员 ID。
        member_id = "member-" + std::to_string(group.next_member_sequence++);
        membership_changed = true;
        // 新成员暂时使用空分配，后续由 AssignLocked() 统一重平衡。
        group.members.emplace(member_id,
                              MemberState{member_id, now, PartitionAssignment{request.topic, {}}});
    } else {
        // 携带 member_id 的请求只能恢复已经登记过的成员。
        auto member = group.members.find(member_id);
        if (member == group.members.end()) {
            return UnknownMember();
        }
        // 重新加入同时证明成员仍然存活，刷新超时计时点。
        member->second.last_heartbeat = now;
    }

    // 成员集合发生变化时进入新代际，并重新选 Leader、分配 Partition。
    if (membership_changed || group.generation_id == 0) {
        if (group.generation_id == std::numeric_limits<std::int32_t>::max()) {
            return Status::Internal("group generation overflow");
        }
        ++group.generation_id;
        AssignLocked(group);
    }

    // JoinGroup 只返回成员身份和代际，具体 Partition 分配由 SyncGroup 获取。
    JoinGroupResponse response;
    response.group_id = request.group_id;
    response.member_id = member_id;
    response.generation_id = group.generation_id;
    response.leader_id = group.leader_id;
    return response;
}

Result<SyncGroupResponse> GroupCoordinator::SyncGroup(const SyncGroupRequest& request) {
    // SyncGroup 必须明确指定合法的消费组和成员。
    Status status = ValidateGroupId(request.group_id);
    if (!status.ok()) {
        return status;
    }
    status = ValidateMemberId(request.member_id);
    if (!status.ok()) {
        return status;
    }
    // 代际从 1 开始，非正数一定不是有效的 JoinGroup 结果。
    if (request.generation_id <= 0) {
        return IllegalGeneration();
    }

    // 在同一临界区内完成组、成员和代际检查，避免并发重平衡改变状态。
    std::lock_guard lock(mutex_);
    auto group_it = groups_.find(request.group_id);
    if (group_it == groups_.end()) {
        return UnknownMember();
    }
    GroupState& group = group_it->second;
    // 成员可能已主动离组或被超时扫描器移除。
    auto member = group.members.find(request.member_id);
    if (member == group.members.end()) {
        return UnknownMember();
    }
    // 旧代际成员不能读取新一轮重平衡产生的分配结果。
    if (request.generation_id != group.generation_id) {
        return IllegalGeneration();
    }
    // 成功同步也视为一次存活活动，延后成员超时时间。
    member->second.last_heartbeat = std::chrono::steady_clock::now();

    // 返回当前代际中 AssignLocked() 已经计算好的成员分配。
    SyncGroupResponse response;
    response.group_id = request.group_id;
    response.member_id = request.member_id;
    response.generation_id = group.generation_id;
    response.assignment = member->second.assignment;
    return response;
}

Result<HeartbeatResponse> GroupCoordinator::Heartbeat(const HeartbeatRequest& request) {
    // 心跳必须携带合法的消费组、成员和正代际编号。
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

    // 与超时扫描器串行访问成员状态，避免成员在刷新时被并发删除。
    std::lock_guard lock(mutex_);
    auto group_it = groups_.find(request.group_id);
    if (group_it == groups_.end()) {
        return UnknownMember();
    }
    GroupState& group = group_it->second;
    // 找不到成员通常意味着它已经离组或超过 session timeout。
    auto member = group.members.find(request.member_id);
    if (member == group.members.end()) {
        return UnknownMember();
    }
    // 代际不匹配说明消费组已经发生过重平衡，客户端需要重新加入。
    if (request.generation_id != group.generation_id) {
        return IllegalGeneration();
    }
    // 更新最后心跳时间，供后台超时扫描器判断成员是否存活。
    member->second.last_heartbeat = std::chrono::steady_clock::now();

    // 回显协调器认可的成员身份和当前代际。
    HeartbeatResponse response;
    response.group_id = request.group_id;
    response.member_id = request.member_id;
    response.generation_id = group.generation_id;
    return response;
}

Result<LeaveGroupResponse> GroupCoordinator::LeaveGroup(const LeaveGroupRequest& request) {
    // 主动离组必须明确指出合法的消费组和成员。
    Status status = ValidateGroupId(request.group_id);
    if (!status.ok()) {
        return status;
    }
    status = ValidateMemberId(request.member_id);
    if (!status.ok()) {
        return status;
    }

    // 与 Join、Sync、Heartbeat 和超时扫描操作串行修改组状态。
    std::lock_guard lock(mutex_);
    auto group_it = groups_.find(request.group_id);
    if (group_it == groups_.end()) {
        return UnknownMember();
    }
    GroupState& group = group_it->second;
    // 已经被移除的成员不能重复执行 LeaveGroup。
    auto member = group.members.find(request.member_id);
    if (member == group.members.end()) {
        return UnknownMember();
    }

    // 成员集合发生变化，需要进入新代际并触发重平衡。
    group.members.erase(member);
    if (group.generation_id == std::numeric_limits<std::int32_t>::max()) {
        return Status::Internal("group generation overflow");
    }
    ++group.generation_id;
    // 先保存代际，因为最后一个成员离开后整个 GroupState 会被删除。
    const std::int32_t generation_id = group.generation_id;
    if (group.members.empty()) {
        // 没有任何成员时释放整个消费组状态。
        groups_.erase(group_it);
    } else {
        // 为剩余成员重新选 Leader 并平均分配 Partition。
        AssignLocked(group);
    }

    // 返回成员离开后产生的新代际编号。
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
