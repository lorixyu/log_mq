# Phase 5.1 Retry/DLQ Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 完成路线图 5.1 的 Nack、5 秒延迟、三次重试和 DLQ。
**Architecture:** 复用 CommitLog；内部 envelope 保存元数据，调度器推进连续可见前缀；SDK 为原 Topic 与 Retry Topic 分别管理游标。
**Tech Stack:** C++20、CMake、标准库、GoogleTest。
**Spec:** `docs/superpowers/specs/2026-10-10-phase5-1-retry-dlq-design.md`

## Global Constraints

- 默认延迟 5000ms，max_retries=3，第 4 次失败进入 DLQ。
- 保留版本 1 布局和现有磁盘格式；新增版本 2 Fetch 元数据。
- 不增加依赖，保留已有未提交改动；在当前工作区实施，最终业务变更保持可审阅 diff。
- 用户已明确要求“实施”，采用当前会话直接执行。

## Review Focus

- batch 中间 offset 必须复制准确记录。
- Retry 尚不存在、Rebalance、重新订阅不能污染双游标。
- 未来记录、并发追加及恢复不能产生可见前缀空洞。
- 内部名称、损坏 envelope、大消息和重试计数必须校验。
- Nack 成功不隐式提交，DLQ 不自动 Nack 或调度。

### Task 1: Protocol

**Files:** `include/logmq/protocol/types.h`, `src/protocol/codec.cpp`, `tests/unit/protocol_test.cpp`.
**Interfaces:** NackRequest{topic,partition_id,offset,reason}; NackResponse{topic,partition_id,offset,retry,dead_letter}; ProtocolRecord::retry 可选 RetryMetadata；kRetryProtocolVersion=2。

- [x] 写入 Nack 往返、v2 Fetch 元数据、截断和 v1 兼容测试。
- [x] 构建并观察新增测试因缺失类型/编解码而失败。
- [x] 实现协议类型、版本支持、Nack 编解码，保留 v1 Fetch 字节布局。
- [x] `cmake --build build -j 4 && build/tests/logmq_protocol_test`。
  Expected: 全部协议测试通过。

### Task 2: Broker and scheduler

**Files:** 新增 `include/logmq/broker/retry_scheduler.h`, `src/broker/retry_scheduler.cpp`；修改 BrokerService、TopicManager、CMake 和 `tests/unit/broker_service_test.cpp`。
**Interfaces:** RetryOptions{delay,max_retries}; RetryScheduler(TopicManager&), Start(), Schedule(topic,partition,offset,time), VisibleEnd(topic,partition), Close(); EncodeRetryRecord/DecodeRetryRecord；BrokerServiceOptions::retry。

- [x] 写入延迟、三次重试/DLQ、准确 batch offset、非法请求、并发、恢复/大消息/损坏 envelope 测试。
- [x] 运行构建与 Broker 测试观察失败。
- [x] 实现单记录内部 envelope、调度器、Nack 路由和连续可见 Fetch。
- [x] `cmake --build build -j 4 && build/tests/logmq_broker_service_test`。
  Expected: 所有 Broker 测试通过。

### Task 3: Consumer and acceptance

**Files:** `include/logmq/client/client.h`, `src/client/client.cpp`, `tests/integration/network_test.cpp`；路线图及开发笔记。
**Interfaces:** Consumer::Nack(const ConsumerRecord&,std::string)->Result<NackResponse>；ConsumerRecord::retry；Assignment::retry_next_offset。

- [x] 写入 SDK Nack、自动重拉、双游标 Commit/重订阅、默认 5 秒、DLQ 检查的真实网络测试。
- [x] 构建并观察 SDK 功能缺失导致的失败。
- [x] 实现 Nack、Poll 双 Topic、CommitSync 与 JoinAndSync 的双游标恢复；添加 Retry Topic Seek 支持以便失败后回退。
- [x] `cmake --build build -j 4 && ctest --test-dir build --output-on-failure`。
  Expected: 完整测试通过。
- [x] 更新路线图四项验收勾选与开发笔记，执行独立代码审阅，修复重要发现并重复全套验证。

最终验证（2026-10-10）：完整构建无警告，CTest 82/82 通过。
独立审阅发现的内部日志损坏被截断、普通 Fetch 临界帧超限两项问题均以 RED→GREEN 回归测试修复。
