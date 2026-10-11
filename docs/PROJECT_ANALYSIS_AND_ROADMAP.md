# xLogMQ 项目分析与分布式演进路线图

> 生成时间: 2026-09-26  
> 分析基于: 当前 main 分支代码状态

---

## 一、项目完成度总结

### 1.1 整体评估

LogMQ 目前已完成 **Phase 1-4** 的核心功能，是一个功能较为完整的**单机高性能消息队列**，具备以下特征：

- ✅ **存储层完整度: 95%** - CommitLog、Segment、OffsetIndex、崩溃恢复机制完备
- ✅ **网络层完整度: 90%** - Reactor 模型、自定义二进制协议、半包/粘包处理
- ✅ **Broker 核心完整度: 85%** - Topic/Partition、Produce/Fetch、元数据管理
- ✅ **消费语义完整度: 80%** - Consumer Group、Offset Store、Rebalance 机制
- ⚠️ **分布式能力: 0%** - 尚未实现 Leader/Follower 复制
- ⚠️ **可靠性增强: 30%** - 缺少 Retry/DLQ、背压机制
- ⚠️ **观测性: 40%** - 有 benchmark，缺少实时 Metrics 和管理接口

### 1.2 已完成的核心模块

#### Layer 1: 基础工具层 ✅
- `Result<T>` / `Status` 错误处理模型
- `Config` YAML 配置解析（StorageConfig、NetworkConfig、ConsumerConfig）
- 基础类型定义（TopicName、PartitionId、Offset、BrokerId）

#### Layer 2: 存储层 ✅✅
**这是项目最成熟的部分，实现质量高**

- **CommitLog** (`commit_log.h/cpp`): 
  - 顺序追加、按 offset 读取
  - 自动 Segment 滚动
  - 崩溃恢复、半条消息截断
  - 线程安全的 append/read
  
- **Segment** (`segment.h/cpp`):
  - 基于 `base_offset` 的文件命名
  - CRC32 校验
  - 支持恢复扫描
  
- **OffsetIndex** (`offset_index.h/cpp`):
  - 稀疏索引（每 4KB 一条）
  - 二分查找 offset → physical position
  
- **Record/RecordBatch** (`record.h/cpp`, `record_batch.h/cpp`):
  - 完整的序列化/反序列化
  - Batch 合并多条消息减少开销

#### Layer 3: Broker 内核层 ✅
- **TopicManager** (`topic_manager.h/cpp`):
  - Topic 创建、Partition 管理
  - Partition 选择策略（key hash / round-robin）
  - 元数据持久化到 `metadata.json`
  - 使用 `std::shared_mutex` 优化读多写少场景
  
- **BrokerService** (`broker_service.h/cpp`):
  - 处理 10 种协议请求（Produce、Fetch、Metadata、CreateTopic、CommitOffset、FetchCommittedOffset、JoinGroup、SyncGroup、Heartbeat、LeaveGroup）
  - 异步刷盘线程（`RunAsyncFlusher`）
  - 请求路由和错误处理

#### Layer 4: 消费语义层 ✅
- **OffsetStore** (`offset_store.h/cpp`):
  - 按 group/topic/partition 存储已提交 offset
  - 持久化到本地文件
  - 原子性写入（临时文件 + rename）
  
- **GroupCoordinator** (`group_coordinator.h/cpp`):
  - Consumer Group 管理
  - JoinGroup / SyncGroup / Heartbeat / LeaveGroup
  - Rebalance 机制（RangeAssignor）
  - Generation ID 防止旧分配冲突
  - 后台线程扫描超时 Consumer

#### Layer 5: 网络协议层 ✅
- **TCP Server** (`tcp_server.h/cpp`):
  - Main Reactor + Sub Reactor 多线程模型
  - EventLoop + Channel + Poller（epoll/kqueue）
  - WorkerPool 处理业务逻辑
  
- **Protocol Codec** (`codec.h/cpp`):
  - 自定义二进制协议（4B total_len + 2B api_key + 2B version + 8B request_id + body）
  - 半包/粘包处理
  - 10 种请求/响应类型完整支持
  
- **Client SDK** (`client.h/cpp`):
  - `AdminClient`: 创建 Topic、查询 Metadata
  - `Producer`: 单条/批量发送
  - `Consumer`: Subscribe、Poll、CommitSync、Rebalance 处理
  - 持久连接复用（2026-09-26 修复）

#### Layer 6: 工具与测试 ⚠️
- **Benchmark** (`logmq_benchmark.cpp`):
  - 存储层直接测试
  - 网络层端到端测试
  - 支持多 Producer/Consumer、多 Partition、不同 Batch Size
  - 输出 QPS、P50/P99、吞吐量
  
- **CLI 工具** (`logmq_cli.cpp`):
  - 命令行创建 Topic、发送/接收消息、查询 Metadata
  
- **测试覆盖**:
  - ✅ 单元测试: storage、protocol、broker_service
  - ✅ 集成测试: network_test（端到端）
  - ⚠️ 缺少故障测试、压力测试

---

## 二、当前架构分析

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      LogMQ (Single Broker)                   │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┴───────────────────┐
        │                                       │
   ┌────▼────┐                          ┌──────▼─────┐
   │ Producer│                          │  Consumer  │
   │  Client │                          │   Client   │
   └────┬────┘                          └──────┬─────┘
        │                                      │
        │   Custom Binary Protocol (TCP)      │
        │                                      │
        └──────────────┬───────────────────────┘
                       │
              ┌────────▼──────────┐
              │   TcpServer       │
              │  (Main Reactor)   │
              └────────┬──────────┘
                       │
       ┌───────────────┼───────────────┐
       │               │               │
   ┌───▼────┐     ┌───▼────┐     ┌───▼────┐
   │SubReactor│   │SubReactor│   │SubReactor│
   │ Thread 1│    │ Thread 2│    │ Thread N│
   └───┬────┘     └───┬────┘     └───┬────┘
       │              │              │
       └──────────────┼──────────────┘
                      │
              ┌───────▼──────────┐
              │   WorkerPool     │
              │  (Thread Pool)   │
              └───────┬──────────┘
                      │
              ┌───────▼──────────┐
              │  BrokerService   │
              └───────┬──────────┘
                      │
       ┌──────────────┼──────────────┐
       │              │              │
   ┌───▼───────┐  ┌──▼──────┐  ┌───▼──────────┐
   │TopicManager│ │OffsetStore│ │GroupCoordinator│
   └───┬───────┘  └──┬──────┘  └───┬──────────┘
       │             │             │
       │         (files)       (memory)
       │
   ┌───▼────────────────────┐
   │   CommitLog Pool       │
   │  (One per Partition)   │
   └───┬────────────────────┘
       │
   ┌───▼─────────┬──────────┬─────────┐
   │ Segment-0   │ Segment-1│Segment-N│
   │  + Index    │  + Index │ + Index │
   └─────────────┴──────────┴─────────┘
        (data/topic/partition/)
```

### 2.2 核心设计亮点

#### 2.2.1 存储设计 ⭐⭐⭐⭐⭐
- **顺序写优化**: CommitLog 是 append-only，磁盘顺序写可达 500+ MB/s
- **稀疏索引**: OffsetIndex 每 4KB 记录一条，减少索引文件大小，查找时少量顺序扫描
- **崩溃恢复**: 启动时扫描最后一个 Segment，校验 CRC 和 batch 长度，截断脏尾
- **Segment 滚动**: 按文件大小自动滚动，便于删除旧数据（Retention）

#### 2.2.2 网络模型 ⭐⭐⭐⭐
- **Reactor 多线程**: Main Reactor accept，SubReactor 处理 IO，Worker 执行业务
- **零拷贝思想**: Buffer 管理读写指针，减少不必要的内存复制
- **协议版本化**: 每个请求带 version 字段，便于未来扩展

#### 2.2.3 消费语义 ⭐⭐⭐⭐
- **Consumer Group**: 同组内负载均衡，不同组广播消费
- **Rebalance**: Member 超时或新增时重新分配 Partition
- **Generation ID**: 防止 Rebalance 期间旧分配仍然生效
- **At-Least-Once**: 先处理再 commit offset，可能重复但不丢消息

### 2.3 性能数据（来自 PERFORMANCE_20260805.md）

| 场景 | QPS | P99 延迟 | 吞吐量 |
|------|-----|---------|-------|
| 4 Producers / 4 Partitions / Batch=1 | 77,834 msg/s | 0.101 ms | 19.0 MiB/s |
| 4 Producers / 4 Partitions / Batch=10 | 278,622 msg/s | 0.213 ms | 68.0 MiB/s |
| 4 Producers / 4 Partitions / Batch=100 | 380,343 msg/s | 1.614 ms | 92.9 MiB/s |
| Sync Flush / 4P / Batch=10 | 143,955 msg/s | 0.782 ms | - |
| Async Flush / 4P / Batch=10 | 278,622 msg/s | 0.213 ms | - |

**关键发现**:
- Batch 提升吞吐显著（Batch=10 比 Batch=1 快 3.6 倍）
- 异步刷盘比同步快 1.9 倍
- 存储层直接测试 255K msg/s，端到端 77K，瓶颈在网络/协议层

---

## 三、存在的问题与不足

### 3.1 功能缺失

| 模块 | 缺失功能 | 影响 |
|------|---------|------|
| **可靠性** | Retry Topic、DLQ | 消费失败无法重试 |
| **背压** | 连接级、队列级限流 | 高负载可能 OOM |
| **分布式** | Leader/Follower、ISR、Controller | 单机故障 = 不可用 |
| **观测性** | Prometheus Metrics、HTTP 管理接口 | 生产环境不可运维 |
| **安全** | 认证、加密、ACL | 无法用于多租户 |

### 3.2 工程问题

#### 3.2.1 连接管理 ⚠️
- **问题**: 无连接级 idle timeout，僵尸连接占用 fd
- **影响**: 长时间运行可能 fd 泄漏
- **解决方案**: 见 `LogMQ_未来拓展与架构TODO.md` 第 1 节

#### 3.2.2 元数据同步 ⚠️
- **问题**: TopicManager 的 `metadata.json` 是同步写，可能阻塞
- **影响**: CreateTopic 期间其他请求卡住
- **解决方案**: 改为后台线程定期持久化 + WAL

#### 3.2.3 测试覆盖 ⚠️
- **问题**: 
  - 缺少故障注入测试（kill broker、磁盘满、网络断连）
  - 缺少长时间 soak test
  - Consumer rebalance 压力测试不足
- **解决方案**: 补充 Phase 7 的测试矩阵

#### 3.2.4 文档 ⚠️
- **问题**: 
  - `05_phase4_week7_consumer_group.md` 是空文件
  - 缺少面试问题文档
  - 缺少 API 文档
- **解决方案**: 补充开发笔记和 README

---

## 四、分布式演进路线图

### 4.1 演进原则

1. **先把单机做深**：补齐 Retry/DLQ、背压、Metrics，让单机版本可生产化
2. **控制面与数据面分离**：Controller 管理元数据，Broker 承载数据流量
3. **渐进式复制**：先做 Leader/Follower，再做 ISR，最后做故障切换
4. **保持面试友好**：每个阶段都有可演示的 demo 和可讲的设计权衡

### 4.2 总体时间线

```
┌───────────┬──────────┬────────────────────────────────┐
│   阶段    │   时间   │          核心目标              │
├───────────┼──────────┼────────────────────────────────┤
│ Phase 5   │ 1-2 周   │ Retry/DLQ、背压、连接管理      │
│ Phase 6   │ 1-2 周   │ Metrics、HTTP 管理、优化报告   │
│ Phase 7   │ 1-2 周   │ 故障测试、文档、简历材料       │
│           │          │ *** 单机版本完成，可投简历 *** │
│ Phase 8   │ 2-3 周   │ Controller + Broker 注册心跳   │
│ Phase 9   │ 2-3 周   │ Leader/Follower 复制 + HW      │
│ Phase 10  │ 2-3 周   │ Leader Failover + 集群测试     │
│           │          │ *** 分布式版本完成 ***         │
└───────────┴──────────┴────────────────────────────────┘
```

---

## 五、Phase 5: 可靠性增强（1-2 周）

### 目标
补齐业务后端最关心的失败治理和系统保护机制。

### 5.1 Retry Topic 与 DLQ

> 已完成：2026-10-10。实现及使用说明见
> [Phase 5.1 开发笔记](dev-notes/06_phase5_1_retry_dlq.md)。
> 到期消息由 RetryScheduler 推进连续可见前缀，不改写 CommitLog；
> Consumer 自动拉取已分配分区对应的 Retry Topic，并分别管理、提交两类消费位置。

#### 设计
```
Consumer Poll → 处理失败 → Nack(record)
    ↓
BrokerService.Nack(topic, partition, offset, reason)
    ↓
写入 %RETRY%{topic} (retry_count=1, next_visible_time=now+5s)
    ↓
后台线程扫描 Retry Topic → 到期消息重新可见
    ↓
Consumer 再次拉取 → 失败 → retry_count++
    ↓
retry_count > max_retries → 写入 %DLQ%{topic}
```

#### 实现要点
- 在 `protocol/types.h` 增加 `NackRequest` / `NackResponse`
- 在 `BrokerService` 增加 `HandleNack`
- Retry 消息的 Header 增加:
  ```cpp
  struct RetryMetadata {
      std::uint32_t retry_count{0};
      std::uint64_t next_visible_time{0};  // unix timestamp ms
      std::string original_topic;
      std::string failure_reason;
  };
  ```
- 实现 `RetryScheduler`:
  - 使用 `std::priority_queue<RetryTask, ..., CompareByVisibleTime>`
  - 后台线程定期扫描
  - 到期消息修改状态为可见

#### 验收标准
- [x] Consumer Nack 后消息进入 Retry Topic
- [x] 5 秒后 Consumer 能重新拉取
- [x] 超过 3 次进入 DLQ（第 4 次失败）
- [x] DLQ 消息不再自动重试

### 5.2 背压机制

#### 多层背压设计

| 层次 | 策略 | 阈值 |
|------|------|------|
| **连接输入 Buffer** | 超过 64KB 暂停读事件 | 64KB |
| **连接输出 Buffer** | 超过 1MB 关闭慢客户端 | 1MB |
| **Worker 队列** | 超过 10000 返回 `kTooManyRequests` | 10000 |
| **CommitLog Pending** | 待刷盘超过 100MB 拒绝写入 | 100MB |

#### 实现要点
- 在 `TcpConnection` 增加:
  ```cpp
  void CheckBackpressure() {
      if (input_buffer_.readable_bytes() > kMaxInputBytes) {
          DisableReading();
      }
      if (output_buffer_.readable_bytes() > kMaxOutputBytes) {
          Shutdown();
      }
  }
  ```
- 在 `WorkerPool` 增加:
  ```cpp
  bool TrySubmit(Task task) {
      std::lock_guard lock(mutex_);
      if (queue_.size() >= max_queue_size_) {
          return false;  // Caller returns kTooManyRequests
      }
      queue_.push(std::move(task));
      return true;
  }
  ```
- 在 `CommitLog` 增加 `pending_bytes_` 计数
- 刷盘后 `pending_bytes_ -= flushed_bytes`

#### 验收标准
- [ ] 慢客户端不读响应时不阻塞其他连接
- [ ] Worker 队列满时返回明确错误
- [ ] 高频写入时 Broker 内存不超过 2GB

### 5.3 连接级 Idle Timeout

**详见 `LogMQ_未来拓展与架构TODO.md` 第 1 节**

核心思路：
- `TcpConnection` 记录 `last_active_`
- `TcpServer` 启动 idle scanner 线程
- 每 15 秒扫描一次，超过 60 秒无活动 → 关闭连接
- Consumer Group 心跳超时不受影响（两层独立机制）

#### 验收标准
- [ ] 连接建立 60 秒不发数据 → 被关闭
- [ ] 持续发请求的连接不被误关
- [ ] Consumer Group 心跳机制不受影响

---

## 六、Phase 6: Metrics 与优化（1-2 周）

### 目标
让项目可量化、可观测、可演示。

### 6.1 Prometheus Metrics

#### 指标设计

| 指标名 | 类型 | 含义 |
|-------|------|------|
| `logmq_produce_total` | Counter | 累计写请求数 |
| `logmq_produce_bytes_total` | Counter | 累计写入字节数 |
| `logmq_produce_latency_seconds` | Histogram | Produce 延迟分布 |
| `logmq_fetch_total` | Counter | 累计拉取请求数 |
| `logmq_fetch_latency_seconds` | Histogram | Fetch 延迟分布 |
| `logmq_flush_latency_seconds` | Histogram | fsync 延迟分布 |
| `logmq_consumer_lag` | Gauge | 消费积压 (high_watermark - committed_offset) |
| `logmq_active_connections` | Gauge | 当前连接数 |
| `logmq_worker_queue_depth` | Gauge | Worker 队列深度 |
| `logmq_rebalance_total` | Counter | Rebalance 次数 |
| `logmq_storage_error_total` | Counter | 存储错误次数 |

#### 实现要点
- 创建 `include/logmq/metrics/` 模块
- 使用 `std::atomic<uint64_t>` 记录 Counter
- 使用 `std::mutex` 保护 Histogram 的 buckets
- HTTP `/metrics` 端点返回 Prometheus 文本格式:
  ```
  # TYPE logmq_produce_total counter
  logmq_produce_total 123456
  # TYPE logmq_produce_latency_seconds histogram
  logmq_produce_latency_seconds_bucket{le="0.001"} 100
  logmq_produce_latency_seconds_bucket{le="0.01"} 950
  logmq_produce_latency_seconds_bucket{le="+Inf"} 1000
  logmq_produce_latency_seconds_sum 5.234
  logmq_produce_latency_seconds_count 1000
  ```

### 6.2 HTTP 管理接口

在单独端口（如 9093）启动 HTTP Server：

| Method | Path | 功能 |
|--------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/api/topics` | 查询所有 Topic |
| POST | `/api/topics` | 创建 Topic |
| GET | `/api/topics/{topic}` | Topic 详情 |
| GET | `/api/groups` | 所有 Consumer Group |
| GET | `/api/groups/{group}/lag` | 消费积压 |

可以使用简单的 HTTP 库（如 `cpp-httplib`）或手写 HTTP 解析（面试加分）。

### 6.3 性能优化专项

#### 优化方向

| 优化 | 当前问题 | 方案 | 预期提升 |
|------|---------|------|----------|
| **减少协议拷贝** | Decode 多次复制 payload | `std::span` + `writev` | 10-20% |
| **Batch 聚合** | Client 单条发送 | Producer 端 linger.ms | 2-3x |
| **零拷贝 Fetch** | 大消息 CPU 拷贝高 | `sendfile` | 20-30% |
| **TopicManager 锁** | 读 metadata 竞争 | `std::shared_mutex` ✅ 已完成 | - |
| **内存池** | 协议解析频繁分配 | `std::pmr::monotonic_buffer_resource` | 5-10% |

#### 优化前后对比报告

必须输出以下表格：

```markdown
## 优化: Batch Aggregation

### Before
| Producers | Batch Size | QPS | P99 |
|-----------|------------|-----|-----|
| 4         | 1          | 77K | 0.10ms |

### After
| Producers | Batch Size | QPS | P99 |
|-----------|------------|-----|-----|
| 4         | 10         | 279K | 0.21ms |

### Conclusion
Batch=10 提升吞吐 3.6x，P99 略增但仍在 1ms 内，适合高吞吐场景。
```

### 6.4 验收标准
- [ ] Prometheus 指标可被抓取
- [ ] HTTP 接口可通过 `curl` 查询
- [ ] 至少 3 个优化有对照组数据
- [ ] 形成 `docs/benchmark_report_phase6.md`

---

## 七、Phase 7: 测试与文档（1-2 周）

### 目标
把项目从"能跑"打磨成"能投简历、能现场演示"。

### 7.1 故障测试矩阵

| 测试场景 | 操作 | 预期结果 | 实际结果 |
|---------|------|---------|---------|
| **Broker 崩溃恢复** | 写入中 `kill -9` broker，重启 | 截断半条消息，offset 连续 | ✅ / ❌ |
| **磁盘满** | 模拟 `ENOSPC` | Produce 返回错误，不崩溃 | ✅ / ❌ |
| **Consumer 掉线** | Kill consumer 进程 | 其他 consumer 接管 partition | ✅ / ❌ |
| **网络断连** | `iptables` 断开连接 | Client 重连，不丢消息 | ✅ / ❌ |
| **Index 损坏** | 删除 `.index` 文件 | 启动重建索引 | ✅ / ❌ |
| **Rebalance 风暴** | 10 个 consumer 反复加入/离开 | 无 panic，无双消费 | ✅ / ❌ |

实现方式：
- 使用 GoogleTest 的 `TEST(FaultTest, BrokerCrashRecovery)`
- 集成测试启动真实 Broker 进程，用信号 kill
- 使用 `mock` 或 `dependency injection` 模拟磁盘错误

### 7.2 长时间 Soak Test

运行 4 小时：
- 4 个 Producer 持续写入
- 8 个 Consumer 持续消费
- 随机 Consumer 断开/重连
- 记录：
  - 总消息数
  - 丢失/重复消息数
  - P99 延迟趋势
  - 内存/CPU 使用率

### 7.3 文档补齐

#### 必须完成的文档

1. **README.md** 完整版:
   - 项目介绍、架构图
   - Quick Start (一键启动 demo)
   - Benchmark 数据
   - 设计亮点

2. **docs/dev-notes/**:
   - `05_phase4_week7_consumer_group.md` ← **当前为空，必须补充**
   - `06_retry_dlq_backpressure.md`
   - `07_benchmark_optimization_report.md`

3. **docs/interview/**:
   - `20_common_interview_questions.md`:
     ```markdown
     ### Q1: CommitLog 并发写如何保证 offset 单调递增？
     A: 单 Partition 内使用 mutex 串行化 append，多 Partition 并行。
        后续可改为单写线程 + append queue 提升性能。
     
     ### Q2: 为什么用 Pull 而不是 Push？
     A: Pull 模型让 Consumer 自己控制消费速率，天然背压。
        Push 需要 Broker 维护每个 Consumer 的状态和速率控制。
     ```

4. **docs/architecture/**:
   - 补充 `分布式设计方案.md`（即使未实现，也要写清楚设计思路）

### 7.4 简历与面试材料

#### 简历版本（Phase 1-7 完成后）

```
LogMQ | C++20 / Linux epoll / CommitLog / Reactor

从零实现基于 CommitLog 的高性能持久化消息队列，支持 Topic/Partition、
批量写入、Pull 消费、Consumer Group、Retry/DLQ 和 Metrics。

• 基于 append-only CommitLog 实现持久化存储，设计 Segment 滚动、
  稀疏 OffsetIndex、CRC 校验和崩溃恢复，支持半条消息截断与索引重建
• 基于 Linux epoll 实现多 Reactor 网络层和自定义二进制协议，处理
  半包粘包、请求关联、连接超时、慢客户端和背压控制
• 实现 Topic/Partition、Batch Produce、Pull Fetch、Offset Commit
  和 Consumer Group，基于 generation id 处理 Rebalance 期间的一致性
• 引入 Retry Topic、DLQ、异步刷盘和 Prometheus Metrics，支持失败
  消息治理、消费积压观测和存储错误暴露
• 编写 GoogleTest、崩溃恢复测试和 benchmark，对比 batch size、
  partition 数、flush 策略对 QPS、P99 和吞吐的影响，单机达到 27.9 万 msg/s
```

#### 面试高频问题准备表

| 问题 | 关键词 | 结合项目回答 |
|------|-------|-------------|
| **CommitLog 并发写如何保证 offset 有序？** | mutex、单写线程、append queue | 见 Q1 |
| **Rebalance 期间 Fetch 会发生什么？** | generation id、状态机 | 见 Q2 |
| **fsync 失败怎么处理？** | 同步/异步、partition error | 见 Q3 |
| **半条消息如何恢复？** | 扫描、CRC、截断 | 见 Q4 |
| **为什么用 Pull 不用 Push？** | 背压、状态、批量 | 见 Q5 |
| **如何防止内存被打爆？** | 多层背压、限流 | 见 Q6 |
| **怎么优化吞吐？** | Batch、多 Partition、writev | 见 Q7 |

---

## 八、Phase 8: Controller 与元数据管理（2-3 周）

### 目标
引入控制面，为分布式扩展打好基础。

### 8.1 总体架构

```
┌─────────────────────────────────────────────────┐
│                   Controller                    │
│  • Broker 注册与心跳                            │
│  • Topic/Partition 元数据                       │
│  • Leader 选举与分配                            │
└─────────────┬───────────────────────────────────┘
              │ Control Plane RPC
      ┌───────┼───────┬───────┐
      │       │       │       │
  ┌───▼───┐ ┌─▼─────┐ ┌─────▼┐
  │Broker1│ │Broker2│ │Broker3│
  └───┬───┘ └─┬─────┘ └─────┬┘
      │       │             │
      └───────┴─────────────┘
         Data Plane TCP
              │
         ┌────▼────┐
         │ Clients │
         └─────────┘
```

### 8.2 核心任务

#### 8.2.1 Broker 增加 BrokerId

在 `config/logmq.yaml` 增加：
```yaml
broker:
  id: 1
  advertised_host: "192.168.1.101"
  advertised_port: 9092
  controller_host: "192.168.1.100"
  controller_port: 9090
```

#### 8.2.2 控制面协议

| ApiKey | 请求 | 说明 |
|--------|------|------|
| 100 | `RegisterBrokerRequest` | Broker 启动时注册 |
| 101 | `HeartbeatRequest` | Broker 定期心跳 |
| 102 | `CreateTopicRequest` | 改为经 Controller 处理 |
| 103 | `DescribeClusterRequest` | 查询集群状态 |
| 104 | `MetadataUpdateRequest` | Controller 推送元数据更新 |

#### 8.2.3 Controller 实现

```cpp
class Controller {
public:
    Status Start();
    
    // 处理 Broker 注册
    Result<RegisterBrokerResponse> RegisterBroker(const RegisterBrokerRequest& req);
    
    // 处理心跳
    Result<HeartbeatResponse> Heartbeat(const HeartbeatRequest& req);
    
    // 创建 Topic（分配 Partition 到 Broker）
    Result<CreateTopicResponse> CreateTopic(const CreateTopicRequest& req);
    
private:
    struct BrokerMetadata {
        BrokerId id;
        std::string host;
        uint16_t port;
        std::chrono::steady_clock::time_point last_heartbeat;
        bool alive{true};
    };
    
    struct PartitionMetadata {
        BrokerId leader;
        std::vector<BrokerId> replicas;
        int32_t leader_epoch{0};
    };
    
    std::unordered_map<BrokerId, BrokerMetadata> brokers_;
    std::unordered_map<TopicName, std::vector<PartitionMetadata>> topics_;
    mutable std::shared_mutex mutex_;
    std::thread heartbeat_checker_;
};
```

#### 8.2.4 Broker 注册流程

```
Broker 启动
    ↓
连接 Controller
    ↓
发送 RegisterBrokerRequest(id, host, port)
    ↓
Controller 记录 broker 元数据
    ↓
返回 RegisterBrokerResponse(assigned_partitions)
    ↓
Broker 启动定期心跳线程 (每 3 秒)
```

### 8.3 验收标准
- [ ] 启动 1 个 Controller + 3 个 Broker
- [ ] Broker 能注册成功并显示在 `/api/cluster`
- [ ] 心跳超时后 Controller 标记 Broker 为 `offline`
- [ ] CreateTopic 通过 Controller 分配 Partition 到不同 Broker

---

## 九、Phase 9: Leader/Follower 复制（2-3 周）

### 目标
实现核心分布式能力：多副本、复制、High Watermark。

### 9.1 复制模型

```
Producer
    ↓
Leader Broker
    │ append local CommitLog
    ├────► Follower-1: ReplicaFetchRequest
    │           ↓ append local CommitLog
    │           ↓ return match_offset
    └────► Follower-2: ReplicaFetchRequest
                ↓ append local CommitLog
                ↓ return match_offset
    ↓
Leader 收集 match_offset → 推进 high_watermark
    ↓
Consumer Fetch ≤ high_watermark
```

### 9.2 核心概念

#### 9.2.1 ISR (In-Sync Replicas)
- Leader 维护一个 ISR 集合
- ISR 内的 Follower 延迟不超过 `replica.lag.time.max.ms` (如 10 秒)
- 只有 ISR 内副本 ACK 后才认为消息已提交
- `ack=all` 时需要等待 ISR 全部复制成功

#### 9.2.2 High Watermark
- `high_watermark = min(follower_match_offset for follower in ISR)`
- Consumer 只能看到 `offset < high_watermark` 的消息
- Leader 故障后，新 Leader 的 high_watermark 保证不丢数据

#### 9.2.3 Leader Epoch
- 每次 Leader 切换，epoch++
- 防止 follower 日志比新 leader 更新（脑裂场景）
- Follower 启动时先获取 Leader Epoch，如果本地 epoch 更高则截断

### 9.3 实现要点

#### 9.3.1 复制协议

```cpp
struct ReplicaFetchRequest {
    TopicName topic;
    PartitionId partition_id;
    Offset follower_offset;
    uint32_t max_bytes;
    int32_t follower_epoch;
};

struct ReplicaFetchResponse {
    Offset leader_end_offset;
    Offset high_watermark;
    int32_t leader_epoch;
    std::vector<Record> records;
};
```

#### 9.3.2 Follower 拉取线程

```cpp
class ReplicationManager {
public:
    void StartFollowing(TopicName topic, PartitionId partition_id, BrokerId leader);
    
private:
    void ReplicaFetchLoop(FollowerState& state);
};
```

#### 9.3.3 Leader 推进 High Watermark

```cpp
void UpdateHighWatermark(const PartitionMetadata& partition) {
    Offset min_match = partition.leader_end_offset;
    for (auto& follower : partition.isr) {
        min_match = std::min(min_match, follower.match_offset);
    }
    partition.high_watermark = min_match;
}
```

### 9.4 验收标准
- [ ] 1 Leader + 2 Follower 写入数据后，Follower 能复制
- [ ] Kill Follower 后 Leader 仍可 ack=1 写入
- [ ] Kill Leader 后 Controller 选新 Leader
- [ ] 新 Leader 的 high_watermark 不暴露未提交数据

---

## 十、Phase 10: Leader Failover（2-3 周）

### 目标
实现完整的故障切换和集群自愈能力。

### 10.1 故障检测

```
Controller 心跳检测线程 (每秒扫描)
    ↓
发现 Broker 心跳超时 (>10 秒)
    ↓
标记 Broker 为 offline
    ↓
触发 Leader Election
```

### 10.2 Leader 选举

#### 选举策略
1. 从 ISR 中选第一个存活副本
2. 如果 ISR 全部离线，考虑 `unclean.leader.election.enable`:
   - `true`: 从 OSR (Out-of-Sync Replicas) 选，可能丢数据
   - `false`: 等待 ISR 恢复

#### 选举流程
```
Controller
    ↓
选出新 Leader (replica-2)
    ↓
增加 leader_epoch
    ↓
发送 MetadataUpdateRequest 给所有 Broker
    ↓
Broker 更新本地 partition 角色
    ↓
Follower 重新向新 Leader 拉取
    ↓
Client 刷新 metadata 后路由到新 Leader
```

### 10.3 日志截断

**问题**: Follower 的日志可能比新 Leader 更长（旧 Leader 写入后立即崩溃）

**解决**:
```
Follower 启动 ReplicaFetch
    ↓
发送 LeaderEpochRequest(my_epoch, my_end_offset)
    ↓
Leader 返回 my_epoch 对应的 end_offset
    ↓
Follower 截断 > end_offset 的日志
    ↓
从 end_offset 继续拉取
```

### 10.4 验收标准
- [ ] Kill Leader 后 30 秒内选出新 Leader
- [ ] Client 自动刷新 metadata 后路由到新 Leader
- [ ] 新 Leader 不暴露旧 Leader 未提交数据
- [ ] Follower 日志截断正确

---

## 十一、里程碑与简历建议

### 11.1 可投简历版本

**完成 Phase 1-7 后即可投简历**，此时项目完成度：
- ✅ 存储层完整
- ✅ 网络层完整
- ✅ 消费语义完整
- ✅ 可靠性增强 (Retry/DLQ)
- ✅ 观测性 (Metrics)
- ✅ 性能数据 (Benchmark)

简历可写：
> 单机高性能消息队列，支持 Topic/Partition、批量写入、Consumer Group、
> Retry/DLQ 和 Prometheus Metrics，单机吞吐 27.9 万 msg/s

### 11.2 强竞争力版本

**完成 Phase 1-10 后**，项目完成度：
- ✅ 上述所有 +
- ✅ 分布式复制
- ✅ Leader Failover
- ✅ Controller 元数据管理

简历可写：
> 分布式持久化消息队列，支持 Leader/Follower 复制、ISR、High Watermark、
> Controller 元数据管理和故障自动切换

### 11.3 面试加分点

| 加分项 | 实现难度 | 面试价值 |
|-------|---------|---------|
| **Leader Epoch 日志截断** | 高 | ⭐⭐⭐⭐⭐ 展示对分布式一致性的理解 |
| **RocksDB StateStore** | 中 | ⭐⭐⭐⭐ 展示状态持久化设计 |
| **零拷贝 sendfile** | 中 | ⭐⭐⭐⭐ 展示性能优化能力 |
| **时间轮 Retry 调度** | 中 | ⭐⭐⭐ 展示数据结构应用 |
| **Docker Compose 集群** | 低 | ⭐⭐⭐ 便于演示 |

---

## 十二、风险与注意事项

### 12.1 时间风险
- **单机版本 (Phase 5-7)**: 2-4 周
- **分布式版本 (Phase 8-10)**: 6-9 周
- **总计**: 8-13 周

如果时间紧张，**优先完成单机版本**，分布式部分可以：
1. 只写设计文档，不实现
2. 实现最小原型（1 Leader + 1 Follower，无 Failover）
3. 面试时讲清楚设计思路和权衡

### 12.2 复杂度风险
- **Controller 单点**: 第一版可以接受，面试时解释"生产环境需要 Raft/etcd"
- **脑裂**: Leader Epoch 可以避免大部分问题，但极端场景仍可能出现
- **数据丢失**: `ack=all` + ISR 机制保证不丢，但实现错误可能引入 bug

### 12.3 工程风险
- **测试覆盖**: 分布式系统 bug 难复现，必须有自动化测试
- **文档滞后**: 代码改了文档没更新 → 面试时说不清
- **过度优化**: 优化要有数据支撑，不要盲目追求极限性能

---

## 十三、总结

### 当前状态
LogMQ 已经是一个功能完整、性能优秀的**单机消息队列**，存储层、网络层、
消费语义层实现质量高，具备投简历的基础。

### 下一步行动
1. **立即行动**: 补充 `05_phase4_week7_consumer_group.md` 开发笔记
2. **1-2 周内**: 完成 Phase 5 (Retry/DLQ/背压)
3. **2-4 周内**: 完成 Phase 6-7 (Metrics/测试/文档) → **可投简历**
4. **可选**: Phase 8-10 分布式扩展 → **强竞争力版本**

### 面试叙事角度
不要只说"实现了 XX 功能"，要围绕问题讲：
- **为什么快**: 顺序写、稀疏索引、批量、异步刷盘
- **为什么可靠**: CRC、崩溃恢复、ISR、Leader Epoch
- **为什么能扩展**: Controller、Leader/Follower、High Watermark
- **怎么权衡**: ack=1 vs ack=all、同步 vs 异步、Pull vs Push

### 最终建议
**先把单机做深，再考虑分布式。** 一个稳定、文档完整、有数据的单机强版本，
比一个半成品分布式更有说服力。祝你面试顺利！🚀
