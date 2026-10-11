# Phase 5.1：Retry Topic 与 DLQ 设计

## 目标与范围

实现 `docs/PROJECT_ANALYSIS_AND_ROADMAP.md` 的 Phase 5.1。Consumer 处理失败后调用
`Nack(record, reason)`，Broker 将原消息复制到 `%RETRY%{topic}`，5 秒后重新可见。
最多允许 3 次重试；初次处理失败产生 retry_count=1，第三次重试仍失败时产生
retry_count=4 的 DLQ 消息。`%DLQ%{topic}` 不参与自动调度。

只实现 5.1，不扩展背压、连接超时、分布式复制或管理 HTTP 接口。
使用 C++20、现有 CommitLog/TopicManager、标准库优先队列及现有 GoogleTest，不增加依赖。
保留工作区已有未提交改动，尤其 Client、网络层及网络集成测试的改动。

## 现有实现约束

- 存储 Record 仅包含 timestamp/key/value/CRC，RecordBatch 无格式版本字段。
- CommitLog::Read 返回包含请求 offset 的完整 batch，Nack 必须取 batch 内的准确记录。
- FetchResponse 用 base_offset 和 records 的下标推导消息 offset，不能过滤中间记录后压紧数组。
- Consumer 当前只持有原 Topic 的分区位置；CommitSync 提交 Poll 已推进的位置。
- TopicManager 当前拒绝 `%` 字符，需要只为合法内部前缀放宽校验。
- 协议版本为 1；新增重试元数据不能无提示地改变旧 Fetch 的编码。

## 消息表示与协议

增加 RetryMetadata：retry_count、next_visible_time（Unix 毫秒）、original_topic、failure_reason。
另外保留 original_partition 和 original_offset，便于追溯首次失败记录。
重试次数由 Broker 读取源记录决定，客户端不得直接指定或重置计数。

新增 ApiKey::kNack=11。NackRequest 包含实际存储 topic、partition_id、offset、reason。
NackResponse 返回目标 topic、partition_id、offset、重试元数据及是否进入 DLQ。
Consumer::Nack(const ConsumerRecord&, std::string reason) 返回 Result<NackResponse>。
ConsumerRecord 在保留现有字段的同时增加可选 RetryMetadata；topic/partition_id/offset
始终指向当前存储位置，重试记录的 topic 因而为 `%RETRY%{original_topic}`。

保持版本 1 的现有消息布局。增加显式版本 2，供携带重试元数据的 Fetch 和 Nack 使用；
两端编解码器支持版本 1、2，版本 2 的 Fetch Record 增加可选元数据字段。
SDK 普通 Fetch 使用版本 1，内部 Topic Fetch 和 Nack 使用版本 2。
版本 1 Fetch 内部 Topic 仍返回原始 key/value，
但没有重试元数据；Nack 计数仍由 Broker 依据持久化记录计算。
非法版本、截断字段、非法 offset、错误 body 类型均返回已有错误模型。

在内部 Topic 的存储 Record.value 中保存带魔数、版本和长度的二进制 envelope，
包含原始 value 与重试元数据；key、timestamp 保持原值。编码器只用于内部 Topic，
普通 Topic 的 value 即使包含相同字节也不解析为重试消息。Fetch 解包后返回原始内容。
不修改普通 Record/RecordBatch 的磁盘格式；内部 envelope 仍受已有 CRC 保护。
编码长度、包版本及元数据必须校验，损坏 envelope 返回错误。

## Nack 与内部 Topic

1. 校验源 topic/partition/offset，读取准确源记录。拒绝对 DLQ 再次 Nack。
2. 普通消息从 retry_count=0 开始；Retry 源记录必须解码其已有元数据。
3. retry_count 加一。计数 1..3 写入 Retry，next_visible_time=now+5000ms；
   计数 4 写入 DLQ，next_visible_time=0。
4. 按需创建内部 Topic，分区数和原 Topic 一致，保留源 partition_id；
   创建过程复用 TopicManager 的幂等创建和元数据持久化。
5. 每次写入一个单记录 batch，遵守 Broker 既有 sync/async 刷盘策略。
6. Retry 写入成功后登记调度任务，DLQ 不登记。失败返回错误，不自动推进消费位置。

只允许 `%RETRY%`、`%DLQ%` 前缀后接合法普通 Topic 名，不允许嵌套前缀。
Broker 拒绝普通 Produce/CreateTopic 使用内部名称；内部创建只能由 Nack 流程执行。
Metadata、Fetch、Offset Commit 可访问已存在的内部 Topic，便于消费和检查 DLQ。

## 延迟可见与调度器

新增 RetryScheduler，由 BrokerService 管理生命周期。使用
`std::priority_queue<RetryTask, ..., CompareByVisibleTime>`、mutex、condition_variable 和后台线程。
任务定位内部 Topic/partition/offset，并保存截止时间；不在内存队列复制消息 payload。

每个 Retry 分区维护连续可见的 end offset。只有到期的连续前缀可见，Fetch 返回该
可见 end offset 作为 high_watermark。已有但尚未到期的 offset 返回成功空结果，
base_offset 保持请求位置，Consumer 不推进游标。过了真实 log end 才返回越界错误。
禁止跳过未到期记录，否则现有连续 offset 语义会让 Consumer 永久漏读。

启动时扫描内部 Retry 日志、校验 envelope，以持久化截止时间重建可见前缀与待调度任务；
到期的历史记录继续可读，不重新追加到原 Topic。恢复读取使用协议支持的最大消息上限，
而非固定 4096 字节。损坏数据或 IO 失败必须使启动失败，不能静默跳过。
内部 Topic 的 CommitLog 使用严格恢复：仅允许截断活动分段末尾未写完的 header/payload；
完整 batch 校验失败或封存分段残缺会报错并保留文件，避免在 envelope 校验前删除消息。
系统时钟回拨可能延迟可见时间；同分区前序消息未到期会阻塞后序消息，保留有序消费。

追加与登记调度任务必须串行发布，防止并发 Nack 产生未登记空洞。
队列只保留尚未可见任务，可见后释放任务。关闭时通知并 join 调度线程，再关闭 TopicManager。
任务只推进内存可见前缀，不改写 CommitLog，也不创建重复消息；重启不会丢失重试次数。

## Consumer 集成

Subscribe 原 Topic 后，Consumer 沿用该 Topic 的 Consumer Group 分配；每个已分配分区
同时维护原 Topic 与对应 Retry Topic 的 next_offset。内部 Topic 尚不存在时视为没有重试消息。
Poll 在每轮拉取原消息及到期 Retry 消息，使用各自的真实存储位置构造 ConsumerRecord。
Rebalance 后分别恢复两类已提交位置，CommitSync 分别提交两类已存在 Topic 的位置。
消费 DLQ 时不自动派生 Retry Topic，也不自动 Nack；管理员可显式订阅或 Fetch 检查。

Nack 成功表示失败消息已转存；Nack 不自动 CommitSync，应用仍须遵守先处理或成功 Nack、
再提交消费进度的现有契约。由于 Poll 以批次推进位置，批次中有尚未处理或 Nack 失败的消息时
不能提交整个批次；应用可以重试 Nack 后处理完批次，或停止消费并恢复已提交位点。
使用 Seek 时必须回退全部尚未处理的 Topic/分区，不能只回退一个失败记录后提交其他分区。

本阶段遵循路线图的 Topic 级重试语义：不同 Consumer Group 各自读 Retry Topic，
因此某组产生的重试消息对其他组也可见。按失败组隔离的定向重试不属于本次范围。
Nack 的请求超时或应用重复调用可能写出重复重试记录，保持 at-least-once，不承诺幂等或 exactly-once。
异步刷盘的断电丢失窗口与现有 Produce 一致，不额外承诺已 fsync。

## 文件职责

- `protocol/types.h`、`protocol/codec.cpp`：Nack 与版本 2 Fetch 元数据编码。
- 新增 `broker/retry_scheduler.h/.cpp`：内部 envelope 编解码、任务恢复与延迟可见前缀。
- `broker/broker_service.h/.cpp`：Nack 路由、内部 Topic 写入、Fetch 可见边界及线程生命周期。
- `broker/topic_manager.cpp`：合法内部名称校验。
- `storage/commit_log.h/.cpp`：内部 Topic 严格恢复，区分损坏完整记录与活动分段残缺尾部。
- `broker/offset_store.cpp`、`broker/group_coordinator.cpp`：提交、恢复及订阅时支持合法内部 Topic 名称。
- `client/client.h/.cpp`：Nack API、双游标 Poll、Commit、Rebalance 恢复。
- `CMakeLists.txt`：纳入新实现文件；现有 unit/integration 测试目标承载新增用例。
- 路线图及开发笔记：记录实际完成状态、使用示例、消费提交契约和上述边界。

实施细化：内部 envelope 复用版本 2 Fetch 编解码，在其前追加魔数和 envelope 版本；
内部 Fetch 的 max_bytes 按原始消息的 RecordBatch 大小计算，内部封装及元数据开销不计入此预算，
响应仍受 kMaxFrameBytes 上限约束。切换订阅 Topic 时先停止旧心跳并 LeaveGroup，再重新加入。

## 验证与完成标准

1. 协议：Nack 请求、成功/错误响应及版本 2 Fetch 往返；版本 1 的原有测试仍通过。
2. Broker：Nack 指定 batch 中间记录，只复制该记录；原 key/value/timestamp、失败原因及来源可追溯。
3. 延迟：默认 5 秒内 Retry Fetch 为空；到期后可读且消费位置没有跳过消息。
4. 上限：三轮 Retry 的计数分别为 1、2、3；第 4 次 Nack 进入 DLQ，之后不再自动调度。
5. 恢复：关闭并重启 Broker，未到期消息仍被延迟，到期后可读，重试计数继续累加。
6. 边界：非法 offset/partition、对 DLQ Nack、内部 Produce、损坏 envelope 返回明确错误；
   大于 4KB 的重试记录可恢复；并发 Nack、较早未到期记录不会造成游标漏读。
7. 网络：真实 TCP Server + SDK，从 Subscribe/Poll/Nack 到自动重拉、CommitSync、Rebalance/重启恢复。
8. 运行完整 CMake 构建和 CTest 测试，并在路线图勾选四项验收标准后才宣告完成。

计时测试使用 deadline 轮询及宽裕超时，不依赖恰好 sleep 5000ms；至少一个集成测试覆盖默认 5 秒。
单元测试可通过 BrokerServiceOptions 的 RetryOptions 缩短延迟，默认仍为 5000ms、max_retries=3。
