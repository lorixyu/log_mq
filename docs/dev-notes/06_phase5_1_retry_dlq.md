# Phase 5.1：Retry Topic 与 DLQ

完成时间：2026-10-10。对应 `PROJECT_ANALYSIS_AND_ROADMAP.md` 的 5.1。

## 流程与默认值

`Consumer::Nack(record, reason)` 发送 ApiKey=11 的请求，Broker 按真实 topic、partition、offset
读取原记录，复制至 `%RETRY%{original_topic}`。每次复制单条消息，保留 key/value/timestamp、
原始 Topic/partition/offset 与最新失败原因。默认延迟 5000ms，允许 3 次重试。

| 失败阶段 | retry_count | 目标 | 可见时间 |
|---|---:|---|---|
| 原消息处理失败 | 1 | `%RETRY%orders` | 当前时间 + 5 秒 |
| 第 1 次重试失败 | 2 | `%RETRY%orders` | 当前时间 + 5 秒 |
| 第 2 次重试失败 | 3 | `%RETRY%orders` | 当前时间 + 5 秒 |
| 第 3 次重试失败 | 4 | `%DLQ%orders` | 立即可见，不再调度 |

内部 Topic 按需创建，分区数与原 Topic 一致，复制保留原分区。普通 Produce/CreateTopic
不能使用内部名称；Metadata/Fetch/Offset Commit 可访问已创建的内部 Topic。
对 DLQ 调用 Nack 返回错误。

## 存储与调度

现有 Record/RecordBatch 磁盘格式保持不变。仅内部 Topic 的 value 使用带魔数和版本的 envelope，
复用版本 2 Fetch 编码保存原消息和 RetryMetadata；Fetch 解包后返回原内容。
普通消息即使含相同 magic 也不做重试解码。

RetryScheduler 使用 priority_queue、mutex、condition_variable 和后台线程。
任务保存位置及截止时间，不复制 payload；到期后推进该分区的连续可见 end offset。
尚未到期时 Fetch 返回成功空结果，Consumer 不推进游标。
若后一条先到期，仍等待前一条，避免连续 offset 消费漏读。

启动时扫描内部日志，校验 envelope、原 Topic/分区关系及截止时间，重建调度状态。
已到期记录保持可见，未到期记录继续等待，重试次数不归零。
关闭时先唤醒并 join 调度器，再关闭日志。恢复支持大于 4KB 的消息；损坏 envelope 使启动失败。
内部日志恢复仅截断活动分段末尾未写完的 header/payload；完整记录 CRC 损坏、
封存分段损坏及 IO 失败会停止启动并保留该日志文件，避免静默丢弃 Retry/DLQ 数据。
内部 Fetch 的 max_bytes 按原消息的存储 batch 大小计费，排除内部封装开销；
实际响应仍受 16MiB 协议帧上限约束。

版本 1 的现有协议编码保持兼容，新增版本 2 的 Fetch Record 包含可选 RetryMetadata。
SDK 普通 Fetch 使用版本 1，内部 Topic Fetch 和 Nack 使用版本 2，
普通记录没有 retry 字段；旧版 Fetch 内部 Topic 返回原内容而不带元数据。

## SDK 用法与提交契约

```cpp
logmq::Consumer consumer({client_options, "orders-group"});
auto subscribed = consumer.Subscribe("orders");
if (!subscribed.ok()) return subscribed.status();

auto polled = consumer.Poll(std::chrono::seconds(1));
if (!polled.ok()) return polled.status();
for (const auto& record : polled.value()) {
    // process_message 由业务实现；失败记录的存储位置由 record 提供。
    auto processed = process_message(record);
    if (!processed.ok()) {
        auto nacked = consumer.Nack(record, std::string(processed.status().message()));
        if (!nacked.ok()) {
            // 停止本轮消费，不提交；新 Consumer 从已提交位点恢复整个批次。
            // 重试 Nack 成功后也可继续处理本批次剩余消息，再提交。
            return nacked.status();
        }
    }
}
return consumer.CommitSync();
```

Poll 会自动检查原 Topic 和对应 Retry Topic 的已分配分区；无需单独订阅 Retry。
`ConsumerRecord.topic/partition_id/offset` 始终指向当前存储位置，因此重试记录的 topic
是内部 Retry 名称，`record.retry` 提供原始来源和次数。
CommitSync 分别提交两类 Topic 的位置，Rebalance、重新订阅和新 Consumer 启动分别恢复这两类位置。
`Seek(partition, offset)` 回退当前订阅 Topic；新增 `Seek(topic, partition, offset)` 可回退对应 Retry。

**Nack 不隐式提交消费位点。** 现有 Poll 会推进整个返回批次的位置，只有全部消息处理成功或
成功 Nack 后才能 CommitSync。Nack 失败时须重试该调用并处理完批次，或停止消费并从已提交位点恢复。
若选择 Seek 回退，须回退所有尚未处理的 Topic/分区，不能只回退单个失败记录后提交其他分区。
订阅其他 Topic 时会先退出旧消费组，
如果该组还有其他成员订阅旧 Topic，则新订阅仍受到“一组一个 Topic”的现有限制。

DLQ 可以通过 AdminClient::Metadata、原始 Fetch，或另一个组的
`Consumer::Subscribe("%DLQ%orders")` 显式检查。DLQ 消费不会自动派生 Retry Topic。

## 语义边界

- Topic 级重试：不同消费组分别读取同一 Retry Topic，某组产生的重试对其他组也可见。
- at-least-once：重复 Nack 或响应丢失可能产生重复重试记录，没有幂等承诺。
- Nack 遵守原有 sync/async 刷盘策略；异步模式仍有现有断电丢失窗口。
- 使用 Unix 毫秒截止时间，时钟回拨会推迟可见；同分区保留有序前缀。
- Nack 追加与调度登记使用全局锁；若失败流量成为瓶颈，可再改为分区锁。
- 尚未实现 5.2 背压和 5.3 Idle Timeout；本阶段不包含自动 DLQ 回放或重试组隔离。

## 验证

2026-10-10 最终验证：完整构建无警告，CTest 82/82 通过。
独立审阅发现的内部日志损坏被截断、普通 Fetch 临界帧超限问题均先复现失败，再修复通过。

```sh
cmake --build build -j 4
ctest --test-dir build --output-on-failure
```

新增测试覆盖：Nack 与 v2 Fetch 协议往返/v1 兼容、batch 中间记录、延迟可见、三次重试后 DLQ、
非法请求/内部 Topic 保护、较大消息恢复、损坏 envelope/CRC、完整与残缺日志恢复、
16MiB 临界大小普通消息、并发 Nack、乱序截止时间、
内部位点持久化、原消息字节预算、真实 TCP 下默认 5 秒、SDK 双游标提交/重订阅/Seek/消费组重平衡。
网络测试需要允许绑定本机回环端口。
