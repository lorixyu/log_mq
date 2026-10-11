# LogMQ Performance Report — 2026-08-05

## Environment

- Machine: Apple M4, 10 CPU cores, 16 GiB memory
- OS: macOS 15.7.2, arm64
- Compiler: AppleClang 17
- Build: `Release` (`-O3 -DNDEBUG`)
- Broker: loopback TCP, 4 IO threads, 8 worker threads
- Message payload: 256-byte value; payload throughput excludes key, record, batch, and protocol headers
- Network benchmark: one persistent TCP connection per producer/consumer; one synchronous request-response in flight per connection
- Async policy: the Broker acknowledges append before the periodic `fsync`; numbers are acknowledged throughput, not per-message durable throughput

The benchmark executable is `build-bench/logmq_benchmark`. It tests the storage layer directly or starts an in-process Broker and drives the binary TCP protocol over `127.0.0.1`.

## Stable End-to-End Results

The following results are medians of three runs.

| Configuration | Produce records/s | Produce P99 | Fetch records/s | Fetch P99 |
|---|---:|---:|---:|---:|
| Async, 4 producers, 4 partitions, batch=1 | 77,834 | 0.101 ms | 44,051 | 0.244 ms |
| Async, 4 producers, 4 partitions, batch=10 | 278,622 | 0.213 ms | 191,561 | 1.217 ms |

One record is one message. Batch latency is per request, not per individual message.

## Producer Concurrency

Async append, one partition, batch=1, 200,000 records:

| Producers | Produce records/s | P50 | P99 |
|---:|---:|---:|---:|
| 1 | 28,114 | 0.035 ms | 0.044 ms |
| 2 | 51,181 | 0.038 ms | 0.050 ms |
| 4 | 60,547 | 0.065 ms | 0.108 ms |
| 8 | 59,500 | 0.132 ms | 0.309 ms |

Throughput peaks near four producers. Eight producers add lock contention without increasing throughput because appends to one partition are serialized by the `CommitLog` mutex.

## Partition Parallelism

Async append, batch=1:

| Producers / Partitions | Produce records/s | Produce P99 | Fetch records/s | Fetch P99 |
|---|---:|---:|---:|---:|
| 4 / 1 | 60,547 | 0.108 ms | 24,735 | 0.539 ms |
| 4 / 4 | 77,834 | 0.101 ms | 44,051 | 0.244 ms |
| 8 / 8 | 78,855 | 0.195 ms | 52,225 | 0.347 ms |

Multiple partitions reduce the single `CommitLog` lock bottleneck, but performance plateaus from four to eight partitions. The storage-vs-network comparison indicates that protocol encoding/decoding, worker dispatch, event loops, and synchronous client round trips are the next major optimization area.

## Batch Effect

Async, 4 producers, 4 partitions:

| Batch size | Produce records/s | Payload MiB/s | Request P99 | Fetch records/s | Fetch P99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 77,834 | 19.0 | 0.101 ms | 44,051 | 0.244 ms |
| 10 | 278,622 | 68.0 | 0.213 ms | 191,561 | 1.217 ms |
| 100 | 380,343 | 92.9 | 1.614 ms | 304,919 | 10.968 ms |

Batching amortizes network, codec, dispatch, CRC, and append overhead. Batch=100 gives the highest throughput, but its larger request and response work increases tail latency. Batch=10 is the more balanced point for a resume.

## Flush Policy

| Configuration | Produce records/s | Produce P99 |
|---|---:|---:|
| Sync, 1 producer, 1 partition, batch=1 | 8,128 | 0.300 ms |
| Sync, 4 producers, 4 partitions, batch=10 | 143,955 | 0.782 ms |
| Async, 4 producers, 4 partitions, batch=10 | 278,622 | 0.213 ms |

`sync` calls `fsync` before acknowledging every Produce request. Batching and partition parallelism partially hide this cost, but async acknowledgement remains about 1.94x faster at batch=10.

## Storage vs Network

Direct storage benchmark, async append, 4 producers, 4 partitions, batch=1:

- Produce: 255,372 records/s, P99 0.059 ms
- Fetch: 96,705 records/s, P99 0.109 ms

The corresponding end-to-end Broker median is 77,834 Produce records/s. The approximately 3.3x gap shows that the current bottleneck is above the append-only storage path.

## Short-Connection Finding

At the time of this report, the public SDK created a TCP connection for every request. A short-connection run succeeded at 500 sequential records but repeatedly failed by 2,000 records with `Connection reset by peer`.

One suspected cause in the original run was fd reuse in `TcpServer`: rapid close/reconnect churn could let a reused fd collide with an older connection entry. This was not proven with tracing or a minimal reproducer. Persistent connections avoid that path and reduce short-connection churn.

As of 2026-09-26, `ClientConnection` reuses a TCP connection and reconnects on the next request after an I/O failure. The Broker now removes closed connections from its fd map before closing the fd, synchronizes connection-count reads, and drains pending connection work before stopping IO loops. The historical failure has not been isolated to a single cause; repeated short-connection stress can also exhaust local ephemeral ports. Do not quote the persistent-connection benchmark as SDK throughput until the SDK itself is rebenchmarked.

## Resume-Safe Statement

> 在 Apple M4 本机回环环境、256B 消息、4 Producer/4 Partition、异步刷盘下，持久连接端到端生产吞吐中位数约 7.8 万 messages/s，P99 约 0.10ms；Batch=10 时达到约 27.9 万 messages/s。对比单 Partition 发现 8 Producer 下锁竞争导致吞吐不再增长，并通过多 Partition 将单消息吞吐提升约 29%。

Always retain the environment, payload size, batch size, partition count, connection model, and flush semantics when quoting these numbers.

## Reproduction

```bash
cmake -S . -B build-bench \
  -DCMAKE_BUILD_TYPE=Release \
  -DLOGMQ_BUILD_TESTS=OFF
cmake --build build-bench --target logmq_benchmark -j4

./build-bench/logmq_benchmark \
  --layer network \
  --connection-mode persistent \
  --flush-policy async \
  --records 200000 \
  --producers 4 \
  --consumers 4 \
  --partitions 4 \
  --batch-size 1 \
  --value-bytes 256 \
  --warmup-batches 1000
```

Raw one-row CSV files are stored beside this report.
