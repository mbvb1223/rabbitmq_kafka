# Performance benchmarks: RabbitMQ vs Kafka

As of 2026-10-04. Demo pins **RabbitMQ 4.3 / Kafka 4.3**. Adds to [02-related-research.md](../02-related-research.md) §1, which covers the Confluent 2020 headline and the compare page's own numbers.

## TL;DR

- **There is no fair, current head-to-head.** No published run compares RabbitMQ 4.x and Kafka 4.x on the same hardware with the same durability. The two most-cited comparisons (Confluent and SoftwareMill, both 2020) test RabbitMQ 3.8.5 against Kafka 2.6, and each one mismatches durability in a different direction.
- **Compare like with like.** Quorum queues **fsync before confirming**. Streams **do not fsync**, and Kafka's default doesn't either. So the fair pairs are *quorum queue ↔ Kafka with `flush.messages=1`* and *stream ↔ Kafka default*. Most benchmarks instead put a queue against a log.
- **Orders of magnitude** (different hardware, so don't divide one by another):
  - One quorum queue: **~45–83 K msg/s** (single node).
  - A stream over AMQP (what PHP gets): **~60–110 K msg/s**.
  - A stream over its native protocol: **~280 K msg/s** on a laptop VM, up to **1–17 M msg/s** on servers with ~20–30-byte messages.
  - Kafka cluster: **~0.4–2 M rec/s** (msg/s).
- **Kafka 4.x has no faster data path.** The visible change is the `linger.ms` default moving from 0 to 5 in 4.0. That gives better p99.9 under high load and *higher* latency under low load. Share groups match consumer-group throughput only after tuning. With defaults, one run got 4.8 K msg/s against a 60 K target.
- **What you can honestly say on stage:** *"Kafka wins every published throughput chart. But every one of those charts compares a log to a queue, or fsync to no fsync. Log against log, they're in the same league. And for PHP workers doing 50 ms of work, the broker is never the bottleneck: 1,000 workers × 20 msg/s = 20 K msg/s, which is below what a single quorum queue does on a laptop."*

## Master table

Tier A = includes both brokers. B = RabbitMQ only. C = Kafka only. D = low credibility.

| # | Benchmark | By | Date | Versions | Durability settings | Throughput | p99 latency | Caveat | Link |
|---|---|---|---|---|---|---|---|---|---|
| A1 | Kafka vs Pulsar vs RabbitMQ (OMB) | Confluent (Nikhil, Chandar) | 2020-08-21 | Kafka 2.6, RMQ 3.8.5, Pulsar 2.6 | Kafka acks=all, min ISR 2, **no fsync**. RMQ **mirrored, persistence off, auto-ack** | Kafka 605 MB/s; Pulsar 305; RMQ 38 (CPU-bound >30 K msg/s); RMQ unreplicated ~100 K msg/s | Kafka 5 ms @200 MB/s; RMQ ~1 ms @30 MB/s | Vendor. Quorum queues deliberately excluded. Mirrored queues removed in 4.0 | [confluent.io](https://www.confluent.io/blog/kafka-fastest-messaging-system/) |
| A2 | mqperf 2020 edition | SoftwareMill (Adam Warski) | 2020-12-08 | Kafka 2.6.0, RMQ 3.8.5 | RMQ **quorum + confirms (fsync)**. Kafka acks=-1, min ISR 2, **no fsync** | Kafka 828,836 msg/s; RMQ 19,120 msg/s | **p95** processing: Kafka 49 ms, RMQ 142 ms | Independent, but durability is asymmetric (fsync vs none). Old versions | [softwaremill.com/mqperf](https://softwaremill.com/mqperf/) |
| A3 | Benchmarking Message Queues (Telecom, MDPI) | Maharjan, Chy, Arju, Cerny (Baylor) | 2023-06-13 | RMQ 3.11.10, Kafka 3.4.0, Redis 7.0.9, Artemis 2.28.0 | Not verified (full text returned 403) | Kafka "significantly outperformed"; RMQ 2nd | Kafka p50 within 3 ms of Redis | Academic, OMB fork. Setup and numbers unverified | [mdpi.com](https://www.mdpi.com/2673-4001/4/2/18) |
| B1 | AMQP 1.0 Benchmarks | RabbitMQ (David Ansari) | 2024-08-21 | 4.0.0-beta.5 vs 3.13.6 | Single node, durable msgs, QQ fsync, 12-byte payload | 4.0 AMQP 1.0: CQ 99,413; QQ 83,181; stream 112,196 msg/s. 0-9-1: CQ 88,534; QQ 70,067; stream 88,912 | CQ 2 ms; QQ 61 ms; stream 1,612 ms (all measured at saturation) | Vendor. One NUC, clients on the same box, tiny messages, no replication | [rabbitmq.com](https://www.rabbitmq.com/blog/2024/08/21/amqp-benchmarks) |
| B2 | 4.1 Performance Improvements | RabbitMQ (Michał Kuratczyk) | 2025-04-08 | 4.0 vs 4.1 | QQ, 20 KB msgs, 10 consumers, prefetch 300 | Both ~7,000 msg/s steady. Long queue: 4.0 publish drops to ~100 msg/s, 4.1 unaffected | n/a | Vendor. Hardware not disclosed | [rabbitmq.com](https://www.rabbitmq.com/blog/2025/04/08/4.1-performance-improvements) |
| B3 | Stream delivery optimization (read-ahead) | RabbitMQ (Arnaud Cogoluègnes) | 2025-09-26 | 4.1.4 vs 4.2.0-beta.3 | 3× m7i.4xlarge, consumer-only | 1-msg chunks, 12 B: 16,000 → 134,000 msg/s | n/a | Read path only, worst-case chunking | [rabbitmq.com](https://www.rabbitmq.com/blog/2025/09/26/stream-delivery-optimization) |
| B4 | Streams overview (Summit talk) | RabbitMQ (Arnaud Cogoluègnes) | 2021-07-14 | 3.9 RC | 3× c2-standard-16, native stream protocol, no fsync | 1 stream 1.14 M (4.14 M with sub-entry batching); 5 streams 4.93 M (16.97 M) msg/s | n/a | Source of the compare page's "4 M / 17 M". **~23-byte messages** (93.2 MB/s) | [speakerdeck](https://speakerdeck.com/acogoluegnes/rabbitmq-streams-overview-at-rabbitmq-summit-2021) |
| B5 | 3.12 Performance Improvements | RabbitMQ (Michał Kuratczyk) | 2023-05-17 | 3.11 vs 3.12 | GKE e2-standard-16, PerfTest, 12 B / 1 KB / 5 KB | QQ into a long queue: 3.11 ~25 K → ~10 K, 3.12 >20 K msg/s. QQ draining a 5 M backlog: >15 K vs <1 K. CQv2 +20–40% | CQv2 <50 ms on long queues | Vendor. Explains why pre-3.12 QQ numbers are stale | [rabbitmq.com](https://www.rabbitmq.com/blog/2023/05/17/rabbitmq-3.12-performance-improvements) |
| B6 | Cluster sizing: quorum queues | RabbitMQ (Jack Vanlightly) | 2020-06-21 | 3.8.x (inferred from date) | QQ RF 3, 1 KB, 100 queues, 200 pub / 200 con, confirms | Best 65 K msg/s (7× c5.4xlarge, io1). gp2 as good as io1 | n/a | Old. Shows QQ throughput scales with queue count | [rabbitmq.com](https://www.rabbitmq.com/blog/2020/06/21/cluster-sizing-case-study-quorum-queues-part-1) |
| B7 | Streams vs quorum queues, measured | Javier Cañete (jacar.es) | 2026-09-29 | **RMQ 4.3.6**, PerfTest 2.25.0, Stream PerfTest 1.8.0 | Single node, Docker 4 CPU / 4 GiB on Apple silicon, 1 KB. QQ fsync; stream no fsync | QQ 45,156; stream/AMQP 62,360 consumed (120,860 published); stream protocol 281,330 (517,072 with 16 GiB) msg/s | At 10 K msg/s: QQ 33.6 ms; stream/AMQP 5.39 ms; stream protocol 2 ms | One blogger, one node. **Closest match to our demo** | [jacar.es](https://jacar.es/en/rabbitmq-streams-vs-quorum-queues/) |
| B8 | 2023 Messaging Benchmark (OMB) | StreamNative (Tornow, West, Merli) | 2023-03-01 | Pulsar 2.11.0, RMQ 3.10.7, NATS 2.9.6 | RMQ quorum, RF 3; 3× i3en.6xlarge; 1 KB | Peak consume: Pulsar 2.6 M, RMQ 48 K, NATS 160 K msg/s | "Pulsar p99 300× better than RMQ at 50 topics" | Vendor (Pulsar), no Kafka. Pre-3.12 QQ. OMB's RMQ driver uses auto-ack | [streamnative.io](https://streamnative.io/blog/comparison-of-messaging-platforms-apache-pulsar-vs-rabbitmq-vs-nats-jetstream) |
| C1 | Kafka performance #1: linger.ms | Jack Vanlightly (Confluent) | 2026-07-07 | **Kafka 3.7.2 vs 4.3.0** | acks=all, idempotence, RF 3, min ISR 2, TLS; 1 KB, 120 partitions | 100 K rec/s in, 200 K out (fan-out 2) | p99.9: 3.7.2 ~700 ms spikes, 4.3.0 ~8 ms. Keyed workload with linger 20: 23 ms | Home lab (Threadripper, k8s). The gain is the linger.ms default, not a faster broker | [jack-vanlightly.com](https://jack-vanlightly.com/blog/2026/7/7/apache-kafka-performance-1-lingerms) |
| C2 | Consumer groups vs share groups + tuning | Jack Vanlightly (Confluent) | 2026-05-22 / 05-25 | Kafka 4.2.0 (checked against 4.3.0) | TLS; k3d on Threadripper + EKS m6i.2xlarge; 1 KB | Share ≈ consumer groups ("same ball park"). Defaults: 4,800 vs 60 K target (300 consumers, 5 ms work, 6 partitions). Tuned: 57–60 K | Share-group p99 "a little more choppy", more CPU | "Educational, not canonical" | [overhead](https://jack-vanlightly.com/blog/2026/5/22/benchmarking-apache-kafka-consumer-groups-vs-share-groups-overhead-test), [tuning](https://jack-vanlightly.com/blog/2026/5/25/kafka-share-groups-and-parallelizing-consumption-part-1-tuning-maxpollrecords) |
| C3 | Can Kafka Queues make consumers faster? | Yaroslav Tkachenko | 2026-04-27 | Kafka 4.2.0 (Strimzi 0.51.0) | 3× m8i.xlarge; 4 partitions; 400 M × 1 KB; no-op consumers | 4 classic consumers ~1 M rec/s. **8 share consumers slower than 1 classic** | n/a | No processing time, so it's a firehose test (see [05](05-convergence-share-groups-vs-streams.md)) | [streamingdata.tech](https://www.streamingdata.tech/p/can-kafka-queues-make-consumers-faster) |
| C4 | Kafka vs Redpanda: do the claims add up? | Jack Vanlightly (Confluent) | 2023-05-15 | Kafka vs Redpanda | 3× i3en.6xlarge, same as the vendor's run | Redpanda: 50 producers → 24 s e2e. Keyed: topped out at 330 of 500 MB/s | Redpanda after 12 h: p99 3.5 s | No RabbitMQ. Included for method lessons | [jack-vanlightly.com](https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up) |
| C5 | 2 million writes/s on three cheap machines | LinkedIn (Jay Kreps) | 2014-04-27 | ~0.8.1 | 3 brokers, 6× 7200 rpm SATA, 1 GbE, 100 B msgs | 1 producer, 3× sync repl: 421,823 rec/s (40.2 MB/s). 3 producers async: 2,024,032 rec/s | e2e p50 2 ms / p99 3 ms / p99.9 14 ms | Historical. Origin of "Kafka = millions/s" | [linkedin.com](https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines) |
| B9 | RabbitMQ hits 1 M msg/s on GCE | Pivotal / Tanzu | 2014-03-25 | not stated | 32 VMs × 8 vCPU; durability not stated | 1,345,531 in / 1,413,840 out msg/s | n/a | Historical, 30+ nodes | [vmware.com](https://blogs.vmware.com/tanzu/rabbitmq-hits-one-million-messages-per-second-on-google-compute-engine/) |
| D1 | "RabbitMQ vs Kafka 2026: 16x Throughput Gap [Tested]" | Tech Insider ("Sofia Lindström") | 2026-04-28 (upd 08-17) | claims 4.1 / 4.3 | Not given | Kafka 880 K; RMQ CQ 52 K; streams 650 K "OpenMessaging 2024" (unverified) | Ranges only | Calls Confluent's 2020 run "2024" on "3× m5.4xlarge" (it was i3en.2xlarge). Sources unlinked. **Don't cite** | [tech-insider.org](https://tech-insider.org/rabbitmq-vs-kafka-2026/) |
| D2 | "Streaming Showdown: Redpanda, Kafka, RabbitMQ" | Medium (@elammarisoufiane) | ~2026 (unverified) | (unverified) | Docker Compose (unverified) | Redpanda 121,977; Kafka 67,509; RMQ Streams 54,048 msg/s (unverified) | "Avg 5.771 μs" for Kafka: not credible for end-to-end | Couldn't fetch (TLS error) | [medium.com](https://medium.com/@elammarisoufiane/the-streaming-showdown-redpanda-kafka-rabbitmq-benchmarking-performance-head-to-head-1e4eb90e4644) |
| D3 | Next-Gen Event-Driven Architectures (arXiv 2510.04404) | Arafat et al. | 2025-10-06 | Kafka 3.5; RMQ unspecified | Kafka only (RF 3) | Kafka "1.2 M msg/s, p95 18 ms"; RMQ "200–500 K" | — | No testbed. Numbers come from cited literature | [arxiv.org](https://arxiv.org/abs/2510.04404) |
| D4 | SEO roundups | sanj.dev, onidel.com, atryx/kafka-vs-rabbitmq | 2025–2026 | — | — | e.g. sanj.dev: RMQ 25 K vs Kafka 2.1 M | — | No method, no harness. sanj.dev: "for educational and demonstration purposes only" | [sanj.dev](https://sanj.dev/post/nats-kafka-rabbitmq-messaging-comparison/) |

**Count:** 17 measured benchmarks (A1–A3, B1–B9, C1–C5) plus 4 flagged low-credibility sources (D1–D4).

## Per benchmark

### A1. Confluent 2020: the "15x" chart
- 3× i3en.2xlarge (8 vCPU, 2× NVMe), 1 KB messages, 100 Kafka partitions vs 24 RabbitMQ queues.
- RabbitMQ ran **mirrored queues with persistence off and auto-ack**. Confluent explains: *"Stronger durability is provided through the more recently introduced quorum queues but at the cost of performance… we restricted our evaluation to classic and mirrored queues."*
- Pulsar fsynced every write (`journalSyncData=true`). Kafka did not fsync. They also argue that *"running Kafka with synchronous fsync is extremely uncommon and also unnecessary."*
- **For the talk:** the numbers are MB/s, not msg/s. Mirrored queues no longer exist. The run uses non-durable RabbitMQ against durable-by-replication Kafka.

### A2. SoftwareMill mqperf 2020
- 3-node clusters on r5.2xlarge. RabbitMQ used quorum queues with publisher confirms, fsynced. Kafka used `acks=-1`, `min.insync.replicas=2`, and the default asynchronous flush.
- Kafka 828,836 msg/s vs RabbitMQ 19,120 msg/s. p95 processing latency: 49 ms vs 142 ms.
- **This is the opposite skew from A1.** Here RabbitMQ pays for fsync and Kafka doesn't. The authors' own verdict: Kafka is fastest "at the cost of feature set".
- It predates the 3.12 quorum-queue rewrite (B5), so the RabbitMQ number is stale.

### A3. Maharjan et al. 2023 (peer-reviewed)
- Used OMB on RMQ 3.11.10 / Kafka 3.4.0. Ranking: Kafka first on throughput, RabbitMQ second; Redis lowest latency.
- The MDPI full text returned 403, so I couldn't verify hardware, durability or the exact figures. Raw JSON results are in the paper's supplement.

### B1. RabbitMQ AMQP 1.0 benchmarks (4.0)
- Intel NUC, 8 cores, single node, client on the same box, 12-byte durable messages.
- The p99 values are taken **at max throughput**, so they mostly measure queueing delay (stream p99 1.6 s).
- The post's own warning: *enterprise-grade disks are critical* for quorum queues. Fsync latency sets the ceiling. The shared Raft WAL makes each fsync cheaper as node traffic rises.

### B2. RabbitMQ 4.1
- Quorum-queue disk reads moved to channel processes. While consumers drain a long queue, publishing in 4.0 collapsed from ~6 K to ~100 msg/s; 4.1 kept ~6 K. Consumption was almost 2× faster.
- **For the talk:** a backlog is where queues historically suffered, and that is version-sensitive. Anything before 4.1 understates RabbitMQ on long queues.

### B3. Stream read-ahead (4.2)
- One consumer reading 3 M pre-filled messages on a 3× m7i.4xlarge cluster. The fix gives ~8.4× for 1-message chunks (16 K → 134 K msg/s).
- **Takeaway:** chunk fullness dominates stream reads, exactly as batch fullness dominates Kafka. It's the RabbitMQ twin of C1.

### B4. Streams overview (source of "4 M / 17 M")
- 3× c2-standard-16 with the native Java stream client. Throughput: 1.14 M msg/s = 33.3 MB/s; with sub-entry batching, 4.14 M msg/s = 93.2 MB/s.
- **Watch out:** that is ~23–29 bytes per message. In MB/s, a single stream is far below Confluent's Kafka 605 MB/s, though on different hardware. Big msg/s numbers usually mean tiny messages.

### B5 / B6. Older RabbitMQ team posts
- 3.12 fixed quorum-queue behaviour on long queues (publish ~10 K → >20 K, drain <1 K → >15 K msg/s). Benchmarks from 3.8–3.11 (A1, A2, B8) predate this.
- 2020 sizing study: with QQ RF 3 and 1 KB messages, the best result was 65 K msg/s on 7× c5.4xlarge, and gp2 matched io1. Throughput grows by adding **queues**; one queue sits on one Raft leader.

### B7. jacar.es (RabbitMQ 4.3.6, laptop-class)
- Same RabbitMQ version as our `compose.yaml` (4.3.6): single node, 4 CPU / 4 GiB container, 1 KB messages, median of 3 runs × 30 s.
- A stream over its own protocol moved **6.2×** the messages of a quorum queue. Over AMQP 0-9-1 a stream consumed only 62 K msg/s against 121 K published.
- **The PHP angle in one number:** PHP consumes streams over AMQP, so it gets the 62 K path, not the 281 K one.
- Also measured 4.3 compaction: with one unacked message, 4.2.9 kept 1.02 GB of quorum-queue segments; 4.3.6 kept 8.9 MB.

### B8. StreamNative 2023
- Pulsar 2.6 M vs RabbitMQ 48 K msg/s peak consume, on quorum queues, RF 3, RMQ 3.10.7.
- Commenters objected that it pits a queue against a log; the fair opponent is streams. It is also pre-3.12 and uses OMB's RabbitMQ driver (see checklist).

### C1. Kafka 3.7.2 vs 4.3.0 (linger.ms)
- With acks=all, RF 3 and min ISR 2: at 5 K rec/s, 4.3.0 had **higher** end-to-end latency than 3.7.2. At 100 K rec/s, p99.9 went from ~700 ms to ~8 ms.
- **Cause:** Kafka 4.0 changed the `linger.ms` default from 0 to 5. Batching depends on the rate *per producer per partition*, not on cluster throughput.
- **For the demo:** `kafka-producer-perf-test.sh` on 4.3 batches by default. Set `linger.ms` explicitly so both sides are comparable.

### C2 / C3. Share groups (KIP-932) performance
- **C2 (Vanlightly):** share groups match consumer groups on raw throughput but use more CPU and have choppier p99. The defaults are a trap: `group.share.partition.max.record.locks=2000` with `max.poll.records=500` allows only ~4 effective consumers per partition. Fix: `max.poll.records ≈ locks / consumers_per_partition`.
- **C3 (Tkachenko):** a no-op firehose ran ~1 M rec/s on 4 classic consumers, and 8 share consumers were slower than 1 classic. Share groups pay off for slow per-message work, which is the PHP worker case, not firehoses.

### C4. Kafka vs Redpanda (method lessons only)
- On the vendor's own hardware, small workload changes flipped the result: 4 → 50 producers, record keys, a 12 h run (drive degradation), or crossing the retention limit.
- **Lesson:** a 30-minute run with the vendor's workload file shows the vendor's best case.

### C5 / B9. 2014 baselines
- Kafka: 421,823 rec/s with acks=all on spinning disks and 100-byte messages. RabbitMQ: 1.3 M msg/s in, but on 32 VMs.
- Both are folklore anchors, not evidence about 4.x.

### D1–D4. Don't cite
- **Tech Insider:** misdates and misdescribes Confluent's run, and its sources are unlinked. [05](05-convergence-share-groups-vs-streams.md) also flags it.
- **Medium "Showdown":** reports microsecond "latency", which is probably client send time. Couldn't fetch it.
- **arXiv 2510.04404:** its numbers come from literature, not a testbed.
- **sanj.dev / onidel / atryx:** no methodology at all.

## How to read benchmarks (checklist)

- [ ] **Same fsync?**
  - Quorum queue: fsync before confirm.
  - Stream: no fsync ([docs](https://www.rabbitmq.com/docs/streams)).
  - Kafka: no fsync by default. `flush.messages=1` turns it on (OMB ships it as `driver-kafka/kafka-sync.yaml`).
- [ ] **Same replication and acks?** Use RF 3 on both sides. For Kafka, `acks=all` **and** `min.insync.replicas=2`. For RabbitMQ, publisher confirms on.
- [ ] **Same consumer safety?** OMB's RabbitMQ driver consumes with `autoAck=true`, which is at-most-once (`RabbitMqBenchmarkConsumer.java:41`).
- [ ] **Same abstraction?** Compare a partition to a stream, and a queue to a share group. A partition against a quorum queue is a log against a queue.
- [ ] **Same protocol?** Native stream protocol vs AMQP 0-9-1 is ~4.5× on the same box (B7), and PHP only has AMQP.
- [ ] **OMB's RabbitMQ driver has limits:**
  - AMQP 0-9-1 Java client 5.18.0, one fanout exchange per topic.
  - `messagePersistence: false` by default.
  - **Throws if partitions ≠ 1**, so "100 partitions" for Kafka means N separate topics for RabbitMQ.
  - `queueType: STREAM` exists but consumes with auto-ack, which streams over 0-9-1 don't support. The docs require manual acks plus prefetch, so OMB can't fairly test streams as shipped. This is inferred from the code, not from running it.
- [ ] **msg/s or MB/s?** 12-byte (B1) and ~23-byte (B4) messages inflate msg/s. Normalize to 1 KB.
- [ ] **Batching matched?** Kafka `linger.ms`/`batch.size`; RabbitMQ stream sub-entry batching, chunk fullness, and confirm window (`-c`).
- [ ] **Latency at which load?** p99 at saturation measures queueing (B1). Ask for p99 at a *fixed* rate below the max (B7: 10 K msg/s).
- [ ] **Steady state or backlog?** Confluent served all reads from cache. Backlogs and long queues behave very differently (B2, B5, C4).
- [ ] **Long enough?** Run past retention and past drive warm-up. A 30 s laptop run is a demo, not a benchmark.
- [ ] **Load generator on the same box?** The Java client competes with the broker for CPU (B1, B7, our laptop).
- [ ] **Era?** Anything with mirrored queues, pre-3.12 quorum queues, or ZooKeeper-era Kafka is history.
- [ ] **Who ran it?** Confluent, StreamNative, Redpanda and the RabbitMQ team are all vendors. Jack Vanlightly wrote RabbitMQ-team posts in 2020 (B6) and Confluent-era posts later (C1, C2, C4).
- [ ] **Reproducible?** Look for linked configs, tool versions and raw results. No harness means don't cite it.

## If you run a mini benchmark in the demo

**Goal:** show *relative* behaviour on one laptop, not a winner. Say on stage that it is single node, RF 1, and a Docker VM disk.

### Tools

| Tool | Measures | Notes |
|---|---|---|
| `rabbitmq-perf-test` (PerfTest 2.25.0, 2026-06-30) | AMQP 0-9-1 queues and streams | `--quorum-queue` / `--stream-queue` force persistent, non-auto-delete queues. **Confirms stay off unless you pass `--confirm`** (default `-1`). Streams default to prefetch 200 |
| `stream-perf-test` (1.8.0, 2026-07-09) | Native stream protocol | Needs the `rabbitmq_stream` plugin and port 5552. Has `--sub-entry-size` and `--batch-size` |
| `kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh` | Kafka produce / consume | Both ship in `apache/kafka:4.3.1`. Use `--num-records` (`--messages` is deprecated) |
| `kafka-e2e-latency.sh` | Kafka end-to-end p50/p99/p99.9 | Named flags. Positional args are deprecated until 5.0 |
| `kafka-share-consumer-perf-test.sh` | Share groups | Ships in 4.3 `bin/` |
| OMB (`openmessaging/benchmark`) | Cross-system, cloud clusters | Needs Java plus Maven and is cluster-oriented. **Not for a live demo** (see checklist) |

### Commands (match `compose.yaml`: user `app/app`, network `rabbitmq-kafka-demo_default`)

```bash
# RabbitMQ: quorum queue, 1 KB, confirms + manual multi-ack, 30 s
docker run --rm --network rabbitmq-kafka-demo_default pivotalrabbitmq/perf-test:latest \
  --uri amqp://app:app@rabbitmq:5672 --quorum-queue --queue perf-qq \
  --producers 1 --consumers 1 --size 1000 \
  --confirm 1000 --qos 1000 --multi-ack-every 100 --time 30

# RabbitMQ: stream over AMQP 0-9-1 (the PHP path)
docker run --rm --network rabbitmq-kafka-demo_default pivotalrabbitmq/perf-test:latest \
  --uri amqp://app:app@rabbitmq:5672 --stream-queue --queue perf-sq \
  --producers 1 --consumers 1 --size 1000 \
  --confirm 1000 --qos 1000 --multi-ack-every 100 --time 30

# RabbitMQ: stream over native protocol (Java/Go/.NET path)
docker compose exec rabbitmq rabbitmq-plugins enable rabbitmq_stream
docker run --rm --network rabbitmq-kafka-demo_default pivotalrabbitmq/stream-perf-test:latest \
  --uris rabbitmq-stream://app:app@rabbitmq:5552/%2f \
  --producers 1 --consumers 1 --size 1000 --time 30

# Kafka: topic, producer (linger pinned), consumer
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --topic perf --partitions 6 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-producer-perf-test.sh --bootstrap-server kafka:19092 \
  --topic perf --num-records 3000000 --record-size 1000 --throughput -1 \
  --producer-props acks=all linger.ms=5 batch.size=65536
docker compose exec kafka /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server kafka:19092 \
  --topic perf --num-records 3000000

# Kafka: fsync-matched run (compare against the quorum-queue number)
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:19092 \
  --alter --entity-type topics --entity-name perf --add-config flush.messages=1

# Kafka: end-to-end latency at low load
docker compose exec kafka /opt/kafka/bin/kafka-e2e-latency.sh --bootstrap-server kafka:19092 \
  --topic perf-lat --num-records 10000 --producer-acks all --record-size 1000
```

### What to expect on a laptop

| Run | Expect | Source |
|---|---|---|
| Quorum queue, 1 KB | ~45 K msg/s; p99 ~34 ms at 10 K msg/s | B7 (4 CPU / 4 GiB, Apple silicon) |
| Stream over AMQP | ~120 K published / ~62 K consumed; p99 ~5 ms at 10 K msg/s | B7 |
| Stream over native protocol | ~280 K msg/s (~517 K with a 16 GiB container); p99 ~2 ms | B7 |
| Kafka producer, 1 KB, RF 1 | No verified laptop figure. Expect it to be bounded by the Docker VM's disk and CPU, not by Kafka. **Dry-run and record it** | (unverified) |
| Kafka with `flush.messages=1` | Should fall toward the quorum-queue number. That is the fsync point made live | (unverified, by design) |

### Gotchas
- **Single node hides replication.** RF 1 with `acks=all` means one leader's page cache. A 1-member quorum queue still fsyncs but doesn't replicate. Say both on stage.
- **Docker Desktop / OrbStack** fsync to a VM virtual disk. Quorum-queue numbers depend on fsync latency (B1), so laptop numbers don't transfer to production NVMe/EBS.
- **Pin `linger.ms`.** The 4.x default is 5 ms (C1), which shifts Kafka latency at low rates.
- **One producer is one partition-batch stream.** Many partitions with one producer means small batches (C1). Keep partitions modest (6).
- **The Java tools measure the broker ceiling, not PHP.** A php-amqplib loop that waits for a confirm after every publish will be far slower. Measure it rather than quote it. The PHP worker is the bottleneck, and that is the talk's point.
- **Run each test 3× and report the median** (B7 did). Close other heavy apps, and don't run both brokers' tests at the same time.

## Sources

- Confluent, *Benchmarking Apache Kafka, Apache Pulsar, and RabbitMQ*, 2020-08-21: https://www.confluent.io/blog/kafka-fastest-messaging-system/ · OMB fork: https://github.com/confluentinc/openmessaging-benchmark
- SoftwareMill, *Evaluating persistent, replicated message queues (2020 edition)*, 2020-12-08: https://softwaremill.com/mqperf/
- Maharjan et al., *Benchmarking Message Queues*, Telecom 4(2), 2023-06-13: https://www.mdpi.com/2673-4001/4/2/18 (doi:10.3390/telecom4020018)
- RabbitMQ blog, *AMQP 1.0 Benchmarks*, 2024-08-21: https://www.rabbitmq.com/blog/2024/08/21/amqp-benchmarks
- RabbitMQ blog, *RabbitMQ 4.1 Performance Improvements*, 2025-04-08: https://www.rabbitmq.com/blog/2025/04/08/4.1-performance-improvements
- RabbitMQ blog, *Delivery Optimization for RabbitMQ Streams*, 2025-09-26: https://www.rabbitmq.com/blog/2025/09/26/stream-delivery-optimization
- A. Cogoluègnes, *RabbitMQ Streams Overview*, RabbitMQ Summit, 2021-07-14: https://speakerdeck.com/acogoluegnes/rabbitmq-streams-overview-at-rabbitmq-summit-2021
- RabbitMQ blog, *RabbitMQ 3.12 Performance Improvements*, 2023-05-17: https://www.rabbitmq.com/blog/2023/05/17/rabbitmq-3.12-performance-improvements
- RabbitMQ blog, *Cluster Sizing Case Study – Quorum Queues Part 1*, 2020-06-21: https://www.rabbitmq.com/blog/2020/06/21/cluster-sizing-case-study-quorum-queues-part-1
- RabbitMQ blog, *RabbitMQ 4.3 Highlights*, 2026-04-23 (only quantified claim: halved per-message QQ memory for messages ≤32 KiB): https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release
- RabbitMQ docs, *Streams* (no fsync; 0-9-1 consumption needs prefetch + acks): https://www.rabbitmq.com/docs/streams · *Stream core vs plugin* ("hundreds of thousands" vs "millions" per second): https://www.rabbitmq.com/docs/stream-core-plugin-comparison
- J. Cañete, *RabbitMQ 4.3 streams vs quorum queues, measured*, 2026-09-29: https://jacar.es/en/rabbitmq-streams-vs-quorum-queues/
- StreamNative, *A Comparison of Messaging Platforms: Pulsar vs RabbitMQ vs NATS JetStream*, 2023-03-01: https://streamnative.io/blog/comparison-of-messaging-platforms-apache-pulsar-vs-rabbitmq-vs-nats-jetstream
- J. Vanlightly, *Apache Kafka performance #1 – linger.ms*, 2026-07-07: https://jack-vanlightly.com/blog/2026/7/7/apache-kafka-performance-1-lingerms
- J. Vanlightly, *Benchmarking Apache Kafka Consumer Groups vs Share Groups (overhead test)*, 2026-05-22: https://jack-vanlightly.com/blog/2026/5/22/benchmarking-apache-kafka-consumer-groups-vs-share-groups-overhead-test
- J. Vanlightly, *Kafka Share Groups … Part 1: Tuning max.poll.records*, 2026-05-25: https://jack-vanlightly.com/blog/2026/5/25/kafka-share-groups-and-parallelizing-consumption-part-1-tuning-maxpollrecords
- Y. Tkachenko, *Can Kafka Queues Make Consumers Faster?*, 2026-04-27: https://www.streamingdata.tech/p/can-kafka-queues-make-consumers-faster
- J. Vanlightly, *Kafka vs Redpanda Performance – Do the claims add up?*, 2023-05-15: https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up
- J. Kreps, *Benchmarking Apache Kafka: 2 Million Writes Per Second*, 2014-04-27: https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines
- Pivotal/Tanzu, *RabbitMQ Hits One Million Messages Per Second on GCE*, 2014-03-25: https://blogs.vmware.com/tanzu/rabbitmq-hits-one-million-messages-per-second-on-google-compute-engine/
- Low credibility (D1–D4): https://tech-insider.org/rabbitmq-vs-kafka-2026/ · https://medium.com/@elammarisoufiane/the-streaming-showdown-redpanda-kafka-rabbitmq-benchmarking-performance-head-to-head-1e4eb90e4644 · https://arxiv.org/abs/2510.04404 · https://sanj.dev/post/nats-kafka-rabbitmq-messaging-comparison/
- Tooling (code read 2026-10-04):
  - OMB RabbitMQ driver: https://github.com/openmessaging/benchmark/tree/master/driver-rabbitmq
  - OMB Kafka configs: https://github.com/openmessaging/benchmark/tree/master/driver-kafka
  - PerfTest: https://github.com/rabbitmq/rabbitmq-perf-test
  - Stream PerfTest: https://github.com/rabbitmq/rabbitmq-stream-perf-test
  - Kafka 4.3.0 tools: https://github.com/apache/kafka/tree/4.3.0/tools/src/main/java/org/apache/kafka/tools
