# Convergence: Kafka becoming a queue, RabbitMQ becoming a log

*As of 2026-10-04. Pinned to Kafka 4.3 / RabbitMQ 4.3. Builds on [03-topic-proposal.md](../03-topic-proposal.md) point 2, so the basics aren't repeated here.*

## TL;DR

- **The convergence is lopsided.** Kafka added a queue-style *way of reading* its log (share groups). RabbitMQ added a log as a *separate queue type* (streams), and streams have **no competing consumers**. A RabbitMQ stream "consumer group" is a super stream plus single active consumer, so it has the **same partition ceiling as a Kafka consumer group**. For work distribution, RabbitMQ still tells you to use a queue: "streams were not introduced to replace queues but to complement them" [R1].
- **PHP can't use share groups today.** librdkafka's share consumer is **Preview**: added in 2.15.0 (2026-06-30), unchanged in 2.15.1 and 2.16.0-RC4 (2026-10-01) [K21][K23]. It also has **no RENEW** [K22]. php-rdkafka has **no binding**: [#616](https://github.com/php-rdkafka/php-rdkafka/issues/616) is open with 0 comments, and the maintainers plan to wait for GA [K25]. Confluent's docs say "Share groups are only available for the Kafka Java clients" [K28]. A pure-PHP client claims a `KafkaShareConsumer`, but it is dev-main only, 4 stars, and was created 2026-09-08 [K27].
- **The head-of-line window is real and verified in source.** The in-flight cap is the **distance SPSO→SPEO**, not the number of locked records (`numInFlightRecords = endOffset - startOffset + 1`) [K15]. One stuck record at the start pins the partition after **2000 offsets** by default [K12].
- **Retention beats acknowledgement.** If the topic's retention deletes a segment, unprocessed share-group records are **silently skipped**. The KIP calls this "roughly equivalent to message-based expiration" [K1]. RabbitMQ never drops an unacked queue message unless you configured a TTL or length limit.
- **The DLQ (KIP-1191) is in 4.4, which is not released yet.** RC3 was tagged 2026-09-29 and downloads still top out at 4.3.1 [K18]. It is opt-in, writes **headers only by default** (no payload), doesn't auto-create the topic, and can occasionally write duplicates [K2][K13].
- **Share groups trade throughput for elasticity.** In an independent 4.2.0 benchmark, **8 share consumers delivered less than 1 classic consumer**. Fetching is still bottlenecked by the partition count [A5]. Use them for slow, independent jobs, not firehoses.
- **Independent writers agree on the "is Kafka replacing RabbitMQ?" question:** no. Kafka users who already run it can drop a second broker *if* the work is unordered and needs no priority, delay, TTL, or routing [A1][A2][A3][A4][A9].

## Feature matrix

| | **Kafka classic consumer group** | **Kafka share group (4.3)** | **RabbitMQ quorum queue (4.3)** | **RabbitMQ stream / super stream (4.3)** |
|---|---|---|---|---|
| Consumption model | Partition owned by one member | Records from the same partition spread across members | Competing consumers; broker pushes with prefetch | Non-destructive; every consumer reads everything |
| Max useful parallel consumers | ≤ partitions | > partitions. **`group.share.max.size` = 200 by default, hard max 1000** [K14] | Unbounded (prefetch per consumer) | Fan-out: unbounded. Ordered work split: ≤ partitions (SAC per partition) [R1] |
| Ordering | Per partition | **None across batches**. Increasing offsets only within one batch [K1] | FIFO per queue. Competing consumers, requeues and priorities reorder | Per stream / partition |
| Ack unit | Offset (cumulative) | Per record: ACCEPT / RELEASE / REJECT / RENEW [K16] | Per message: ack / nack / reject | Over AMQP, ack is only a **credit** that advances the offset [R1] |
| Does ack free disk? | No (retention) | **No** (retention) | **Yes** (queue drains) | No (retention) |
| Redelivery trigger | Rebalance / seek | Lock expiry: **30 s default**. Per-group override bounded **15–60 s** unless the broker max is raised (≤ 1 h) [K12] | Channel close / consumer timeout (**30 min default**) [R5] | n/a (consumer re-attaches at an offset) |
| Poison-message limit | None (app) | `delivery.count.limit` **5** (2–10, broker max ≤ 25), then **archived** [K12] | `delivery-limit` **20** (since 4.0) [R4] | None |
| DLQ | App-built topic | **4.4 (unreleased)**: one DLQ topic per group, headers-only by default [K2][K13] | DLX via exchanges: routable, reason in `x-death`, at-least-once optional [R4] | No DLX [R1] |
| Delay / backoff | App-built retry topics | **None**. RELEASE = immediate redelivery [A1] | **Delayed retry** `x-delayed-retry-*` (4.3) [R6] | No |
| TTL | Topic retention | Topic retention, which **skips unprocessed records** [K1] | Per-message + per-queue TTL [R4] | Retention (`max-age`, `max-length-bytes`), segment-granular [R1] |
| Priority | No | No | **32 strict levels** (4.3) [R4] | No |
| Broker-side filtering | No ([KAFKA-6020](https://issues.apache.org/jira/browse/KAFKA-6020) open since 2017) [K20] | No | Exchanges / bindings | Bloom (all protocols) + AMQP 1.0 property filters (4.1) + **SQL filters (4.2)**, AMQP 1.0 only [R2][R8] |
| Pattern subscription | `subscribe(Pattern)` | **No.** Only `subscribe(Collection<String>)` [K16] | Topic exchange `orders.#` | Super-stream routing (hash / key) [R10] |
| Replay | Seek anywhere | `kafka-share-groups.sh --reset-offsets` to earliest / latest / datetime; **group must be inactive** [K17] | No (acked = gone) | `x-stream-offset`: first / last / next / offset / timestamp / interval [R1] |
| In-flight cap | `max.poll.records` (client) | **SPSO→SPEO window 2000** (group 100–4000, broker ≤ 10000) [K12][K15] | Prefetch per consumer | Prefetch / credit |
| Server-side progress | Committed offsets | Per-record state in `__share_group_state` | Message state in Raft log | Offset tracking: **stream protocol only** [R1][R3] |
| Exactly-once | Transactions | `read_committed` reads yes. **Transactional ack: no** (future work) [K1][K10] | No | Publisher dedup: **stream protocol only** [R3] |
| Fetch from follower | Yes ([KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica)) | **No** [K1] | n/a | Stream protocol connects to leader / replicas [R1] |
| Confirm means | `acks=all`: ISR page cache | same | Raft majority **written and fsynced** [R4] | Quorum replicated, **no explicit fsync** (same as Kafka) [R1] |
| Status | GA forever | GA since 4.2.0 (2026-02-17). Run ≥ 4.2.1 (deadlock fix KAFKA-20505) [K7][K19] | GA since 3.8 | Streams 3.9 (2021), super streams + SAC 3.11 (2022) [R1] |
| **PHP client** | php-rdkafka 6.0.5 (2024-11-04) [K26] | **None usable.** librdkafka Preview, no php-rdkafka binding [K21][K25] | php-amqplib (0-9-1) | php-amqplib over 0-9-1 (offset spec + Bloom only). Native: community `crazy-goat/rabbit-stream` v1.4.0, 0 stars [R13] |

## 1. Kafka "Queues for Kafka" (KIP-932 share groups)

### Timeline

| Date | Version | Status | Source |
|---|---|---|---|
| 2025-03-18 | Kafka 4.0 | **Early access**: disabled by default, not for production. 4.0 clients **can't** talk to 4.1 brokers | [K5][K1] |
| 2025-09-04 | Kafka 4.1 | **Preview**: enable with `share.version=1` | [K6][K9] |
| 2026-02-17 | Kafka 4.2 | **GA / "production-ready"**. Adds RENEW (KIP-1222), strict fetch limit `share.acquire.mode` (KIP-1206), adaptive coordinator batching (KIP-1224) and lag metrics (KIP-1226) | [K7][A7] |
| 2026-03-03 | Confluent | GA on Confluent Cloud. Java only. "Non-Java client support targeted for the second half of 2026" | [A6] |
| 2026-05-22 | Kafka 4.3 | KIP-1240: **per-group** `share.delivery.count.limit`, `share.partition.max.record.locks`, `share.renew.acknowledge.enable` (absent in 4.2's `GroupConfig`) | [K8][K13] |
| 2026-05-30 | Kafka 4.2.1 | Critical share-path deadlock fixed (KAFKA-20505, also in 4.3.0) | [K9][K19] |
| 2026-06-30 | librdkafka 2.15.0 | Share consumer **Preview** (C API only) | [K21] |
| 2026-09-29 | Kafka 4.4.0-rc3 tagged | KIP-1191 DLQ behind `share.version=2`. **Not released** as of 2026-10-04 | [K4][K18] |
| (draft) | Kafka 4.5? | KIP-1316 circuit breaker for DLQ floods. Status: Draft | [K3] |

### Semantics (verified against 4.3 source)

- **Record states:** Available → Acquired → Acknowledged / Archived [K1].
- **Acquisition lock:** `group.share.record.lock.duration.ms`, 30000 ms default (broker range 1000–3600000). The per-group `share.record.lock.duration.ms` is clamped to broker min/max, **15000 / 60000** by default [K12]. **PHP impact:** a job longer than 60 s gets redelivered unless the client sends RENEW, and librdkafka can't [K22].
- **Delivery count:** `group.share.delivery.count.limit`, default 5, range 2–10. The broker cap `group.share.max.delivery.count.limit` defaults to 10 (≤ 25) [K12]. The count is "not performed with exactly-once semantics… cannot be relied upon to be precise" [K1].
- **Ack types:** ACCEPT, RELEASE (immediate redelivery), REJECT (archive), RENEW (extend lock; explicit mode only, KIP-1222) [K7][K16].
- **Ack modes:** `share.acknowledgement.mode=implicit` (default; the next poll accepts everything) or `explicit` (must ack every record before the next poll, otherwise `IllegalStateException`) [K10].
- **Start position:** `share.auto.offset.reset` defaults to **latest** [K13]. A share group started after the backlog was produced sees nothing. This is a demo gotcha.

### Limits worth a slide

- **Head-of-line window.** `canAcquireRecords()` returns false once `endOffset - startOffset + 1 >= maxInFlightRecords` [K15]. The KIP says the leader "limits the distance between the SPSO and the SPEO" [K1]. The official design doc words it more softly ("limits the number of records… acquired") [K11]. **The source code agrees with the KIP.**
  - Worst case for a poison record sitting at SPSO with default settings: about **5 × 30 s ≈ 2.5 min** of pinning before it is archived. Meanwhile the partition can serve at most 1999 newer offsets *(derived from defaults)*.
- **No ordering, no key-ordering yet.** Key-based ordering is listed as future work [K1]. Confluent's GA post lists "key-based ordering" on the roadmap [A6].
- **No TTL, priority or delay.** All three are out of scope [K1]. TTL is approximated by retention, which drops unprocessed records [K1].
- **No regex subscription.** The interface has only `subscribe(Collection<String>)` [K16].
- **No fetch-from-follower.** "The fetch-from-follower optimization is not supported by share-groups" [K1]. This costs cross-AZ traffic in the cloud.
- **No transactional ack / EOS** [K1].
- **Size cap:** 200 members per share group by default (≤ 1000) [K14]. That is exactly the "boot 200 PHP workers" number from 03.

### DLQ — KIP-1191 (Accepted, ships in 4.4)

- **Trigger:** REJECT, or the delivery limit is reached. The record moves Archiving → DLQ write → Archived [K2].
- **Configs:**
  - Group: `errors.deadletterqueue.topic.name` (blank = off) and `errors.deadletterqueue.copy.record.enable`, which **defaults to false**, so only the `__dlq.errors.*` context headers are written, not the payload [K13].
  - Cluster: `errors.deadletterqueue.auto.create.topics.enable` defaults to false, plus a permitted topic-name prefix [K2].
- **Caveat:** "more than one DLQ record could be written" because the DLQ write isn't atomic with the share-state write [K2].
- **Gap vs RabbitMQ DLX:** a share group gets one DLQ topic, with no routing by reason, no delayed redrive, and no per-message TTL path. KIP-1316 (draft, targets 4.5) exists because a DLQ flood can cascade into a primary outage [K3].

### Client support — the PHP answer

| Client | Share consumer status | Source |
|---|---|---|
| Java `KafkaShareConsumer` | GA (4.2+) | [K7] |
| Spring for Apache Kafka 4.0 | Supported (blog 2025-10-14, written against 4.1 preview) | [A10] |
| librdkafka 2.15.0 / 2.15.1 / 2.16.0-RC4 | **Preview, "should not be used in production"**. C API only, no C++ wrapper | [K21][K22][K23] |
| confluent-kafka-python 2.15.0 | Preview (wraps librdkafka) | [K24] |
| confluent-kafka-go / -dotnet 2.15.x | No mention in CHANGELOG *(unverified beyond changelog grep)* | [K24] |
| Karafka (Ruby) | "actively in progress" (2026-08-16). Preview PR on karafka-rdkafka | [A11] |
| **php-rdkafka** | **None.** #616 opened 2026-07-02, 0 comments: "we might want to wait until the interfaces are finalised". Last stable release 6.0.5 (2024-11-04) | [K25][K26] |
| lisachenko/kafka-client (pure PHP 8.4) | README claims `Consumer\KafkaShareConsumer` on `dev-main`. No release, repo created 2026-09-08. **Experimental** | [K27] |

The librdkafka Preview limitations, which would carry into any future PHP binding [K22]:
- No RENEW (KIP-1222).
- No wakeup.
- `max.poll.records` is a soft bound.
- No share-group admin APIs.
- Failed acks aren't retried.
- One broker fetched per poll.
- Not thread-safe (fine for PHP).

### Analyses and talks (2025–2026)

| Date | Who | Take | Src |
|---|---|---|---|
| 2025-03-05 | Gunnar Morling | "I don't think Kafka queues in their current form will make users of… Artemis or RabbitMQ migrate" | [A1] |
| 2025 (London, Bengaluru) | Andrew Schofield, Apoorv Mittal (Current 2025) | "Queues for Kafka" talk. Recordings and slides online | [A12] |
| 2025-08-07 | Instaclustr (Paul Brebner) | Early access: 200-member default, "no maximum queue depth", in-flight limit instead | [A13] |
| 2026-03-03 | Confluent (Jonathan Lacefield) | GA. "Consolidate queue and stream workloads on a single Kafka cluster" (vendor) | [A6] |
| 2026-04-27 | Yaroslav Tkachenko | Benchmark (4.2.0, 4 partitions, 400M × 1 KB): share consumer "dramatically lower". Even 8 instances < 1 classic consumer | [A5] |
| 2026-05-06 (upd. 09-22) | Factor House | Per-record state makes lag monitoring harder. Ordering "completely broken". Pitches consolidation | [A8] |
| 2026-06 update | Conduktor (Derosiaux) | "Share groups don't turn the log into a queue. They add a queue-shaped way to read it." | [A2] |
| 2026-07-02 | Aiven (Olena Babenko) | "Not true queues — and that's good": relaxed ordering = elastic scaling. Java-only limits adoption | [A3] |
| 2026-09-28 | Conduktor glossary | "Not a drop-in RabbitMQ or ActiveMQ Artemis replacement" | [A4] |

## 2. RabbitMQ streams and super streams (through 4.3)

### What's shipped

| Version (date) | Stream feature | Source |
|---|---|---|
| 3.9 (2021-07-23) | Streams: append-only replicated log, `x-stream-offset`, retention by age / size | [R1] |
| 3.11 (2022-09-26) | **Super streams** (hash-partitioned, ≈ Kafka topic) + **single active consumer** (≈ consumer group) | [R1][R10] |
| 4.0 (2024-09) | Native AMQP 1.0. Stream filtering for AMQP 1.0 clients (4.0.1 notes) | [R7] |
| 4.1 (2025-04-15) | AMQP 1.0 **property filter expressions** (message-level, broker-side) | [R7][R2] |
| 4.2 (2025-10-27) | **SQL filter expressions** (AMQP 1.0 only): 404 k msg/s alone, 4.87 M msg/s with Bloom. Read-ahead: up to ~10× faster delivery for small-chunk streams, **all protocols incl. AMQP 0-9-1** | [R8][R9] |
| 4.3 (2026-04-23) | OSS: `stream.read_ahead` setting, deletion robustness. Streams don't evaluate consumer timeouts. **Stream Browser UI and Spark connector are Tanzu-only (commercial)** | [R6][R7] |

### The three access paths (what PHP actually gets)

| Capability | Stream protocol (port 5552) | AMQP 1.0 | AMQP 0-9-1 (**php-amqplib**) |
|---|---|---|---|
| Throughput | "Millions/s" | "Hundreds of thousands/s" | "Hundreds of thousands/s" [R3] |
| Super streams | Yes | No | No [R3] |
| Single active consumer | Yes | No | No [R3] |
| Server-side offset tracking | Yes | No | No [R1][R3] |
| Publish dedup | Yes | No | No [R3] |
| Bloom filter | Yes | Yes | Yes (`x-stream-filter`) [R2] |
| Property / SQL filters | No | **Yes** | No [R2] |
| PHP client | Community only (`crazy-goat/rabbit-stream`) | **None** | php-amqplib |

### Streams vs Kafka topics: posts and claims

- **RabbitMQ compare page (vendor):** "The two systems started from opposite ends of the problem and have been growing towards each other for years." It says streams match Kafka's throughput techniques but lack **log compaction** and **tiered storage**, and are "bounded by local disk" [R11].
- **RabbitMQ super streams blog (2022-07-13, Cogoluègnes):** super stream ≈ topic, stream ≈ partition, but "there is no real 1-to-1 mapping". Streams are first-class named objects [R10].
- **Tanzu (2022-12-19, Gregory Green, vendor):** streams handle "millions of events per second". The pitch is that RabbitMQ scales by adding streams while Kafka partitions must be planned up front [R12].
- **SQL filter post (2025-09-23, Ansari, vendor):** "Kafka users have requested it for years (see KAFKA-6020) — but Kafka still lacks this capability" [R8]. Verified: KAFKA-6020 is open, created 2017-10-06 [K20].
- **Tiered storage for RabbitMQ streams** exists only as a talk/prototype: Simon Unge (AWS), MQ Summit 2025, "Breaking Storage Barriers" [R14]. It is **not** in OSS 4.3 *(no shipping product found; unverified)*.
- **Durability parity nobody mentions:** streams "do not explicitly flush (fsync)… rely on the operating system". That is the same trade Kafka makes. RabbitMQ's own docs say to use quorum queues if you need more safety [R1].

## 3. "Is Kafka replacing RabbitMQ now that it has queues?"

| Source (date) | Verdict |
|---|---|
| Kai Waehner (2026-07-17) | "Kafka added queue semantics… RabbitMQ added a log." But "the models underneath, and the trade-offs… have not merged". Both often run side by side [A9] |
| Morling (2025-03-05) | Won't drive RabbitMQ users to migrate. Useful for teams already on Kafka [A1] |
| Conduktor (2026-09-28) | Not a drop-in replacement. Gaps: no DLQ, no delayed / backoff retry, retention not drain [A4] |
| Aiven (2026-07-02) | Different trade-off by design: ordering relaxed for elasticity [A3] |
| IoT Digital Twin PLM (2026-09-20, smaller blog) | **The deciding hole is the DLQ.** "If you do not already run Kafka, Queues for Kafka is not the cheap option" [A14] |
| Factor House (2026-05/09), Confluent (2026-03) | Pro-consolidation: retire the separate queue broker (Kafka-side vendors) [A6][A8] |
| RabbitMQ streams docs | "Streams were not introduced to replace queues but to complement them" [R1] |
| tech-insider.org "RabbitMQ vs Kafka 2026" | **Don't cite.** Fabricated-looking quotes and wrong event names (flagged during research) |
| MQ Summit 2026 (Haarlem, **2026-10-21/22**) | Viktor Gamov (Confluent), "From Queues to Logs and Back Again". No abstract yet, and it happens **after** today. Check the recording before the talk [A15] |

**Consensus:** convergence is real at the feature-list level, not at the model level. Kafka's queue is still a log with a cursor window. RabbitMQ's log is still a separate queue type that doesn't do work distribution.

## Demo talking points

1. **"Both added the other's thing, and neither became the other."** Kafka reads a log like a queue. RabbitMQ stores a log beside its queues. Neither added the other's core: no ack-deletes in Kafka, no competing consumers on a RabbitMQ stream.
2. **The Demo B twist.** Kafka *did* fix the partition ceiling — for Java. Show the status line: librdkafka Preview, php-rdkafka #616 open with 0 comments, Confluent docs "only available for the Kafka Java clients".
   - Optionally show it live with the Java CLI against the same 4-partition `jobs` topic *(commands untested here)*:
     ```sh
     docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:19092 \
       --entity-type groups --entity-name workers --alter --add-config share.auto.offset.reset=earliest
     # run in N terminals: all N receive records from 4 partitions
     docker compose exec kafka /opt/kafka/bin/kafka-console-share-consumer.sh --bootstrap-server kafka:19092 --topic jobs --group workers
     docker compose exec kafka /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server kafka:19092 --describe --group workers --members
     ```
   - `compose.yaml` already sets the RF=1 share-state overrides. Without the `earliest` override the group starts at **latest** and shows nothing.
3. **The 60-second rule.** A share-group lock maxes out at 60 s per group by default, and the only extension is RENEW, which librdkafka lacks. A 2-minute PHP job (PDF render, video, import) **will be redelivered**. A RabbitMQ consumer timeout is 30 min.
4. **The window, not the lock.** One stuck record pins 2000 offsets. Draw SPSO and SPEO on a slide. This is "HOL blocking reduced, not eliminated" with a number on it.
5. **Retention beats ack.** Leave a share group down past `retention.ms` and the unprocessed jobs vanish without a DLQ entry. In RabbitMQ they wait.
6. **The DLQ is coming, with a footnote.** Kafka 4.4 adds it, but by default it stores headers only, it's one topic per group, and it can write duplicates. RabbitMQ's DLX is routable with a reason.
7. **Share groups are for slow work, not throughput.** In the Tkachenko benchmark, 8 share consumers delivered less than 1 classic consumer.
8. **Streams from PHP = the 0-9-1 subset.** You get replay plus Bloom filtering. You don't get offset tracking, SAC, super streams, dedup, or SQL filters. Same message as 03 point 5, now with the version table.

## Sources

**Kafka primary**
- [K1] KIP-932 Queues for Kafka (wiki v138, 2026-01-26): https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
- [K2] KIP-1191 Dead-letter queues for share groups (Accepted, updated 2026-07-16): https://cwiki.apache.org/confluence/display/KAFKA/KIP-1191:+Dead-Letter+Queues+for+Share+Groups
- [K3] KIP-1316 Circuit Breaker for Share Group DLQ Overflow (Draft, 2026-04-06): https://cwiki.apache.org/confluence/spaces/KAFKA/pages/406622499/KIP-1316+Circuit+Breaker+for+Share+Group+DLQ+Overflow
- [K4] Release Plan 4.4.0 (KIP-1191 "Complete"; release ≥ 2026-09-09): https://cwiki.apache.org/confluence/spaces/KAFKA/pages/429064575/Release+Plan+4.4.0
- [K5] Apache Kafka 4.0.0 release announcement (2025-03-18): https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/
- [K6] Apache Kafka 4.1.0 release announcement (2025-09-04): https://kafka.apache.org/blog/2025/09/04/apache-kafka-4.1.0-release-announcement/
- [K7] Apache Kafka 4.2.0 release announcement (2026-02-17): https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/
- [K8] Apache Kafka 4.3.0 release announcement (2026-05-22): https://kafka.apache.org/blog/2026/05/22/apache-kafka-4.3.0-release-announcement/
- [K9] Kafka 4.2 upgrade notes (GA wording, < 3 brokers, 4.2.1 fix): https://kafka.apache.org/42/getting-started/upgrade/
- [K10] KafkaShareConsumer Javadoc 4.3.1: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html
- [K11] Kafka 4.3 design doc, "The Share Consumer": https://github.com/apache/kafka/blob/4.3/docs/design/design.md
- [K12] ShareGroupConfig.java (4.3, defaults and ranges): https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/share/ShareGroupConfig.java
- [K13] GroupConfig.java (4.2 vs 4.3 vs 4.4: per-group share and DLQ configs): https://github.com/apache/kafka/blob/4.4/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupConfig.java
- [K14] GroupCoordinatorConfig.java (4.3, `group.share.max.size` 200, between 1–1000): https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java
- [K15] SharePartition.java (4.3, `canAcquireRecords` / `numInFlightRecords`): https://github.com/apache/kafka/blob/4.3/core/src/main/java/kafka/server/share/SharePartition.java
- [K16] ShareConsumer.java + AcknowledgeType.java (4.3): https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/ShareConsumer.java
- [K17] ShareGroupCommandOptions.java (4.3, reset-offsets): https://github.com/apache/kafka/blob/4.3/tools/src/main/java/org/apache/kafka/tools/consumer/group/ShareGroupCommandOptions.java
- [K18] Kafka downloads (latest 4.3.1 / 4.2.2, no 4.4.0) and tag 4.4.0-rc3 (2026-09-29): https://downloads.apache.org/kafka/ · https://github.com/apache/kafka/tree/4.4.0-rc3
- [K19] KAFKA-20505 share-path deadlock (fix 4.2.1, 4.3.0): https://issues.apache.org/jira/browse/KAFKA-20505
- [K20] KAFKA-6020 Broker side filtering (Open, 2017-10-06): https://issues.apache.org/jira/browse/KAFKA-6020

**Clients**
- [K21] librdkafka CHANGELOG (2.15.0 Preview; 2.16.0 no share changes): https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md
- [K22] librdkafka INTRODUCTION, "Share consumers" + "Current limitations (preview)": https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#share-consumers-queues-for-kafka
- [K23] librdkafka releases (2.15.0 2026-06-30; 2.15.1 2026-09-09; v2.16.0-RC4 2026-10-01): https://github.com/confluentinc/librdkafka/releases
- [K24] confluent-kafka-python CHANGELOG (2.15.0 Preview): https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md · Go/.NET changelogs: https://github.com/confluentinc/confluent-kafka-go · https://github.com/confluentinc/confluent-kafka-dotnet
- [K25] php-rdkafka #616 "Add share consumers" (open, 2026-07-02): https://github.com/php-rdkafka/php-rdkafka/issues/616
- [K26] php-rdkafka releases (6.0.5, 2024-11-04): https://github.com/php-rdkafka/php-rdkafka/releases
- [K27] lisachenko/kafka-client (pure PHP; created 2026-09-08; PR #227 merged 2026-09-24): https://github.com/lisachenko/kafka-client
- [K28] Confluent Platform docs, share consumers ("only available for the Kafka Java clients", 2026-03-03): https://docs.confluent.io/platform/current/clients/share-consumers.html

**Analyses / talks**
- [A1] Gunnar Morling, "Let's Take a Look at… KIP-932" (2025-03-05): https://www.morling.dev/blog/kip-932-queues-for-kafka/
- [A2] Conduktor, "Kafka Isn't a Queue" (2025-02-21, updated June 2026): https://www.conduktor.io/blog/kafka-isnt-a-queue
- [A3] Aiven, "Share Groups are NOT true queues" (2026-07-02): https://aiven.io/blog/apache-kafka-share-groups-are-not-true-queues
- [A4] Conduktor glossary, "Kafka Queues: Share Groups Explained" (2026-09-28): https://www.conduktor.io/glossary/kafka-share-groups
- [A5] Yaroslav Tkachenko, "Can Kafka Queues Make Consumers Faster?" (2026-04-27): https://www.streamingdata.tech/p/can-kafka-queues-make-consumers-faster
- [A6] Confluent, "Kafka Queue Semantics Now GA" (2026-03-03): https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/
- [A7] Confluent, "Apache Kafka 4.2.0 Released" (2026-02-20): https://www.confluent.io/blog/apache-kafka-4-2-release/
- [A8] Factor House, "KIP-932 queues for Kafka explained" (2026-05-06, upd. 2026-09-22): https://factorhouse.io/articles/kip-932-queues-for-kafka-explained/
- [A9] Kai Waehner, "When to Use AMQP, JMS, Kafka, or MQTT" (2026-07-17): https://www.kai-waehner.de/blog/2026/07/17/when-to-use-amqp-jms-kafka-or-mqtt-trade-offs-not-a-winner/
- [A10] Spring, "Introducing Share Consumer Support" (2025-10-14): https://spring.io/blog/2025/10/14/introducing-spring-kafka-share-consumer/
- [A11] Karafka docs, "Consumer Groups vs Share Groups" (2026-08-16): https://karafka.io/docs/Basics-Consumer-Groups-vs-Share-Groups/
- [A12] Current 2025, "Queues for Kafka" (Schofield / Mittal): https://current.confluent.io/post-conference-videos-2025/queues-for-kafka-lnd25 · https://current.confluent.io/post-conference-videos-2025/queues-for-kafka-bng25
- [A13] Instaclustr, "Kafka 4.0 share groups" (2025-08-07): https://www.instaclustr.com/blog/apache-kafka-4-0-share-groups-what-you-need-to-know-about-queues-for-kafka/
- [A14] IoT Digital Twin PLM, "Kafka Queues vs RabbitMQ vs SQS: 2026 Decision Guide" (2026-09-20): https://iotdigitaltwinplm.com/kafka-queues-vs-rabbitmq-vs-sqs-task-queues-2026/
- [A15] MQ Summit 2026 programme (2026-10-21/22): https://mqsummit.com/

**RabbitMQ**
- [R1] Streams and Super Streams docs (4.3): https://www.rabbitmq.com/docs/streams
- [R2] Stream Filtering docs (4.3): https://www.rabbitmq.com/docs/stream-filtering
- [R3] Stream core vs stream plugin: https://www.rabbitmq.com/docs/stream-core-plugin-comparison
- [R4] Quorum queues docs (4.3): https://www.rabbitmq.com/docs/quorum-queues
- [R5] Consumers docs, delivery acknowledgement timeout (30 min default): https://www.rabbitmq.com/docs/consumers
- [R6] RabbitMQ 4.3 Highlights (2026-04-23): https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release
- [R7] Release notes 4.0.1 / 4.1.0 / 4.2.0 / 4.3.0 and GitHub release dates: https://github.com/rabbitmq/rabbitmq-server/tree/main/release-notes · https://github.com/rabbitmq/rabbitmq-server/releases
- [R8] "Broker-Side SQL Filtering with RabbitMQ Streams" (2025-09-23): https://www.rabbitmq.com/blog/2025/09/23/sql-filter-expressions
- [R9] "Delivery Optimization for RabbitMQ Streams" (2025-09-26): https://www.rabbitmq.com/blog/2025/09/26/stream-delivery-optimization
- [R10] "RabbitMQ 3.11 Feature Preview: Super Streams" (2022-07-13): https://www.rabbitmq.com/blog/2022/07/13/rabbitmq-3-11-feature-preview-super-streams
- [R11] RabbitMQ vs Apache Kafka (vendor compare page): https://www.rabbitmq.com/docs/compare/kafka
- [R12] Tanzu, "RabbitMQ vs Kafka: How to Choose an Event-Streaming Broker" (2022-12-19): https://blogs.vmware.com/tanzu/rabbitmq-event-streaming-broker/
- [R13] crazy-goat/rabbit-stream (v1.4.0, 2026-09-21): https://github.com/crazy-goat/rabbit-stream
- [R14] MQ Summit 2025, Simon Unge, "Breaking Storage Barriers": https://mqsummit.com/talks/breaking-storage-barriers/ · https://www.youtube.com/watch?v=CimsLxLmnOo
