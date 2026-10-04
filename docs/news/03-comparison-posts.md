# RabbitMQ vs Kafka: curated comparison posts

As of 2026-10-04. Versions pinned: RabbitMQ 4.3 / Kafka 4.3. Excludes rabbitmq.com/docs/compare/kafka (covered in `../01-source-summary.md`).

**Baseline used to judge "outdated?"**
- RabbitMQ: streams since 3.9 (2021), super streams since 3.11, mirrored classic queues removed in 4.0, quorum-queue `delivery-limit` default 20 since 4.0. 4.3 (2026-04-23) added 32 strict priorities, delayed retry with backoff, and consumer timeouts to quorum queues, and made Khepri the only metadata store.
- Kafka: ZooKeeper removed in 4.0 (2025-03-18). Share groups (KIP-932) went early access in 4.0, preview in 4.1, GA in 4.2 (2026-02-17). Share groups have no DLQ yet: KIP-1191 is accepted and targets 4.4. librdkafka 2.15.0 (2026-06-30) shipped a share consumer in **preview** only.

## Top 5 to read first

| # | Post | Why |
|---|---|---|
| 1 | Kafka Queues (KIP-932) vs RabbitMQ vs SQS (2026-09) | Most version-precise post found (Kafka 4.3, RabbitMQ 4.3). It answers "can we delete RabbitMQ now?" with specifics: DLQ, ordering, delivery limits. |
| 2 | Vercel: RabbitMQ vs Kafka (2026-08) | Debunks the 2020 "15x" benchmark, covers 4.3 quorum-queue features, and gives a one-question decision rule. |
| 3 | Gunnar Morling: KIP-932 (2025-03) | Clearest explanation of share-group semantics and gaps. Its status line is dated, but the gaps it lists still apply. |
| 4 | Factor House: difference + performance (2026-08) | Balanced and covers share groups and quorum queues. Says outright that "no neutral like-for-like benchmark exists". |
| 5 | Eran Stiller (2023) + HN thread, dated | The classic most of the audience has read. Use it as the "before" picture the talk updates. |

## Master table

| # | Title | Publisher | Date | Bias | Rating | Link |
|---|---|---|---|---|---|---|
| 1 | Kafka Queues (KIP-932) vs RabbitMQ vs SQS: Picking a Task Queue After Kafka 4.2 | iotdigitaltwinplm.com (Riju) | 2026-09-20 | none sold; obscure site | ★★★★★ | [link](https://iotdigitaltwinplm.com/kafka-queues-vs-rabbitmq-vs-sqs-task-queues-2026/) |
| 2 | How should teams shipping on serverless choose between RabbitMQ vs Kafka | Vercel | 2026-08-19 (upd 09-01) | sells Vercel Queues/Workflows | ★★★★ | [link](https://vercel.com/i/rabbitmq-vs-kafka) |
| 3 | Let's Take a Look at... KIP-932: Queues for Kafka! | Gunnar Morling (blog) | 2025-03-05 | Kafka-ecosystem engineer | ★★★★★ | [link](https://www.morling.dev/blog/kip-932-queues-for-kafka/) |
| 4 | The difference between Kafka and RabbitMQ / Kafka vs RabbitMQ performance | Factor House (Chad Harris) | 2026-08-29 (upd 10-01) | sells Kafka tooling (Kpow) | ★★★★ | [diff](https://factorhouse.io/resources/kafka/brokers/difference-between-kafka-and-rabbitmq/) · [perf](https://factorhouse.io/resources/kafka/troubleshooting/kafka-vs-rabbitmq-performance/) |
| 5 | RabbitMQ vs Kafka: Which Platform Should You Choose in 2023? | Eran Stiller | 2023-10-18 (dated) | independent | ★★★★ | [link](https://eranstiller.com/rabbitmq-vs-kafka) · [HN](https://news.ycombinator.com/item?id=37574552) |
| 6 | Is Kafka a Message Queue? | Redisson | 2026-08-03 | sells Redis/Valkey queue | ★★★★ | [link](https://redisson.pro/blog/is-kafka-a-message-queue.html) |
| 7 | Apache Kafka Share Groups are NOT true queues | Aiven (Olena Babenko) | 2026-07-02 | sells managed Kafka | ★★★ | [link](https://aiven.io/blog/apache-kafka-share-groups-are-not-true-queues) |
| 8 | Kafka vs RabbitMQ: Streams vs Queues / Kafka Queues: Share Groups Explained | Conduktor (Stéphane Derosiaux) | shows 2026-09-28 (likely site refresh) | sells Kafka tooling | ★★★ | [KvR](https://www.conduktor.io/glossary/kafka-vs-rabbitmq) · [SG](https://www.conduktor.io/glossary/kafka-share-groups) |
| 9 | When (Not) to Use Queues for Kafka? | Kai Waehner | 2026-01-28 | then Confluent Field CTO | ★★★ | [link](https://www.kai-waehner.de/blog/2026/01/28/when-to-use-queues-for-kafka/) |
| 10 | Queues for Kafka Is Here: Your Guide to Getting Started in Confluent | Confluent (Jonathan Lacefield) | 2026-03-03 | sells Kafka (IBM-owned) | ★★★ | [link](https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/) |
| 11 | Talks: "Queues for Kafka" (Current London) + AWS re:Invent OPN413 | Confluent / AWS | 2025-05 / 2025-12-03 | Kafka vendors | ★★★ | [Current](https://current.confluent.io/post-conference-videos-2025/queues-for-kafka-lnd25) · [OPN413](https://www.youtube.com/watch?v=zbN1VeenxY4) |
| 12 | What's the Difference Between Kafka and RabbitMQ? | AWS | date unknown | sells both (MSK, Amazon MQ) | ★★ | [link](https://aws.amazon.com/compare/the-difference-between-rabbitmq-and-kafka/) |
| 13 | A Comparison of RabbitMQ vs Apache Kafka and When to Use Each | Confluent | date unknown | sells Kafka | ★ | [link](https://www.confluent.io/compare/rabbitmq-vs-apache-kafka/) |
| 14 | What Is Apache Kafka? (Kafka vs RabbitMQ section) | IBM Think | 2025-04-30 (upd 2026-06-22) | owns Confluent; sells Event Streams, IBM MQ | ★★ | [link](https://www.ibm.com/think/topics/apache-kafka) |
| 15 | RabbitMQ vs. Kafka: Understanding the differences | Redpanda | 2025-11-04 (last mod.) | sells Kafka-compatible broker | ★★ | [link](https://www.redpanda.com/guides/kafka-tutorial-rabbitmq-vs-kafka) |
| 16 | When to use RabbitMQ or Apache Kafka | CloudAMQP (Lovisa Johansson) | 2025-02-14 (upd) | sells hosted RabbitMQ | ★★★ | [link](https://www.cloudamqp.com/blog/when-to-use-rabbitmq-or-apache-kafka.html) |
| 17 | EP203: RabbitMQ vs Kafka vs Pulsar | ByteByteGo | 2026-02-21 | none (education) | ★★ | [link](https://blog.bytebytego.com/p/ep203-rabbitmq-vs-kafka-vs-pulsar) |
| 18 | Kafka vs RabbitMQ: Key Differences & When to Use Each | DataCamp (Josep Ferrer) | 2025-02-11 | none (education) | ★★ | [link](https://www.datacamp.com/blog/kafka-vs-rabbitmq) |
| 19 | Event-Driven Architecture: Kafka vs. RabbitMQ vs. Pulsar, A 2025 Decision Framework | Java Code Geeks (E. Drosopoulou) | 2025-12-09 | none | ★★★ | [link](https://www.javacodegeeks.com/2025/12/event-driven-architecture-kafka-vs-rabbitmq-vs-pulsar-a-2025-decision-framework.html) |
| 20 | RabbitMQ vs Kafka vs Amazon SQS (2026) | DanubeData (Adrian Silaghi) | 2026-04-01 | sells hosted RabbitMQ | ★★★ | [link](https://danubedata.ro/blog/rabbitmq-vs-kafka-vs-sqs-comparison-2026) |
| 21 | Kafka vs RabbitMQ: Which Should You Pick in 2026? | Fastero | 2026-08-30 | sells BI (neutral on brokers) | ★★ | [link](https://fastero.com/blog/kafka-vs-rabbitmq-streaming-vs-messaging) |
| 22 | RabbitMQ vs Kafka 2026: 1M vs 50K msg/sec and a 16x Gap [Tested] | Tech Insider ("Sofia Lindström") | 2026-04-28 (upd 08-17) | ad-driven SEO site | ★ | [link](https://tech-insider.org/rabbitmq-vs-kafka-2026/) |
| 23 | Benchmarking Apache Pulsar, Kafka, and RabbitMQ | Confluent (Nikhil, Chandar) | 2020-08-21 (dated) | sells Kafka | ★★ | [link](https://www.confluent.io/blog/kafka-fastest-messaging-system/) |
| 24 | RabbitMQ vs Kafka series (6 parts) | Jack Vanlightly | 2017-12 to 2018-09 (dated) | independent at the time | ★★★★ | [link](https://jack-vanlightly.com/blog/2017/12/3/rabbitmq-vs-kafka-series-introduction) |
| 25 | Ask HN: What's your go-to message queue in 2025? | Hacker News | 2025-05-18 | community | ★★★ | [link](https://news.ycombinator.com/item?id=43993982) |

---

## 1. Kafka Queues (KIP-932) vs RabbitMQ vs SQS (iotdigitaltwinplm.com)
- Claims:
  - Share groups decouple consumer count from partition count, but give no per-key ordering. Quotes KIP-932: records "can be delivered out of order… in particular when redeliveries occur".
  - The deciding gap is the DLQ. When a record exceeds `share.delivery.count.limit` (default 5) it goes to Archived with "nothing written anywhere". KIP-1191 is accepted but unshipped and gated behind `share.version=2`.
  - RabbitMQ quorum queues: `delivery-limit` defaults to 20 since 4.0 and DLX is mature. FIFO order breaks with redelivery or prefetch > 1, and Single Active Consumer restores strict order. 4.3 adds `x-delayed-retry-type`.
  - If you already run Kafka, a queue costs almost nothing extra. Otherwise "standing up a Kafka cluster to get a work queue is a poor trade".
- Outdated? No. It cites Kafka 4.3 and RabbitMQ 4.3.
- Verdict ★5: best current post found. The author and site are obscure, so cite the KIPs and docs it links rather than the post itself.

## 2. Vercel: RabbitMQ vs Kafka
- Claims:
  - The most-quoted gap was "measured in 2020 against RabbitMQ 3.8.5 with persistence disabled, in a mirrored-queue mode removed in RabbitMQ 4.0".
  - Streams (3.9) and share groups (4.2) erased the old "stream vs queue" capability split.
  - 4.3 quorum queues have 32 strict priorities and delayed retries with linear backoff. Khepri migration is complete.
  - Streams only reach full speed over the stream protocol (Java/Go clients) and run slower over AMQP. This matters for PHP.
  - Decision rule: "Does a message still have value after the first consumer reads it, and how many independent consumers need it?" A single RabbitMQ node is a legitimate production setup, while Kafka needs more infrastructure to start.
- Outdated? No.
- Verdict ★4: accurate and current. Skip the closing Vercel Queues pitch.

## 3. Gunnar Morling: KIP-932
- Claims:
  - Several share-group members can read one partition. Records are acked one by one with ACCEPT/RELEASE/REJECT, and state lives in `__share_group_state`.
  - "Messages with higher offsets may be consumed before messages with lower offsets."
  - Missing: DLQ, delayed or backoff retries, key-based ordering, and non-Java clients.
  - He doubts it will make Artemis or RabbitMQ users migrate.
- Outdated? Partly. It was written for 4.0 early access, and GA came in 4.2 with RENEW acks added. DLQ, backoff and ordering gaps are still true in 4.3.
- Verdict ★5: best explanation of the semantics. Update only the status line.

## 4. Factor House: difference + performance
- Claims:
  - Kafka is an append-only log. RabbitMQ queues delete on ack, while streams give non-destructive reads.
  - "RabbitMQ parallelism is worker-bound: any number of competing consumers on one queue." Share groups narrow this gap.
  - "Pick Kafka when the workload is a stream… Pick RabbitMQ when the workload is a task."
  - Performance: "no neutral like-for-like benchmark exists". Kafka handles small messages well and large ones poorly. Quorum-queue Raft adds latency. Scorecard: Kafka 39/60, RabbitMQ 37/60.
- Outdated? No.
- Verdict ★4: balanced and version-aware. The scores are subjective.

## 5. Eran Stiller (2023) + HN thread (2023-09-19, 292 pts / 166 comments)
- Claims:
  - Kafka orders within a partition. RabbitMQ gives weak ordering with competing consumers.
  - RabbitMQ routes by exchange rules. Kafka has no broker-side filtering.
  - RabbitMQ has TTL and delayed messages, Kafka does not. Kafka scales out, RabbitMQ scales up.
  - HN: "smart messaging, dumb clients" vs the reverse. Partition count caps read concurrency. Commenters already raised streams.
- Outdated? Yes. No streams, quorum queues or share groups, and "Kafka can't do competing consumers" is no longer true.
- Verdict ★4 (dated): the canonical "before" framing, worth a contrast slide.

## 6. Redisson: Is Kafka a Message Queue?
- Claims:
  - "Kafka is a log that can now also behave like a queue."
  - No DLQ in 4.2: exhausted records are archived. KIP-1191 was voted in January 2026 and is slated for 4.4.
  - Share groups have no priority and no delayed delivery.
  - Share groups "now cover much of what teams use RabbitMQ for". RabbitMQ still leads on routing and failure handling, and 4.3 added strict quorum-queue priority and delayed retries.
- Outdated? No. Its 4.3 claims match the RabbitMQ 4.3 highlights.
- Verdict ★4: crisp list of gaps. Ignore the Redis pitch.

## 7. Aiven: Share Groups are NOT true queues
- Claims:
  - Share groups give "elastic scaling & per-message control, without strict ordering".
  - Per-message acks and poison-pill handling, which you monitor via `deliveryCount() > 1`.
  - Defaults need tuning: "only 4 clients can read messages efficiently during high load".
  - "Only a Java Share Group client is supported."
- Outdated? Partly. The Java-only claim aged immediately: librdkafka 2.15.0 (2026-06-30) shipped a preview share consumer, not yet meant for production.
- Verdict ★3: useful tuning gotcha, but specific to Aiven's platform.

## 8. Conduktor: two pages
- Claims:
  - "Kafka is a log you can re-read; RabbitMQ is a queue that empties."
  - KvR page: a task queue on Kafka needs application logic that RabbitMQ provides out of the box.
  - Share-groups page: "not a drop-in RabbitMQ or ActiveMQ Artemis replacement". There's no DLQ and no delayed/backoff retry, and it keeps log retention rather than draining like a queue.
  - "No ordering guarantee across batches, across members, or across redeliveries."
- Outdated? The KvR page partly: it never mentions share groups or quorum queues even though the vendor's own glossary does. The share-groups page is current.
- Verdict ★3: read the share-groups page, skip the KvR page.

## 9. Kai Waehner: When (Not) to Use Queues for Kafka?
- Claims:
  - Queues for Kafka turns Kafka into a "cloud-native integration backbone" that merges streaming and messaging.
  - Don't use it for strict ordering, exactly-once/transactions, request/reply, legacy protocols (JMS/AMQP/MQTT), or analytics.
  - It complements IBM MQ, TIBCO and RabbitMQ rather than replacing them.
- Outdated? No. It was written 3 weeks before 4.2 shipped. The author left Confluent in June 2026 after the IBM acquisition.
- Verdict ★3: honest "when not" list from the Kafka camp, but it skips the DLQ gap.

## 10. Confluent: Queues for Kafka GA guide
- Claims:
  - Orgs run two systems (Kafka plus a traditional MQ), and Queues for Kafka consolidates them.
  - The broker locks acquired records for 30 s by default, and consumers scale beyond the partition count.
  - Roadmap: DLQ via KIP-1191 targeting 4.4, non-Java clients in 2026, and later key-based ordering, exactly-once and exponential backoff.
- Also: InfoWorld "How Apache Kafka flexed to support queues" (Sandon Jacobs, Confluent, 2026-03-31) is the same pitch: consolidate onto Kafka at the cost of order.
- Outdated? No.
- Verdict ★3: a vendor pitch, but the roadmap list honestly admits what's missing.

## 11. Talks: Current London 2025 "Queues for Kafka" (Andrew Schofield, KIP author) + re:Invent 2025 OPN413
- Claims:
  - Per-message acks and load balancing on ordinary topics, with no need to match partitions to consumers.
  - OPN413 demo: 3 consumers on 2 partitions. It names head-of-line blocking and over-partitioning as the motivation.
  - OPN413 (Dec 2025) said 4.1 was preview with no DLQ and no share-group lag metrics yet.
- Outdated? Status is outdated: early access/preview then, GA now. The DLQ is still missing.
- Verdict ★3: best video intros. Neither compares with RabbitMQ directly.

## 12. AWS: Difference between Kafka and RabbitMQ
- Claims:
  - RabbitMQ pushes and supports priority queues. Kafka has no priority.
  - RabbitMQ deletes a message on ack. Kafka's sequential disk I/O gives "millions of messages per second".
  - Calls RabbitMQ a broker that "collects streaming data" (muddled). Mentions KRaft.
- Outdated? Partly: no streams, quorum queues or share groups.
- Verdict ★2: fine 101-level table, but the worldview predates streams. AWS is the least partisan big vendor because it sells both.

## 13. Confluent: RabbitMQ vs Kafka compare page
- Claims:
  - "RabbitMQ messages are stored in memory, while Kafka messages are stored on disk."
  - "Kafka can handle billions of messages per second vs. RabbitMQ's millions."
  - RabbitMQ is "designed for quick message publishing and deletion", and Kafka is more scalable.
- Outdated? Yes. Quorum queues and streams are disk-based. The page doesn't mention share groups, Confluent's own GA feature.
- Verdict ★1: a source of myths. Quote it only to debunk.

## 14. IBM Think: What Is Apache Kafka?
- Claims: the table says Kafka is a "distributed log system" and RabbitMQ a "message queue broker". RabbitMQ gets "single-consumer message delivery" and "ephemeral (deleted after consumption)" storage. Kafka is for streaming, RabbitMQ for routing and low latency.
- Outdated? Yes. "Single-consumer" is wrong even for old RabbitMQ (fanout exchanges), and streams are ignored.
- Verdict ★2. Note that IBM owns Confluent since 2026-03-17.

## 15. Redpanda: RabbitMQ vs. Kafka guide
- Claims:
  - Kafka does "1 million messages per second" vs RabbitMQ "4K–10K".
  - Smart broker/dumb consumer vs the inverse.
  - RabbitMQ for low latency, Kafka for throughput. Delayed messages need a RabbitMQ plugin.
- Outdated? Yes: no streams, quorum queues or share groups.
- Verdict ★2: SEO page and a main carrier of the 4K–10K myth.

## 16. CloudAMQP: When to use RabbitMQ or Apache Kafka
- Claims:
  - Streams are an immutable append-only log with replay.
  - "Kafka is built from the ground up with horizontal scaling… RabbitMQ is mostly designed for vertical scaling."
  - Log compaction "does not exist in RabbitMQ".
  - Most workloads fit either broker, and RabbitMQ is better for complex routing.
- Outdated? Partly: no share groups, and the vertical-scaling framing ignores super streams.
- Verdict ★3: a fair RabbitMQ-vendor take that admits where Kafka wins (compaction).

## 17. ByteByteGo: EP203
- Claims:
  - RabbitMQ: "messages are pushed, acknowledged, and then gone."
  - Kafka is "not a queue, it's a distributed log" with offset replay. Pulsar mixes both via BookKeeper.
  - Choose by "how long it should live, and how many times it needs to be read."
- Outdated? Yes, despite being published 4 days after 4.2: it ignores streams and share groups.
- Verdict ★2: nice diagram, but it's the framing this talk retires.

## 18. DataCamp
- Claims:
  - Kafka does millions of messages per second vs RabbitMQ "4K-10K".
  - Kafka pulls, RabbitMQ pushes. RabbitMQ discards on consumption and has low latency.
  - Briefly mentions streams and quorum queues.
- Outdated? Partly. It predates share groups and repeats the 4K–10K number.
- Verdict ★2.

## 19. Java Code Geeks: 2025 Decision Framework
- Claims:
  - KRaft lowers Kafka's barrier to entry.
  - RabbitMQ "pushes Quorum Queues… as the default for high availability".
  - RabbitMQ is sub-ms for routed transactions but degrades when queues back up. Kafka offers exactly-once via transactional producers.
- Outdated? Partly: no streams, no share groups.
- Verdict ★3: sensible snapshot with no numbers to misuse.

## 20. DanubeData: RabbitMQ vs Kafka vs SQS
- Claims:
  - RabbitMQ sub-ms vs Kafka 5–15 ms batched.
  - SQS at 10k msg/s costs "$3,120–$31,200/month" vs self-hosted RabbitMQ at "$520–$980".
  - Exchanges route without application code. Streams are "less mature than Kafka".
- Outdated? Partly. No share groups six weeks after GA, and "KRaft replaced ZooKeeper in 2024" is wrong (ZooKeeper was removed in 4.0, March 2025).
- Verdict ★3: useful SQS cost angle, but the numbers come from a vendor.

## 21. Fastero: Which Should You Pick in 2026?
- Claims:
  - Kafka 500K–1M msg/s on 3 brokers vs RabbitMQ 20–30K on one node.
  - RabbitMQ Streams offer "limited log semantics, not full replay".
  - "KRaft (default since Kafka 3.4)".
- Outdated? Wrong in places. Streams replay from an offset or timestamp. KRaft became production-ready in 3.3 and mandatory in 4.0. No share groups as of August 2026.
- Verdict ★2: 2026 title, 2022 content.

## 22. Tech Insider: "16x Gap [Tested]" (sibling: "1M msgs/sec vs 40K")
- Claims:
  - Kafka 4.1 does ~1M msg/s per broker vs RabbitMQ 4.1 classic queues at ~50K.
  - Cites "Confluent's 2024 study": 605,000 vs 38,000 msg/s with "classic mirrored queues".
  - Includes quotes attributed to Gwen Shapira, ThePrimeagen, Fireship, Jay Kreps and a "Broadcom principal engineer".
- Outdated? Wrong. The 605/38 figures are **MB/s** from Confluent's **2020** benchmark on RabbitMQ 3.8.5. Mirrored queues were removed in 4.0. No quote links to a source, and no Shapira "InfoQ essay" could be found.
- Verdict ★1: likely AI-generated. It ranks high in search, so prepare the rebuttal.

## 23. Confluent 2020 benchmark
- Claims:
  - Peak throughput: Kafka 605 MB/s vs RabbitMQ 38 MB/s.
  - p99 latency: Kafka 5 ms at 200 MB/s vs RabbitMQ 1 ms, but only up to 30 MB/s.
  - The authors themselves note that mirroring hurt RabbitMQ and that classic queues without mirroring do better.
- Outdated? Yes. It tested RabbitMQ 3.8.5 with mirrored classic queues (removed in 4.0), no quorum queues or streams, and persistence off.
- Verdict ★2: origin of the "15x" number. Cite it only with these caveats.

## 24. Jack Vanlightly: RabbitMQ vs Kafka series
- Claims:
  - "Most of us don't deal with a scale where RabbitMQ has problems."
  - RabbitMQ is about moving messages with routing primitives. Kafka is about "storing the current and historical state of a system".
  - Both offer at-most-once and at-least-once. Kafka's exactly-once works only "in a very limited scenario". Ordering is per queue vs per partition, and Kafka has no built-in delay.
- Outdated? Yes: mirrored-queue and ZooKeeper era. The semantics parts (1 and 4) still hold. The author later joined the RabbitMQ core team.
- Verdict ★4 (dated): deepest semantics comparison written.

## 25. HN: Ask HN, go-to message queue 2025
- Claims:
  - RabbitMQ "doesn't require babysitting". Recent versions are a "decent alternative to Kafka if you don't need to scale to the moon".
  - There's a strong Postgres-as-queue camp, plus NATS.
  - Kafka is for replay and fan-out. "Kafka hate" comes from using it as a plain queue.
- Outdated? No. It reflects practitioner sentiment, but KIP-932 never comes up.
- Verdict ★3: practitioner view that RabbitMQ is the boring default.

---

## Consensus across posts
- **Log vs queue** is the core mental model: replay and retention on one side, ack-then-delete on the other. Nearly every post opens with it.
- **RabbitMQ wins on:**
  - broker-side routing (exchanges)
  - per-message features: TTL, priority, DLX, delayed retry
  - latency at low or moderate load
  - simpler small deployments (one node is legitimate)
  - multi-protocol support (AMQP 0-9-1/1.0, MQTT, STOMP)
- **Kafka wins on:**
  - sustained throughput at scale
  - long retention and replay
  - fan-out to many independent consumers
  - ecosystem: Connect, Streams, Debezium CDC, Flink, compaction
- **Most teams don't need Kafka scale** (Vanlightly 2017, CloudAMQP, HN 2023/2025).
- **Running both is common**: Kafka as the event backbone, RabbitMQ for task dispatch.
- **Share groups (4.2) fix "consumers ≤ partitions"** but trade away ordering. As of 4.3 they still have no DLQ, delay or priority, and non-Java clients are preview at best. Every 2026 post that covers them agrees on this.

## Where posts disagree / common myths

| Myth / disagreement | Seen in | Reality (2026-10) |
|---|---|---|
| "RabbitMQ does 4K–10K msg/s" | #15, #18, Simplilearn | Number of unclear origin. Even 2026 SEO posts cite 20–50K per node for classic queues, and streams are far higher. Depends on config. |
| "Kafka is 15–16x faster" | #22, many SEO posts | Comes from #23 (2020, RabbitMQ 3.8.5, mirrored queues, MB/s). #22 restates it as msg/s and as a "2024 study". |
| "RabbitMQ keeps messages in memory" | #13 | Quorum queues (Raft log) and streams are disk-first. Classic queues v2 also page to disk. |
| "RabbitMQ can't replay" / "limited replay" | #12, #14, #17, #21 | Streams since 3.9 replay from first, last, offset or timestamp. |
| "RabbitMQ delivers to a single consumer" | #14 | Fanout/topic exchanges have always copied to many queues. Streams allow many readers. |
| "RabbitMQ only scales vertically" | #5, #13, #16 | Partly true for a single queue (one leader). Super streams (3.11) partition, and clusters spread queue leaders. |
| "Kafka can't do competing consumers beyond partitions" | #5, HN 2023, #24 | Fixed by share groups GA in 4.2, at the cost of ordering. |
| Share groups replace RabbitMQ? | Yes-ish: #9, #10, InfoWorld. No: #1, #3, #6, #8, rabbitmq.com | Enough for simple unordered work queues on an existing Kafka. Not for DLQ, delay, priority, routing or ordering needs. |
| Latency: RabbitMQ lower? | Most say yes. Modal (2024) says Kafka is "consistent" under load. | Both are right at different load points. RabbitMQ is sub-ms at moderate load and degrades when queues back up. |
| "Share groups are Java-only" | #7 | Java is GA. librdkafka 2.15.0 has a preview share consumer (2026-06-30), not for production. |
| "Kafka needs ZooKeeper" / "KRaft default since 3.4/2024" | older posts, #20, #21 | KRaft production-ready in 3.3. ZooKeeper removed in 4.0 (2025-03-18). |
| Expert quotes (Shapira "the boring answer…", ThePrimeagen, Fireship) | #22 | No primary source found. Don't put them on slides. |

## Gap for a PHP audience
- None of the 25 posts mentions PHP. Client maturity and the process model are covered in `../02-related-research.md`.
- PHP share-consumer status:
  - librdkafka 2.15.0 has a preview share consumer (verified via GitHub release).
  - No php-rdkafka binding for it was found in search.
  - A pure-PHP `KafkaShareConsumer` exists in lisachenko/kafka-client PR #235 (search result only, unverified).
- Vercel (#2) notes streams run fastest over the stream protocol with Java/Go clients. That supports the "no official PHP stream client" point.

## Also seen, not included
- Not found:
  - Microsoft: no direct article. Azure Learn compares only Service Bus, Event Hubs and Event Grid.
  - Google Cloud: none.
  - Baeldung: `baeldung.com/java-rabbitmq-vs-kafka` returned 403, unverified.
  - DZone: only 2016–2019 pieces.
  - InfoQ: only July 2024 KIP-932 news.
  - Reddit: no threads surfaced.
  - The New Stack: "Choosing Between Message Queues and Event Streams" (~2023), fetch failed.
- Skipped as dated or thin:
  - Instaclustr (2021-05-09)
  - Modal (2024-09-25)
  - VMware Tanzu, Gregory Green (2022-12-19, Broadcom/RabbitMQ vendor)
  - SoftwareMill KIP-932 preview (2024-05-08)
  - HN "KIP-932: Queues for Kafka" (2023)
  - StackShare, Simplilearn, ProjectPro (SEO, contain the exactly-once and ZooKeeper errors)
  - Hussein Nasser RabbitMQ course with a Kafka segment
- Upcoming: MQ Summit 2026 (Oct 21–22, Haarlem), Viktor Gamov "From Queues to Logs and Back Again" (search result only).

## Sources
- https://iotdigitaltwinplm.com/kafka-queues-vs-rabbitmq-vs-sqs-task-queues-2026/
- https://vercel.com/i/rabbitmq-vs-kafka
- https://www.morling.dev/blog/kip-932-queues-for-kafka/
- https://factorhouse.io/resources/kafka/brokers/difference-between-kafka-and-rabbitmq/
- https://factorhouse.io/resources/kafka/troubleshooting/kafka-vs-rabbitmq-performance/
- https://eranstiller.com/rabbitmq-vs-kafka
- https://news.ycombinator.com/item?id=37574552
- https://redisson.pro/blog/is-kafka-a-message-queue.html
- https://aiven.io/blog/apache-kafka-share-groups-are-not-true-queues
- https://www.conduktor.io/glossary/kafka-vs-rabbitmq
- https://www.conduktor.io/glossary/kafka-share-groups
- https://www.kai-waehner.de/blog/2026/01/28/when-to-use-queues-for-kafka/
- https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/
- https://www.infoworld.com/article/4143957/how-apache-kafka-flexed-to-support-queues.html
- https://current.confluent.io/post-conference-videos-2025/queues-for-kafka-lnd25
- https://www.youtube.com/watch?v=zbN1VeenxY4
- https://zenn.dev/kiiwami/articles/c9c520f3fc121d2b?locale=en
- https://aws.amazon.com/compare/the-difference-between-rabbitmq-and-kafka/
- https://www.confluent.io/compare/rabbitmq-vs-apache-kafka/
- https://www.ibm.com/think/topics/apache-kafka
- https://www.confluent.io/press-release/ibm-completes-acquisition-of-confluent/
- https://www.redpanda.com/guides/kafka-tutorial-rabbitmq-vs-kafka
- https://www.cloudamqp.com/blog/when-to-use-rabbitmq-or-apache-kafka.html
- https://blog.bytebytego.com/p/ep203-rabbitmq-vs-kafka-vs-pulsar
- https://www.datacamp.com/blog/kafka-vs-rabbitmq
- https://www.javacodegeeks.com/2025/12/event-driven-architecture-kafka-vs-rabbitmq-vs-pulsar-a-2025-decision-framework.html
- https://danubedata.ro/blog/rabbitmq-vs-kafka-vs-sqs-comparison-2026
- https://fastero.com/blog/kafka-vs-rabbitmq-streaming-vs-messaging
- https://tech-insider.org/rabbitmq-vs-kafka-2026/
- https://www.confluent.io/blog/kafka-fastest-messaging-system/
- https://jack-vanlightly.com/blog/2017/12/3/rabbitmq-vs-kafka-series-introduction
- https://news.ycombinator.com/item?id=43993982
- https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release
- https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/
- https://github.com/confluentinc/librdkafka/releases/tag/v2.15.0
- https://modal.com/blog/rabbitmq-vs-kafka-article
- https://www.instaclustr.com/blog/rabbitmq-vs-kafka/
- https://blogs.vmware.com/tanzu/rabbitmq-event-streaming-broker/
- https://softwaremill.com/kafka-queues-now-and-in-the-future/
