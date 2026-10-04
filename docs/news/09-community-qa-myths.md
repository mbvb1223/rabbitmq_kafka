# Community debate & Q&A prep — RabbitMQ vs Kafka

As of 2026-10-04. Pinned to RabbitMQ 4.3 / Kafka 4.3. This extends the Risks table in [03-topic-proposal.md](../03-topic-proposal.md#risks--things-to-prepare-for). `(↔03)` marks answers that back up a point already in the proposal.

## TL;DR

- **The audience's 8 most likely questions:** which one to pick, "isn't Kafka faster", "Kafka has queues now", exactly-once, ordering, idle Kafka consumers, retries/DLQ, and "why not just Postgres/SQS". All are answered below with official-doc sources.
- **The biggest 2025-26 community fight wasn't RabbitMQ vs Kafka.** It was "do you need a broker at all": Kozlovski's "Kafka is Fast – I'll use Postgres" (HN, 561 points, Oct 2025) vs Morling's "…Considered Harmful" (Nov 2025). Expect someone in the room to bring it up.
- **Prepare hardest for three questions:**
  1. The fsync challenge (Q10). Kafka people will cite Vanlightly's "Kafka doesn't need fsync to be safe".
  2. "Share groups make RabbitMQ obsolete" (Q4).
  3. Exactly-once (Q6).
- **PHP traps the room won't expect:** librdkafka (under php-rdkafka) ships with **idempotence off**, and with **automatic offset store + auto commit**, it commits messages *before* you have processed them. php-amqplib can't send heartbeats while your job runs.
- **Coverage gap:** Reddit blocks our crawler, and YouTube comments can't be retrieved. Instead I mined HN (via the Algolia API), Lobsters, engineers' blogs, Bluesky/LinkedIn snippets, and GitHub issues.

## Likely audience questions (with answers)

**Q1. "So which one should we pick?"**
Decide by the shape of the workload, not by brand.
- Work items that need retry, TTL, priority or runtime routing → RabbitMQ.
- A replayable log that many teams read, compaction, CDC, or Flink/Iceberg → Kafka.

The community one-liner: *"One is smart messaging; dumb clients. The other is dumb messaging; smart clients."* ([HN, Sep 2023](https://news.ycombinator.com/item?id=37574552)). Below a few MB/s, "neither yet, use Postgres" is a respectable answer (see Hot takes).

**Q2. "Isn't Kafka just way faster?"** (↔03)
The headline gap (Confluent 2020: 605 vs 38 MB/s) measured RabbitMQ *mirrored* queues, which were [removed in 4.0](https://github.com/rabbitmq/rabbitmq-server/blob/main/release-notes/4.0.1.md). RabbitMQ streams over the native protocol are in Kafka's league. Over AMQP, which is what PHP uses, expect hundreds of thousands of msg/s. Per [RabbitMQ's docs](https://www.rabbitmq.com/docs/queues), "a single queue replica … is limited to a single CPU core", so scale out with more queues. In a PHP shop, the workers are the bottleneck long before the broker is.

**Q3. "Doesn't Kafka still need ZooKeeper?"**
No. [Kafka 4.0 (2025-03-18)](https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/) removed it, and KRaft is the only mode. Upgrades must migrate on 3.9 first. RabbitMQ did the same: Khepri (Raft) is its only metadata store in 4.3.

**Q4. "Kafka has queues now. Doesn't that kill RabbitMQ?"** (↔03)
Share groups went GA in [4.2 (2026-02-17)](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/). They give per-record ack, delivery count (default 5), 30 s record locks, and no partition cap on consumers.
- **Not covered:** ordering, EOS, TTL, priority and delay. DLQ arrives in 4.4 (KIP-1191).
- **Even the vendor says so:** Confluent's Kai Waehner [lists "strict ordering / EOS / request-reply" as when *not* to use them](https://www.kai-waehner.de/blog/2026/01/28/when-to-use-queues-for-kafka/) (Jan 2026).
- **From PHP: no.** php-rdkafka has no binding and the librdkafka share consumer is still Preview. Confluent targets non-Java clients for [H2 2026](https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/).

**Q5. "Can RabbitMQ replay events like Kafka?"** (↔03)
- **RabbitMQ:** yes, with [streams](https://www.rabbitmq.com/docs/streams) (since 3.9, 2021). Set `x-stream-offset` to `first`/`last`/`next`/an offset/a timestamp/an interval like `1D`. Classic and quorum queues can't replay, because ack deletes the message.
- **Kafka:** replay means resetting group offsets (`kafka-consumer-groups.sh --reset-offsets --to-datetime …`). It only works within retention, which defaults to [168 h](https://kafka.apache.org/43/configuration/broker-configs/).

**Q6. "Does Kafka give exactly-once?"**
Only inside Kafka. The idempotent producer plus transactions plus `read_committed` consumers (or Kafka Streams) give EOS for read-process-write between topics. [Kafka's docs](https://kafka.apache.org/43/design/design/) say "Kafka guarantees at-least-once delivery by default". For an external system you must "coordinate the consumer's position with what is actually stored as output". So once you write to MySQL or send an email, you need idempotency. Share groups have no EOS at all. RabbitMQ is [at-least-once with confirms + acks](https://www.rabbitmq.com/docs/reliability).

**Q7. "What about ordering?"** (↔03)
- **Kafka:** ordered per partition, which means per key.
- **RabbitMQ:** FIFO per queue only with a [single active consumer](https://www.rabbitmq.com/docs/consumers), no requeues, and no priorities. Quorum queues always have [priorities on](https://www.rabbitmq.com/docs/quorum-queues), with a default of 4.
- **Per-key order plus parallelism in RabbitMQ:** use an `x-modulus-hash`/consistent-hash exchange to fan out to N queues, each with a single active consumer.
- **PHP trap:** librdkafka [`message.send.max.retries`](https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md) says "retrying may cause reordering unless `enable.idempotence` is set", and that defaults to **false**.

**Q8. "We have 12 partitions. Why are workers 13–20 idle?"** (↔03)
A consumer group gives each partition to exactly one consumer, so extra consumers sit as standbys ([HN, 2023](https://news.ycombinator.com/item?id=37574552): "max read concurrency is equal to the number of partitions"). Adding partitions has costs: [Kafka "does not currently support reducing the number of partitions"](https://kafka.apache.org/43/operations/basic-kafka-operations/), it remaps `hash(key)`, and it doesn't move existing data. KIP-848 doesn't lift this ceiling; only share groups do.

**Q9. "How do retries and DLQs work on each?"**
- **RabbitMQ:** reject with `requeue=false` sends the message to the DLX. Reasons are `rejected`/`expired`/`maxlen`/`delivery_limit` ([DLX docs](https://www.rabbitmq.com/docs/dlx)). Quorum queues default to delivery-limit 20 (4.0+), and 4.3 adds `x-delayed-retry-*` backoff.
- **Kafka consumer groups:** you build retry topics with increasing delays, then a DLQ topic ([Uber, 2018-02-16](https://www.uber.com/en-US/blog/reliable-reprocessing/)).
- **Built-in Kafka DLQs** exist only in Connect, in Streams ([KIP-1034, 4.2](https://blogit.michelin.io/dead-letter-queue-in-kafka-streams-kip-1034/)), and in share groups (KIP-1191, 4.4).

**Q10. "Will RabbitMQ lose messages? Will Kafka?"** (↔03, the fsync slide)
Both lose messages only if you skip the safety settings.
- **RabbitMQ:** quorum queues + publisher confirms + manual acks. Transient messages are ["discarded during recovery, even if they were stored in durable queues"](https://www.rabbitmq.com/docs/queues).
- **Kafka:** `acks=all` is the default, but `min.insync.replicas` defaults to [1](https://kafka.apache.org/43/configuration/broker-configs/), so set it to 2 with RF=3.
- **Expect pushback on fsync:** Kafka people argue replication + recovery makes skipping fsync safe ([Vanlightly, 2023-04-24](https://jack-vanlightly.com/blog/2023/4/24/why-apache-kafka-doesnt-need-fsync-to-be-safe); [KIP-966](https://jack-vanlightly.com/blog/2023/8/17/kafka-kip-966-fixing-the-last-replica-standing-issue)). Agree that it's safe across AZs, then make the point about correlated failure.

**Q11. "Why not just Postgres / Redis / SQS?"** (↔03)
This is a legitimate question and the loudest thread of the past year.
- **For Postgres:** [Kozlovski](https://topicpartition.io/blog/postgres-pubsub-queue-benchmarks) (2025-10-28) measured 2,885 msg/s on modest hardware for a Postgres queue, which "covers the scale 99% of companies ever hit with a single queue".
- **Against:** [Morling](https://www.morling.dev/blog/you-dont-need-kafka-just-use-postgres-considered-harmful/) (2025-11-03) answers that "building a robust queue on top of Postgres is actually harder than it may sound", so use pgmq.
- **Laravel:** its built-in drivers (DB/Redis/SQS) are fine until you need routing, fan-out or replay.

**Q12. "Our job takes 10 minutes. What breaks?"**
- **RabbitMQ heartbeats:** php-amqplib is single-threaded, so it misses heartbeats while blocked. Two missed beats (default [60 s](https://www.rabbitmq.com/docs/heartbeats)) close the connection, and the message is redelivered while it's still running ([php-amqplib #725](https://github.com/php-amqplib/php-amqplib/issues/725)).
- **RabbitMQ `consumer_timeout`:** defaults to [30 min](https://www.rabbitmq.com/docs/consumers).
- **Kafka:** if you don't call `consume()` within `max.poll.interval.ms` (5 min), the consumer is kicked, the group rebalances, and the message is processed twice.
- **Fixes:** `PCNTLHeartbeatSender` (only helps if the work is interruptible), raise the timeouts, or split the job into smaller ones.

**Q13. "How do we avoid duplicate processing?"**
You can't avoid duplicates; you make them harmless. In practice both brokers are at-least-once. RabbitMQ's docs advise designing the consumer "to be idempotent rather than to explicitly perform deduplication" ([reliability](https://www.rabbitmq.com/docs/reliability)). The usual tool is a processed-message-ID table written in the same DB transaction as the side effect.

**Q14. "How do we publish reliably after a DB commit?"**
Use a transactional outbox: write the event row in the same transaction, then relay it (a poller for RabbitMQ, Debezium/CDC for Kafka). Here both camps agree: "Use PostgreSQL for transactions, use a real queue for messages" ([Lobsters, 2025-10-29](https://lobste.rs/s/oj3ce4/kafka_is_fast_i_ll_use_postgres)).

**Q15. "Laravel / Symfony support?"** (↔03)
- **Laravel 13:** built-in [queue drivers](https://laravel.com/docs/queues) are SQS, Redis, database, Beanstalkd (plus sync/null/failover/deferred/background). There's no RabbitMQ or Kafka; use `vyuldashev/laravel-queue-rabbitmq` or a Kafka package. Horizon ["requires that you use Redis"](https://laravel.com/docs/horizon).
- **Symfony 8.1 Messenger:** the [AMQP transport](https://symfony.com/doc/current/messenger.html) is built in but needs `ext-amqp`. Kafka is only available via third-party transports (Enqueue).

**Q16. "The delayed-message plugin was archived. Are our delayed jobs broken?"** (↔03)
Probably not.
- **Symfony's AMQP transport** ([Connection.php](https://raw.githubusercontent.com/symfony/symfony/8.1/src/Symfony/Component/Messenger/Bridge/Amqp/Transport/Connection.php)) creates per-delay queues with `x-message-ttl` + `x-dead-letter-exchange`.
- **vyuldashev's Laravel driver** ([RabbitMQQueue.php](https://raw.githubusercontent.com/vyuldashev/laravel-queue-rabbitmq/master/src/Queue/RabbitMQQueue.php)) creates `{queue}.delay.{ttl}` queues.
- **Neither uses the plugin.** You're affected only if your own code sets `x-delay` or declares an `x-delayed-message` exchange.

**Q17. "Can Kafka be our event store (event sourcing)?"**
Not on its own. Kafka has no optimistic-concurrency append ("append only if the stream is still at version X"), and loading one aggregate means scanning a partition ([Dudycz, "Event Streaming is not Event Sourcing!"](https://event-driven.io/en/event_streaming_is_not_event_sourcing/)). Keep events in an event store or Postgres, and publish to Kafka through an outbox. RabbitMQ isn't an event store either.

**Q18. "What happens when consumers fall behind?"** (back-pressure)
- **Kafka:** nothing, by design. It's pull-based: "the consumer simply falls behind and catches up when it can" ([design doc](https://kafka.apache.org/43/design/design/)). Lag grows until retention deletes unread data. Producers block when `buffer.memory` (32 MB) is full, for up to `max.block.ms` (60 s).
- **RabbitMQ:** prefetch caps what each consumer holds, and the queue grows. [Memory/disk alarms](https://www.rabbitmq.com/docs/alarms) block publishers, but "connections that only consume … are not blocked". `max-length` + `reject-publish` [nacks publishers](https://www.rabbitmq.com/docs/maxlength).

**Q19. "Is RabbitMQ still open source under Broadcom? Who owns Kafka now?"**
- **RabbitMQ:** still MPL 2.0. Since June 2024, [free community support](https://www.rabbitmq.com/blog/2024/05/31/new-community-support-policy) covers only the latest release series. Old-series patches and some features (e.g. Warm Standby Replication, and now publisher-set delays) are Tanzu-only.
- **Kafka:** Apache 2.0 under the ASF. Confluent, its biggest contributor, has been [IBM's since 2026-03-17](https://newsroom.ibm.com/2026-03-17-ibm-completes-acquisition-of-confluent,-making-real-time-data-the-engine-of-enterprise-ai-and-agents).

**Q20. "What about large messages?"**
RabbitMQ 4.0+ defaults `max_message_size` to [16 MiB](https://github.com/rabbitmq/rabbitmq-server/blob/main/release-notes/4.0.1.md) (it was 128). Kafka's `message.max.bytes` is about [1 MB](https://kafka.apache.org/43/configuration/broker-configs/). The advice is the same for both (the claim-check pattern): put the blob in S3 and send the ID.

**Q21. "Don't Kafka rebalances stop the world?"**
Less so now. KIP-848 (broker-driven, incremental rebalances) went GA in Kafka 4.0. It's opt-in with `group.protocol=consumer` and needs librdkafka ≥ 2.12.0, so php-rdkafka gets it through the C library ([librdkafka config](https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md)). It becomes the default only in Kafka 5.0 (KIP-1274, see [02-kafka-news.md](02-kafka-news.md)). It does **not** raise the partition ceiling from Q8.

**Q22. "Isn't Kafka expensive in the cloud?"**
The hidden cost is cross-AZ replication traffic (RF=3 across 3 AZs). Diskless topics (KIP-1150) were [accepted on 2026-03-02](https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead) but aren't in 4.3; today only vendors sell them (Aiven, Redpanda, AutoMQ, all vendor sources). Bring real MSK / Confluent / CloudAMQP / Amazon MQ prices (↔03 risk table).

## Myths vs facts

| # | Myth (heard in the wild) | Fact | Source |
|---|---|---|---|
| 1 | "Kafka needs ZooKeeper" | Removed in 4.0 (2025-03-18); KRaft only | [Kafka 4.0](https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/) |
| 2 | "RabbitMQ is in-memory / can't persist" | Durable queues + persistent messages survive restarts. Quorum queues persist everything "regardless of the message delivery mode". Streams live on disk | [queues](https://www.rabbitmq.com/docs/queues), [quorum](https://www.rabbitmq.com/docs/quorum-queues) |
| 3 | "RabbitMQ loses messages / crashes a lot" (e.g. [Lobsters 2025](https://lobste.rs/s/uwp2hd/what_s_your_go_message_queue_2025): "crashing pretty often" in 2014-16) | That's the mirrored-queue era: the only independent Jepsen run was 2014 on 3.3. Mirroring was removed in 4.0. Quorum queues (Raft) pass a harsher Jepsen-derived test in CI. Safety still requires confirms + manual acks | [quorum](https://www.rabbitmq.com/docs/quorum-queues), [4.0 notes](https://github.com/rabbitmq/rabbitmq-server/blob/main/release-notes/4.0.1.md) |
| 4 | "Kafka can't do queues" | Share groups GA in 4.2: per-record ack and N consumers per partition. Java only for now | [4.2](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/) |
| 5 | "Kafka share groups = RabbitMQ in Kafka" | No ordering, no EOS, no TTL/priority/delay; DLQ only from 4.4. Even Aiven titles its post "NOT true queues" | [Aiven 2026-07-02](https://aiven.io/blog/apache-kafka-share-groups-are-not-true-queues) |
| 6 | "Kafka guarantees exactly-once end-to-end" ([Lobsters 2025](https://lobste.rs/s/uwp2hd/what_s_your_go_message_queue_2025): "strict guarantees for things like exactly once deliveries") | EOS covers Kafka→Kafka only (transactions / Streams). The default is at-least-once. External side effects need idempotency | [design](https://kafka.apache.org/43/design/design/) |
| 7 | "Kafka guarantees ordering" | Per partition only. librdkafka retries can reorder because idempotence defaults to `false` | [librdkafka](https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md) |
| 8 | "RabbitMQ queues are strictly FIFO" | Only with a single active consumer, no requeue and no priorities (always on in quorum queues) | [queues](https://www.rabbitmq.com/docs/queues) |
| 9 | "RabbitMQ can't replay" | Streams since 3.9 support `x-stream-offset` by offset/timestamp/interval | [streams](https://www.rabbitmq.com/docs/streams) |
| 10 | "Kafka deletes messages once consumed" | Deletion is retention-based (7 days by default) and independent of consumers | [broker configs](https://kafka.apache.org/43/configuration/broker-configs/) |
| 11 | "Kafka has no DLQ" | Connect has one, Streams has had one since 4.2 (KIP-1034), share groups get one in 4.4. Plain consumer groups: build retry topics | [Michelin](https://blogit.michelin.io/dead-letter-queue-in-kafka-streams-kip-1034/), [Uber](https://www.uber.com/en-US/blog/reliable-reprocessing/) |
| 12 | "`acks=all` means N copies on disk" | It means all *in-sync* replicas, and `min.insync.replicas` defaults to 1. Writes land in the page cache, not fsync | [broker configs](https://kafka.apache.org/43/configuration/broker-configs/), [design](https://kafka.apache.org/43/design/design/) |
| 13 | "Add consumers to scale Kafka" | Consumer groups are capped at the partition count. Partitions can't be reduced, and adding them remaps keys | [ops](https://kafka.apache.org/43/operations/basic-kafka-operations/) |
| 14 | "Kafka is always faster than RabbitMQ" | The famous benchmark tested now-removed mirrored queues. Streams are comparable. The real limit is one CPU core per queue, so use more queues | [queues](https://www.rabbitmq.com/docs/queues) |
| 15 | "RabbitMQ isn't open source anymore (Broadcom)" | Still MPL 2.0. Community *support* and old-series patches are restricted | [policy 2024-05-31](https://www.rabbitmq.com/blog/2024/05/31/new-community-support-policy) |
| 16 | "Rebalances always stop the world" | KIP-848 (GA in Kafka 4.0, librdkafka 2.12) makes them incremental. Opt-in until 5.0 | [librdkafka](https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md) |
| 17 | "Kafka is an event store" | No optimistic concurrency on append, and no efficient per-aggregate reads | [Dudycz](https://event-driven.io/en/event_streaming_is_not_event_sourcing/) |
| 18 | "Our Symfony/Laravel delayed jobs die with the plugin" | Both use TTL + DLX delay queues, not the plugin | [Symfony](https://raw.githubusercontent.com/symfony/symfony/8.1/src/Symfony/Component/Messenger/Bridge/Amqp/Transport/Connection.php), [Laravel pkg](https://raw.githubusercontent.com/vyuldashev/laravel-queue-rabbitmq/master/src/Queue/RabbitMQQueue.php) |

## Hot takes worth quoting

| Quote | Who / where | Date | Use it for |
|---|---|---|---|
| "**A 500 KB/s workload should not use Kafka.**" + "Just use Postgres until it breaks." | Stanislav Kozlovski, [topicpartition.io](https://topicpartition.io/blog/postgres-pubsub-queue-benchmarks), [HN 561 pts / 401 comments](https://news.ycombinator.com/item?id=45747018) | 2025-10-28 | Opening slide: "do you need either?" |
| "Postgres and Kafka are tools designed for very different purposes" / "building a robust queue on top of Postgres is actually harder than it may sound" | Gunnar Morling (Confluent, disclosed), [morling.dev](https://www.morling.dev/blog/you-dont-need-kafka-just-use-postgres-considered-harmful/), [HN](https://news.ycombinator.com/item?id=45793216) | 2025-11-03 | The rebuttal; note the vendor affiliation |
| "You're not Google. Your company will never be Google." / "My bar for 'Is there a reason we can't just do this all in Postgres?' is much, much higher than it was a decade ago." | hn_throwaway_99, [HN "Why do MQ architectures seem less popular now?"](https://news.ycombinator.com/item?id=40725060) (389 pts) | 2024-06-19 | Resume-driven-design slide |
| "Kafka's not a message queue, but ok. Message queues don't suffer from head of line blocking." | Mike Perham (Sidekiq author), [Lobsters](https://lobste.rs/s/syv1il/postgres_better_message_queue_than_kafka) | 2022-10-05 | Point 2 (position vs work item) |
| "That guy above? Yep, that's me, whenever someone says 'Kafka queue'. Because, that's not what Apache Kafka is." | Gunnar Morling, [KIP-932 post](https://www.morling.dev/blog/kip-932-queues-for-kafka/) | 2025-03-05 | Share-groups slide (he ends up positive) |
| "So far I've shown that you most definitely can hold share groups wrong." | Jack Vanlightly, [Broker-Visible vs Client-Local Parallelism](https://jack-vanlightly.com/blog/2026/6/3/broker-visible-vs-client-local-parallelism); see also [Part 1](https://jack-vanlightly.com/blog/2026/5/25/kafka-share-groups-and-parallelizing-consumption-part-1-tuning-maxpollrecords) ("2000 messages … 500 records … only 4 effective consumers per partition") | 2026-05/06 | HOL / in-flight window on slide 8 |
| "Apache Kafka doesn't need fsyncs to be safe because it includes recovery in its replication protocol." | Jack Vanlightly, [blog](https://jack-vanlightly.com/blog/2023/4/24/why-apache-kafka-doesnt-need-fsync-to-be-safe) | 2023-04-24 | **Read before slide 9**: the strongest counterargument |
| "One is smart messaging; dumb clients. The other is dumb messaging; smart clients." | robertlagrant, [HN](https://news.ycombinator.com/item?id=37574552) | 2023-09 | Mental-model slide |
| "I wish I had just used postgresql for both messages and data. It would have been easier to manage by an order of magnitude." vs "We transitioned from a postgres job queue to Rabbit. We had never ending problems after that, many of them were misunderstandings" | tensor / throwitaway222, [HN "PostgreSQL for Everything"](https://news.ycombinator.com/item?id=49361279) (451 pts) | 2026-08-19/20 | Honest "RabbitMQ has a learning curve" moment |
| "Rabbit sucks more than it shines … Everything has to be done in its weird and quirky way" | raverbashing, [same thread](https://news.ycombinator.com/item?id=49372482) | 2026-08-20 | Shows you're not a RabbitMQ shill |
| "We used a database as a message queue. Now we use Kafka" | Tigris (Xe Iaso & Garren), [blog](https://www.tigrisdata.com/blog/quick-fdb-kafka/) | 2026-09-22 | The counter-trend: when the DB-as-queue breaks |
| "Queues will often be applied in ways that totally mess up end-to-end principles for no good reason." | Fred Hebert, [Queues Don't Fix Overload](https://ferd.ca/queues-don-t-fix-overload.html) | 2014-11-19 | Back-pressure slide (classic) |

## Concepts that confuse people

### 1. Ordering

| Setup | Guarantee | Silently broken by |
|---|---|---|
| Kafka consumer group | Per partition (per key) | librdkafka retries with idempotence off (the default); adding partitions; changing keys |
| Kafka share group | None | n/a, by design |
| RabbitMQ queue, 1 consumer / SAC | FIFO | `nack`/`reject` requeue, priorities (always on in quorum queues), adding competing consumers |
| RabbitMQ `x-modulus-hash` → N queues + SAC | Per key | Rebinding queues |
| RabbitMQ stream / super stream + SAC | Per stream / partition | Consuming via plain AMQP competing consumers |

### 2. Exactly-once
- **Inside Kafka:** EOS = idempotent producer + transactional producer + `isolation.level=read_committed` (the consumer default is `read_uncommitted`). It covers Kafka→Kafka only ([design](https://kafka.apache.org/43/design/design/)).
- **Everything else is effectively-once:** at-least-once delivery plus an idempotent consumer, or offsets stored atomically with the output.
- **PHP:** Java clients ≥ 3.0 enable idempotence by default; librdkafka doesn't, so set `enable.idempotence=true` explicitly.

### 3. Consumer groups vs competing consumers vs share groups

| | RabbitMQ competing consumers | Kafka consumer group | Kafka share group |
|---|---|---|---|
| Unit of work | Message | Partition | Record (with a lock) |
| Max useful consumers | Unlimited | Number of partitions | Unlimited (default `group.share.max.size` 200) |
| Ack | Per message, deletes it | Offset commit (a high-water mark) | Per record: accept/release/reject/renew |
| One slow message | Blocks only its consumer slot | Blocks its partition | Pins the in-flight window (2000 locks/partition) |
| Fan-out to N apps | One queue per app (bindings) | One group per app | One group per app |

### 4. Prefetch vs `max.poll.records`

| | RabbitMQ `basic_qos` prefetch | Kafka Java `max.poll.records` | php-rdkafka (librdkafka) |
|---|---|---|---|
| Meaning | Max *unacked* deliveries pushed to this consumer | Max records *returned* per `poll()`. Fetching is separate (`fetch.*`) | Doesn't exist for the classic consumer. `consume()` returns 1 message; a background queue prefetches up to `queued.min.messages` = 100 000 per partition |
| Default | 0 = unlimited ([docs](https://www.rabbitmq.com/docs/consumer-prefetch)). Without `basic_qos`, one worker hoards the backlog | 500 | 100 000 msgs / 64 MB |
| Affects fairness? | **Yes**: a low prefetch spreads work across workers | No: partitions are assigned | No |
| Share groups | n/a | **Yes**: big batches starve other consumers ([Vanlightly](https://jack-vanlightly.com/blog/2026/5/25/kafka-share-groups-and-parallelizing-consumption-part-1-tuning-maxpollrecords)) | Share consumer only (Preview) |

**Offset-commit trap in PHP:** librdkafka defaults `enable.auto.offset.store=true` + `enable.auto.commit=true`. That stores the offset as soon as the message is *handed to you* and commits it every 5 s, so a crash mid-job skips the message. Fix: `enable.auto.commit=false` and `$consumer->commit($message)` after processing, or turn off auto offset store and store offsets yourself.

### 5. DLQ / retry patterns

| Mechanism | RabbitMQ 4.3 | Kafka 4.3 |
|---|---|---|
| Poison message cap | Quorum queue `x-delivery-limit`, default 20 | Share group `group.share.delivery.count.limit`, default 5 (then the record is archived, not routed) |
| Delayed retry | `x-delayed-retry-*` (quorum queues, 4.3); TTL + DLX delay queues (Symfony/Laravel) | Retry topics with increasing delays ([Uber](https://www.uber.com/en-US/blog/reliable-reprocessing/)) |
| DLQ | DLX with `x-death` reasons; at-least-once dead-lettering on quorum queues | Connect DLQ; Streams DLQ (4.2); share-group DLQ (4.4) |
| Framework layer | Symfony `retry_strategy` (default 3 retries, 1 s delay, ×2 multiplier) + `failure_transport` ([docs](https://symfony.com/doc/current/messenger.html)) | Same Symfony layer via third-party transport |

**Gotcha:** on 4.3 use `reject`, not `nack`, if you want delivery-count backoff (↔03 Demo A).

### 6. Back-pressure
- **Kafka is pull-based.** A slow consumer just builds lag, so the risk is retention deleting unread data. Monitor consumer lag.
- **RabbitMQ is push-based with prefetch.** Credit flow throttles fast publishers (connection state `flow`, see [flow control](https://www.rabbitmq.com/docs/flow-control)). Alarms block publishers. Queue length limits with `reject-publish` nack them. Monitor queue depth and scale workers on it (KEDA).
- **Neither fixes overload; they defer it** ([Hebert](https://ferd.ca/queues-don-t-fix-overload.html)).

### 7. Replay
- **Kafka:** any group can re-read from any offset or timestamp within retention. Compacted topics keep the latest value per key regardless of age.
- **RabbitMQ:** only streams replay. In queues, ack = delete. Hybrid is common: a stream for history, quorum queues for work.

### 8. Timeouts cheat-sheet (the "why did my worker die?" question)

| Setting | Default | When it fires |
|---|---|---|
| RabbitMQ heartbeat | 60 s, 2 missed → close | A PHP job blocks longer than ~2 heartbeat intervals |
| RabbitMQ `consumer_timeout` | 30 min | Unacked delivery held too long → channel closed, requeued |
| Kafka `max.poll.interval.ms` | 5 min | No `consume()` call → member leaves, rebalance |
| Kafka `session.timeout.ms` | 45 s | Process dead (librdkafka heartbeats from a background thread) |
| Kafka share record lock | 30 s | Lock expires → record goes to another consumer (use RENEW) |
| Laravel `timeout` vs `retry_after` | — | `timeout` ≥ `retry_after` → job runs twice ([docs](https://laravel.com/docs/queues)) |

## Sources

**Official docs (verified 2026-10-04)**
- Kafka 4.3: [design](https://kafka.apache.org/43/design/design/) · [consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/) · [producer configs](https://kafka.apache.org/43/configuration/producer-configs/) · [broker configs](https://kafka.apache.org/43/configuration/broker-configs/) · [basic ops](https://kafka.apache.org/43/operations/basic-kafka-operations/)
- Kafka releases: [4.0, 2025-03-18](https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/) · [4.2, 2026-02-17](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/)
- [librdkafka CONFIGURATION.md](https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md) · [php-rdkafka README](https://github.com/php-rdkafka/php-rdkafka) ("Not calling flush can lead to message loss!")
- RabbitMQ: [queues](https://www.rabbitmq.com/docs/queues) · [reliability](https://www.rabbitmq.com/docs/reliability) · [consumers](https://www.rabbitmq.com/docs/consumers) · [prefetch](https://www.rabbitmq.com/docs/consumer-prefetch) · [quorum queues](https://www.rabbitmq.com/docs/quorum-queues) · [streams](https://www.rabbitmq.com/docs/streams) · [DLX](https://www.rabbitmq.com/docs/dlx) · [flow control](https://www.rabbitmq.com/docs/flow-control) · [alarms](https://www.rabbitmq.com/docs/alarms) · [max length](https://www.rabbitmq.com/docs/maxlength) · [heartbeats](https://www.rabbitmq.com/docs/heartbeats) · [4.0 release notes](https://github.com/rabbitmq/rabbitmq-server/blob/main/release-notes/4.0.1.md) · [support policy, 2024-05-31](https://www.rabbitmq.com/blog/2024/05/31/new-community-support-policy)
- [php-amqplib README](https://github.com/php-amqplib/php-amqplib) · [issue #725 (heartbeats + long jobs)](https://github.com/php-amqplib/php-amqplib/issues/725)
- [Laravel 13 queues](https://laravel.com/docs/queues) · [Horizon](https://laravel.com/docs/horizon) · [Symfony 8.1 Messenger](https://symfony.com/doc/current/messenger.html) · [Symfony AMQP Connection.php](https://raw.githubusercontent.com/symfony/symfony/8.1/src/Symfony/Component/Messenger/Bridge/Amqp/Transport/Connection.php) · [vyuldashev RabbitMQQueue.php](https://raw.githubusercontent.com/vyuldashev/laravel-queue-rabbitmq/master/src/Queue/RabbitMQQueue.php)

**Community threads**
- HN: [Kafka is Fast – I'll use Postgres, 2025-10-29](https://news.ycombinator.com/item?id=45747018) · [Considered Harmful, 2025-11](https://news.ycombinator.com/item?id=45793216) · [Ask HN: go-to MQ in 2025, 2025-05-15](https://news.ycombinator.com/item?id=43993982) · [Why are MQ architectures less popular, 2024-06-18](https://news.ycombinator.com/item?id=40723302) · [PostgreSQL for Everything, 2026-08-19](https://news.ycombinator.com/item?id=49361279) · [RabbitMQ vs Kafka – Architect's Dilemma, 2023-09](https://news.ycombinator.com/item?id=37574552) · [Tigris DB→Kafka, 2026-09-30](https://news.ycombinator.com/item?id=49913886)
- Lobsters: [go-to MQ 2025](https://lobste.rs/s/uwp2hd/what_s_your_go_message_queue_2025) · [Kafka is Fast, 2025-10-29](https://lobste.rs/s/oj3ce4/kafka_is_fast_i_ll_use_postgres) · [Considered Harmful](https://lobste.rs/s/kbp8xn/you_don_t_need_kafka_just_use_postgres) · [Postgres better than Kafka?, 2022-10-05](https://lobste.rs/s/syv1il/postgres_better_message_queue_than_kafka)

**Engineers / blogs**
- Kozlovski: [benchmark, 2025-10-28](https://topicpartition.io/blog/postgres-pubsub-queue-benchmarks) · [Just use Postgres recap, 2025-12-27](https://blog.2minutestreaming.com/p/just-use-postgres)
- Morling: [Considered Harmful, 2025-11-03](https://www.morling.dev/blog/you-dont-need-kafka-just-use-postgres-considered-harmful/) · [KIP-932, 2025-03-05](https://www.morling.dev/blog/kip-932-queues-for-kafka/)
- Vanlightly: [fsync, 2023-04-24](https://jack-vanlightly.com/blog/2023/4/24/why-apache-kafka-doesnt-need-fsync-to-be-safe) · [KIP-966, 2023-08-17](https://jack-vanlightly.com/blog/2023/8/17/kafka-kip-966-fixing-the-last-replica-standing-issue) · [share groups pt 1, 2026-05-25](https://jack-vanlightly.com/blog/2026/5/25/kafka-share-groups-and-parallelizing-consumption-part-1-tuning-maxpollrecords) · [broker-visible parallelism, 2026-06-04](https://jack-vanlightly.com/blog/2026/6/3/broker-visible-vs-client-local-parallelism)
- Vendors: [Confluent GA, 2026-03-03](https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/) · [Kai Waehner, 2026-01-28](https://www.kai-waehner.de/blog/2026/01/28/when-to-use-queues-for-kafka/) · [Aiven "NOT true queues", 2026-07-02](https://aiven.io/blog/apache-kafka-share-groups-are-not-true-queues) · [Aiven KIP-1150](https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead)
- Others: [Uber retry/DLQ, 2018-02-16](https://www.uber.com/en-US/blog/reliable-reprocessing/) · [Michelin KIP-1034](https://blogit.michelin.io/dead-letter-queue-in-kafka-streams-kip-1034/) · [Dudycz](https://event-driven.io/en/event_streaming_is_not_event_sourcing/) · [Hebert, 2014-11-19](https://ferd.ca/queues-don-t-fix-overload.html) · [Tigris, 2026-09-22](https://www.tigrisdata.com/blog/quick-fdb-kafka/) · [IBM–Confluent close, 2026-03-17](https://newsroom.ibm.com/2026-03-17-ibm-completes-acquisition-of-confluent,-making-real-time-data-the-engine-of-enterprise-ai-and-agents)

**Gaps:** I couldn't fetch Reddit (r/devops, r/apachekafka, r/PHP, r/laravel all block the crawler) or YouTube comments. LinkedIn/X posts are covered only through search snippets (e.g. Kozlovski's 2023 LinkedIn post on KIP-932, Morling's Bluesky). Skim r/PHP and r/laravel by hand for "RabbitMQ" and "Kafka" from 2025-26 before the talk.
