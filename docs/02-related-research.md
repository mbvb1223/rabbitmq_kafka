# Related research — cross-checks, bias audit, and the PHP angle

> Companion to [01-source-summary.md](01-source-summary.md). Purpose: don't walk into the room quoting a vendor page as neutral fact.

---

## 1. Bias audit — what to verify before you present it

The page is **written and published by the RabbitMQ team**. It says so up front, which is honest, but the framing is still an argument, not a neutral survey. Three levels of claim:

| Claim type | Examples | Trust level |
|---|---|---|
| **Verifiable facts** | Kafka's default disables application fsync; Kafka docs say Windows is not well supported; Kafka reports via JMX; RabbitMQ mirrored queues removed in 4.0; share groups GA in 4.2 | **High** — quoted from Kafka's own docs / release notes |
| **Architectural reasoning** | Multi-Raft + shared WAL amortises fsync; per-partition flush lock in Kafka; share-group start-offset pinning | **High-ish** — technically sound, but stated in the most flattering direction |
| **Conclusions** | "RabbitMQ is the better default, and increasingly so"; "Kafka's architecture cannot achieve all three" | **Opinion** — present as *their* verdict, not yours |

### Facts I independently confirmed

- **Kafka 4.2.0 released 2026-02-17**, with KIP-932 share groups at **GA / production-ready**. Timeline: 4.0 (Mar 2025) early access → 4.1 preview → 4.2 GA. Adds `RENEW` ack type, adaptive share-coordinator batching, lag metrics. Spring for Apache Kafka 4.1 promotes share consumers to full support. → *The page's headline premise is accurate and current.*
- **Kafka 4.3.0 shipped 2026-05-22**; 4.3.1 is current. **4.4 is in RC** and adds dead-letter queues for share groups (KIP-1191) — re-check before the talk.
- **RabbitMQ streams shipped in 3.9 (2021)**; super streams + single active consumer in 3.11 (2022); classic queue mirroring removed in **4.0**; quorum queues GA since 2019. → accurate.
- **RabbitMQ 4.3 (2026-04-23)** is the version behind the page's per-message features: 32 priorities on quorum queues, delayed retry, quorum-queue consumer timeouts, Khepri as the only metadata store (Mnesia removed).
- **Kafka's durability doc position** (replication over fsync, `flush.messages` discouraged) is genuine Kafka documentation, not a strawman. But `acks=all` with the default `min.insync.replicas=1` can mean a single broker's page cache.

### The one number to challenge on stage

There is one primary head-to-head benchmark, and it's old:

| Source | Kafka | RabbitMQ | Setup |
|---|---|---|---|
| rabbitmq.com page | "same ballpark" | classic ~100k/s · quorum ~80k/s · stream several M/s (>4M w/ batching) | Vendor; stream numbers need the native stream protocol and well-filled chunks |
| [Confluent, 2020](https://www.confluent.io/blog/kafka-fastest-messaging-system/) | 605 MB/s | 38 MB/s | Vendor; RabbitMQ 3.8.5 **mirrored queues**, persistence off, 3× i3en.2xlarge, 1 KB messages — predates quorum queues as default and streams in the test |

**Honest reading:** both are vendor runs, and the Confluent one tests a queue type RabbitMQ has since removed. Over AMQP — the only mature path from PHP — RabbitMQ's own docs put streams at "hundreds of thousands per second", not millions. Use the page's own warning as your line: *"a benchmark that tunes one side hard while leaving the other on defaults compares tuning effort, not systems."*

### Things the page soft-pedals

- **Quorum queues have a consensus cost.** RabbitMQ's own docs say they're a bad fit for temporary queues, high-frequency create/delete workloads, and apps that don't use acks + publisher confirms. The page's "~80k msg/s" is a best case.
- **Streams aren't queues.** Streams can be bound to any exchange (topic included), but they have no dead-lettering, TTL or priorities, and every consumer reads the whole stream — a per-consumer subset needs filtering or a separate stream. Real deployments run a **hybrid topology** — streams for fan-out, quorum queues for task dispatch — which is more moving parts, not fewer.
- **Ordering with competing consumers.** RabbitMQ guarantees order in a single queue with a *single* consumer; the documented answer is **single active consumer**. With competing consumers, order is not preserved (a slow consumer holds an unacked message while faster ones move ahead), requeue/redelivery reorders, and on 4.3 quorum queues priorities are always on, so any publisher that sets a priority reorders too. Kafka guarantees order **per partition**; share groups give that up. Neither gives you global ordering.
- **Scheduled delivery left open source.** The community `rabbitmq-delayed-message-exchange` plugin was archived on 2026-09-24 and doesn't run on 4.3 (it depends on Mnesia). Publisher-set delays are now a Tanzu (commercial) feature. Anyone using `x-delay` has an upgrade trap.
- **Ecosystem gravity is real and one-directional.** Flink / Spark / Iceberg / Debezium / ksqlDB / Schema Registry / the connector long tail assume Kafka. If your company's data platform, CDC, or analytics team already speaks Kafka, "start with RabbitMQ" is advice about a greenfield you may not have.

---

## 2. The two-sentence mental model (use this on the first slide)

- **Kafka = "dumb broker, smart consumer."** The broker is a distributed commit log. It never opens the envelope. The consumer owns its offset and its filtering. Everything is written to disk regardless of who consumed it; a 9:00 AM message is still there at 9:00 PM.
- **RabbitMQ = "smart broker, dumb consumer."** The broker opens the envelope: routes it, filters it, prioritises it, delays it, retries it, dead-letters it, deletes it when done. The consumer just receives.

Everything else in the comparison is a consequence of that one sentence.

---

## 3. Where this actually bites a PHP shop

This is the part the RabbitMQ page doesn't cover, and it's the most relevant part for your audience.

### 3.1 Client maturity: not close

| | RabbitMQ | Kafka |
|---|---|---|
| Main PHP client | **php-amqplib** — pure PHP, AMQP 0-9-1, most widely used, co-maintained by RabbitMQ/VMware engineers | **php-rdkafka** — PHP ext wrapping the C `librdkafka`. Last stable 6.0.5 (2024-11-04); 7.0.0alpha1 (2026-05-07) |
| Install | `composer require` + `docker-php-ext-install sockets` (official `php:*` images lack `ext-sockets`) | Compile/install a **C extension** + a matching `librdkafka` on every image, worker box, and dev laptop |
| Alternative | `ext-amqp` (PECL, C, faster) — required by Symfony's built-in transport | None maintained (`php-rdkafka-ffi` archived 2025-10-27) |
| AMQP 1.0 support | Not in php-amqplib (request #583 closed 2018, never implemented); no mature PHP AMQP 1.0 client | n/a |
| Stream protocol | **No official PHP client.** One young community client ([crazy-goat/rabbit-stream](https://github.com/crazy-goat/rabbit-stream)). Official: Java, .NET, Go, Rust; Python/JS are community-maintained. | n/a |
| Share consumer (KIP-932) | n/a | librdkafka 2.15.0 (2026-06-30) has a **Preview** share consumer; php-rdkafka has no binding ([#616](https://github.com/php-rdkafka/php-rdkafka/issues/616) open) |

**Practical consequence for streams in PHP:** you consume a stream *as if it were an AMQP queue*:

```php
$ch->queue_declare(
    queue: 'events',
    durable: true,
    auto_delete: false,
    arguments: new AMQPTable(['x-queue-type' => 'stream'])
);
$ch->basic_qos(0, 100, false);          // prefetch is mandatory for streams
$ch->basic_consume(
    queue: 'events',
    callback: $cb,
    // first | last | next | <int offset> | DateTimeImmutable | interval '1D'
    // a PHP int is read as an offset, not a timestamp
    arguments: new AMQPTable(['x-stream-offset' => 'next'])
);
```

What you give up going through AMQP 0-9-1 instead of the native stream protocol: **throughput** (the binary protocol is the performance path), **server-side offset tracking** (you store offsets yourself), **single active consumer**, client-side super-stream routing, and **publishing deduplication** — so no effectively-once publishing from PHP. Bloom-filter stream filtering does work over 0-9-1 (`x-stream-filter`). If you need native stream performance from PHP, the options are a young community client or a **sidecar in Go/Java/.NET** exposing results over HTTP/gRPC — which is a real architectural cost, and a good slide.

### 3.2 Framework support

| | RabbitMQ | Kafka |
|---|---|---|
| **Symfony Messenger** | Built-in AMQP transport (`symfony/amqp-messenger`) — **requires `ext-amqp`**. Third-party `jwage/phpamqplib-messenger` runs on php-amqplib ("streaming" = push `consume()`, not RabbitMQ streams) | No first-party transport — third-party bundles |
| **Laravel** | Community `queue` drivers over php-amqplib; Horizon is Redis-only | Third-party packages; not a queue driver in the Laravel sense |
| Retry / backoff / DLQ | Maps naturally onto Messenger's retry + failure transport, because the broker already has the primitives | You build the retry topic / delay topic / DLQ topic pattern yourself |

### 3.3 The PHP process model problem

This is the sleeper issue and worth its own slide:

- In PHP's **default** worker model there are no threads and no event loop: one process = one consumer. Escape hatches exist — Fibers + Revolt (AMPHP v3, ReactPHP), Swoole, RoadRunner (its Kafka driver consumes in Go and feeds a fixed PHP worker pool) — but each costs per-key ordering or adds a runtime.
- **Kafka's parallelism ceiling is the partition count** (classic and KIP-848 consumer groups alike): 12 partitions = max 12 PHP workers doing useful work, no matter how many you boot. Java has the same ceiling; what PHP lacks is cheap in-process fan-out (Java has Confluent Parallel Consumer). "Just over-partition" doesn't save you: the count is fixed up front, can't shrink, and increasing it remaps keys → breaks per-key ordering.
- **RabbitMQ's parallelism is the number of consumers.** Boot 200 PHP workers on one queue; the broker hands each one a message. Autoscale on queue depth with the **KEDA RabbitMQ scaler** (supervisord runs a fixed `numprocs`, `queue:work` is one worker, Horizon's autoscaling is Redis-only). KEDA's Kafka scaler caps replicas at the partition count unless `allowIdleConsumers: true`.
- Kafka **share groups fix this** (more consumers than partitions, per-message ack) — but from PHP there's only librdkafka's Preview share consumer and no php-rdkafka binding. Share consumers also **can't use regex topic subscription** and give no ordering.
- `php-rdkafka` in long-running processes has operational sharp edges (shutdown, rebalance handling, fork-safety). Without `pcntl`, SIGTERM can't trigger `$consumer->close()`, so on scale-down partitions stay assigned until `session.timeout.ms` (45 s). php-amqplib long-running consumers are a boringly well-trodden path.

**Conclusion for your audience:** for a PHP shop, *"RabbitMQ for work, Kafka for the data platform"* isn't just architectural preference — it's a client-maturity and process-model fact.

---

## 4. The one Kafka feature that most often ends the argument

**Log compaction.** If someone needs a keyed changelog they can replay from the beginning to rebuild state (CDC, event sourcing with state rebuild, "the current value of every entity"), RabbitMQ streams cannot do it — they retain by size/age only. That's not a tuning gap; it's a missing feature. Same for **tiered storage** if retention is measured in months/years.

Everything else in the "only Kafka" column is ecosystem, not capability.

---

## 5. Sources

- [RabbitMQ vs. Apache Kafka — rabbitmq.com](https://www.rabbitmq.com/docs/compare/kafka) (primary, vendor)
- [Apache Kafka 4.2.0 Release Announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/) · [4.3.0 Release Announcement](https://kafka.apache.org/blog/2026/05/22/apache-kafka-4.3.0-release-announcement/)
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/x/4hA0Dw) · [Preview release notes](https://cwiki.apache.org/confluence/x/CIq3FQ) · [KIP-1191: Dead-Letter Queues for Share Groups](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1191:+Dead-Letter+Queues+for+Share+Groups)
- [Kafka Queue Semantics Now GA with Share Consumer API — Confluent](https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/)
- [Let's Take a Look at KIP-932: Queues for Kafka — Gunnar Morling](https://www.morling.dev/blog/kip-932-queues-for-kafka/)
- [Kafka Queues (Share Consumer) — Spring Kafka docs](https://docs.spring.io/spring-kafka/reference/kafka/kafka-queues.html)
- [Benchmarking Apache Kafka, Apache Pulsar, and RabbitMQ — Confluent, 2020](https://www.confluent.io/blog/kafka-fastest-messaging-system/) (vendor)
- [RabbitMQ 4.3 release](https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release) · [Quorum queues](https://www.rabbitmq.com/docs/quorum-queues) · [Streams](https://www.rabbitmq.com/docs/streams) · [Stream filtering](https://www.rabbitmq.com/docs/stream-filtering) · [Stream plugin vs core](https://www.rabbitmq.com/docs/stream-core-plugin-comparison)
- [rabbitmq-delayed-message-exchange (archived)](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)
- [When to use RabbitMQ or Apache Kafka — CloudAMQP](https://www.cloudamqp.com/blog/when-to-use-rabbitmq-or-apache-kafka.html)
- [Kafka vs RabbitMQ: Streams vs Queues — Conduktor](https://www.conduktor.io/glossary/kafka-vs-rabbitmq)
- [The difference between RabbitMQ and Kafka — AWS](https://aws.amazon.com/compare/the-difference-between-rabbitmq-and-kafka/)
- [Kafka vs RabbitMQ: The Consumer-Driven Choice — OpenCredo](https://opencredo.com/blogs/kafka-vs-rabbitmq-the-consumer-driven-choice/)
- [php-amqplib/php-amqplib](https://github.com/php-amqplib/php-amqplib) · [AMQP 1.0 request #583 (closed)](https://github.com/php-amqplib/php-amqplib/issues/583) · [Stream support discussion #998](https://github.com/php-amqplib/php-amqplib/discussions/998)
- [pdezwart/php-amqp (ext-amqp)](https://packagist.org/packages/pdezwart/php-amqp) · [symfony/amqp-messenger composer.json](https://github.com/symfony/amqp-messenger/blob/7.4/composer.json) · [jwage/phpamqplib-messenger announcement](https://symfony.com/blog/introducing-a-streaming-amqp-transport-for-symfony-messenger)
- [php-rdkafka/php-rdkafka](https://github.com/php-rdkafka/php-rdkafka) · [PECL rdkafka](https://pecl.php.net/package/rdkafka) · [librdkafka CHANGELOG](https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md)
- [crazy-goat/rabbit-stream](https://github.com/crazy-goat/rabbit-stream) (community PHP stream client)
- [RabbitMQ client libraries & devtools](https://www.rabbitmq.com/client-libraries/devtools)
- [RoadRunner Kafka jobs driver](https://docs.roadrunner.dev/docs/queues-and-jobs/kafka) · [KEDA Kafka scaler](https://keda.sh/docs/latest/scalers/apache-kafka/)
