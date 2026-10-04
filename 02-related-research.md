# Related research — cross-checks, bias audit, and the PHP angle

> Companion to `01-source-summary.md`. Purpose: don't walk into the room quoting a vendor page as neutral fact.

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
- **RabbitMQ streams shipped in 3.9 (2021)**; classic queue mirroring removed in **4.0**; quorum queues GA since 2019. → accurate.
- **Kafka's durability doc position** (replication over fsync, `flush.messages` discouraged) is genuine Kafka documentation, not a strawman.

### The one number to challenge on stage

Third-party benchmarks **do not** agree with each other, and rarely with the page:

| Source | Kafka | RabbitMQ classic | RabbitMQ quorum | RabbitMQ stream |
|---|---|---|---|---|
| rabbitmq.com page | "same ballpark" | ~100k/s | ~80k/s | several M/s (>4M w/ batching) |
| OpenMessaging Benchmark (6-broker, cited by third parties) | 880k/s | 52k/s | — | 650k/s |
| Single-node 2025 benchmark (secondary source) | ~110k/s | 22k/s | **14k/s** | — |

**Honest reading:** every one of these is a different cluster size, message size, ack setting, and batch config. The quorum-queue number swings 14k → 80k depending on who ran it. Use the page's own warning as your line: *"a benchmark that tunes one side hard while leaving the other on defaults compares tuning effort, not systems."*

### Things the page soft-pedals

- **Quorum queues have a consensus cost.** RabbitMQ's own docs say they're a bad fit for temporary queues, high-frequency create/delete workloads, and apps that don't use acks + publisher confirms. The page's "~80k msg/s" is a best case.
- **Streams don't get the full AMQP routing model.** No dead-letter exchange, no topic-exchange semantics on a stream in the way you get on a queue. Real deployments run a **hybrid topology** — streams for fan-out, quorum queues for task dispatch — which is more moving parts, not fewer.
- **Ordering with competing consumers.** RabbitMQ guarantees order in a single queue with a *single* consumer. With competing consumers, order is not preserved (a slow consumer holds an unacked message while faster ones move ahead). Kafka guarantees order **per partition**, not across a topic. Neither gives you global ordering. Don't let the "queue" word imply FIFO across workers.
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
| Main PHP client | **php-amqplib** — pure PHP, AMQP 0-9-1, most widely used, co-maintained by RabbitMQ/VMware engineers | **php-rdkafka** — PHP ext wrapping the C `librdkafka` |
| Install | `composer require` — done | Compile/install a **C extension** + `librdkafka` on every image, worker box, and dev laptop |
| Alternative | `ext-amqp` (PECL, C, faster) | `php-rdkafka-ffi` (FFI, no ext build) |
| AMQP 1.0 support | Not in php-amqplib (long-standing open request) — 0-9-1 only | n/a |
| Stream protocol | **No official PHP client.** RabbitMQ ships stream clients for Java, .NET, Go, Rust, Python, JS — not PHP. | n/a |

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
    arguments: new AMQPTable(['x-stream-offset' => 'next'])  // first | last | next | <int> | <timestamp>
);
```

What you give up going through AMQP 0-9-1 instead of the native stream protocol: **throughput** (the binary protocol is the performance path), **server-side offset tracking** (you store offsets yourself), **super-stream partitioning** and single-active-consumer, and reliable access to stream filtering. If you need native stream performance from PHP, the honest answer is a **sidecar in Go/Java/.NET** exposing results over HTTP/gRPC — which is a real architectural cost, and a good slide.

### 3.2 Framework support

| | RabbitMQ | Kafka |
|---|---|---|
| **Symfony Messenger** | Built-in AMQP transport (`symfony/amqp-messenger`), plus a newer streaming AMQP transport | No first-party transport — third-party bundles |
| **Laravel** | Community `queue` drivers over php-amqplib; Horizon is Redis-only | Third-party packages; not a queue driver in the Laravel sense |
| Retry / backoff / DLQ | Maps naturally onto Messenger's retry + failure transport, because the broker already has the primitives | You build the retry topic / delay topic / DLQ topic pattern yourself |

### 3.3 The PHP process model problem

This is the sleeper issue and worth its own slide:

- PHP has **no threads and no event loop** in the typical worker. One process = one consumer.
- **Kafka's parallelism ceiling is the partition count** (in classic consumer groups): 12 partitions = max 12 PHP workers doing useful work, no matter how many you boot. Adding workers 13-40 changes nothing. Scaling up means **repartitioning**, which changes key→partition mapping and therefore ordering.
- **RabbitMQ's parallelism is the number of consumers.** Boot 200 PHP workers on one queue; the broker hands each one a message. Autoscaling on queue depth is trivial and is exactly what `supervisord` / k8s HPA / Laravel `queue:work` already do.
- Kafka **share groups fix this** (more consumers than partitions, per-message ack) — but as of 4.2 the PHP client story for share consumers is far behind Java/Spring, and share consumers **can't use regex topic subscription**.
- `php-rdkafka` in long-running processes has known operational sharp edges (shutdown/`consumeStop` hangs, rebalance handling, no fork-safety) — there are multi-year open issues about exactly this. php-amqplib long-running consumers are a boringly well-trodden path.

**Conclusion for your audience:** for a PHP shop, *"RabbitMQ for work, Kafka for the data platform"* isn't just architectural preference — it's a client-maturity and process-model fact.

---

## 4. The one Kafka feature that most often ends the argument

**Log compaction.** If someone needs a keyed changelog they can replay from the beginning to rebuild state (CDC, event sourcing with state rebuild, "the current value of every entity"), RabbitMQ streams cannot do it — they retain by size/age only. That's not a tuning gap; it's a missing feature. Same for **tiered storage** if retention is measured in months/years.

Everything else in the "only Kafka" column is ecosystem, not capability.

---

## 5. Sources

- [RabbitMQ vs. Apache Kafka — rabbitmq.com](https://www.rabbitmq.com/docs/compare/kafka) (primary, vendor)
- [Apache Kafka 4.2.0 Release Announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/)
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/x/4hA0Dw) · [Preview release notes](https://cwiki.apache.org/confluence/x/CIq3FQ)
- [Kafka Queue Semantics Now GA with Share Consumer API — Confluent](https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/)
- [Let's Take a Look at KIP-932: Queues for Kafka — Gunnar Morling](https://www.morling.dev/blog/kip-932-queues-for-kafka/)
- [Kafka Queues (Share Consumer) — Spring Kafka docs](https://docs.spring.io/spring-kafka/reference/kafka/kafka-queues.html)
- [When to use RabbitMQ or Apache Kafka — CloudAMQP](https://www.cloudamqp.com/blog/when-to-use-rabbitmq-or-apache-kafka.html)
- [Kafka vs RabbitMQ: Streams vs Queues — Conduktor](https://www.conduktor.io/glossary/kafka-vs-rabbitmq)
- [The difference between RabbitMQ and Kafka — AWS](https://aws.amazon.com/compare/the-difference-between-rabbitmq-and-kafka/)
- [Kafka vs RabbitMQ: The Consumer-Driven Choice — OpenCredo](https://opencredo.com/blogs/kafka-vs-rabbitmq-the-consumer-driven-choice/)
- [php-amqplib/php-amqplib](https://github.com/php-amqplib/php-amqplib) · [AMQP 1.0 support request #583](https://github.com/php-amqplib/php-amqplib/issues/583) · [Stream support discussion #998](https://github.com/php-amqplib/php-amqplib/discussions/998)
- [pdezwart/php-amqp (ext-amqp)](https://packagist.org/packages/pdezwart/php-amqp)
- [arnaud-lb/php-rdkafka — long-running process issue #448](https://github.com/arnaud-lb/php-rdkafka/issues/448) · [consumeStop issue #115](https://github.com/arnaud-lb/php-rdkafka/issues/115)
- [php-rdkafka-ffi high-level consumer](https://idealo.github.io/php-rdkafka-ffi/usage/consumer-high-level/)
- [RabbitMQ client libraries & devtools](https://www.rabbitmq.com/client-libraries/devtools)
- [Introducing a Streaming AMQP Transport for Symfony Messenger](https://symfony.com/blog/introducing-a-streaming-amqp-transport-for-symfony-messenger)
- Secondary benchmark claims (treat with caution): [tech-insider.org](https://tech-insider.org/rabbitmq-vs-kafka-2026/), [danubedata.ro](https://danubedata.ro/blog/rabbitmq-vs-kafka-vs-sqs-comparison-2026)
