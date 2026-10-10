# Sharing session — topic proposal (DRAFT, awaiting approval)

## Proposed title

> **"Smart Broker vs Smart Consumer — what RabbitMQ and Kafka actually disagree about in 2026"**
> *and why the answer is different when your workers are PHP*

**Audience:** senior-ish PHP devs
**Length:** 45 min talk + 15 min Q&A
**Format:** slides + 2 live demos in PHP
**Versions:** Kafka 4.3 / RabbitMQ 4.3 — re-check before delivery

---

## The one-sentence takeaway

> They converged on streaming. The real difference is **how much the broker knows about each individual message** — and in PHP, the second difference is **client maturity and the process model**.

If people remember only that, the session worked.

---

## Why this framing (and not "RabbitMQ vs Kafka: which is better")

1. **It kills a dead rule of thumb.** Everyone in the room believes "Kafka = streaming, RabbitMQ = queueing." That rule has been eroding since RabbitMQ 3.9 added streams (2021) and died when Kafka 4.2 took share groups GA (Feb 2026; early access since 4.0). Correcting a belief the audience holds is the strongest possible opener.
2. **It's decision-useful.** People leave with a checklist for the next architecture review, not trivia.
3. **It's honest.** The source is a vendor page — disclosed on slide 1. Presenting it *as* an argument — and then stress-testing it — is more credible and more interesting than reading it out.
4. **It lands in PHP.** The parallelism ceiling, `php-rdkafka` vs `php-amqplib`, and the lack of an official PHP stream client are things this room will hit personally.

---

## Main points (5)

### 1. They converged — and that's the news
Same playbook on both sides: batching as the unit of work, one binary format end-to-end, `sendfile()` zero-copy, page cache over heap, publisher-side dedup, and Raft for metadata (KRaft vs Khepri). Kill the "RabbitMQ is slow" myth carefully: **streams are in Kafka's league over the native stream protocol** (millions/sec per stream when chunks are well filled). Over AMQP — the only mature path from PHP — RabbitMQ's docs say hundreds of thousands/sec. Quorum queues trade throughput for fsync (that's Point 3). The only primary head-to-head (Confluent 2020) tested mirrored queues, which RabbitMQ has since removed — every public benchmark is really measuring tuning effort.

### 2. A message is a *position* vs a message is a *work item* ← **the core of the talk**
Kafka: a message is a position in a shared log; its fate is coupled to its neighbours; the broker never opens the envelope.
RabbitMQ: a message is an independent item of work; the broker parses the envelope.

Everything downstream follows: TTL, 32 priorities (quorum queues, 4.3+), delayed retry with backoff (4.3), dead-letter **routing** through exchanges, and — the underrated one — **acking deletes, so disk comes back and the queue converges to empty**. Kafka sizes disk by retention window, not by backlog. Over AMQP 1.0 only: per-message delay override, deferral by token, consumer-annotated returns — none reachable from PHP. Publisher-set scheduled delivery is now Tanzu-only (the community delayed-message plugin was archived 2026-09-24 and doesn't run on 4.3).

Then the fair part: **Kafka share groups (GA in 4.2) close some of this** — per-message ack, delivery counting, acquisition locks. And the part they don't close: ack ≠ delete, no TTL/priorities/delays, no per-partition ordering, and head-of-line blocking **reduced, not eliminated** (the in-flight window — 2000 offsets by default — pins on the oldest unfinished message and the partition stops being fetched). DLQ routing is coming in 4.4 (KIP-1191). And without share groups, Kafka's answer is the retry-topic / delay-topic / DLQ-topic pattern you build yourself.

### 3. "Acknowledged" doesn't mean the same thing on both sides
Kafka `acks=all` = in the **page cache** of every in-sync replica — and with the default `min.insync.replicas=1` that can be one broker. Kafka's own docs recommend disabling application fsync. Lose power across correlated replicas → confirmed messages can be lost.
RabbitMQ quorum queues = Raft majority has **written and flushed** before the confirm.
And the *why*: Kafka's log is per-partition so each fsync is separate; RabbitMQ's Ra is multi-Raft over a **shared WAL** — thousands of queues, one batched fsync.

The fair part: racks/AZs make correlated power loss unlikely — the page says so too, and for most cloud deployments a 3-AZ Kafka with `min.insync.replicas=2` is a reasonable trade. The point is that RabbitMQ lets you choose per destination.

This is the slide for anyone doing payments or orders.

### 4. Who decides what a consumer gets — the broker or the producer
The `orders.#` worked example. RabbitMQ: one binding, declared at runtime, nobody else touched. Kafka: topic sprawl, or filter-everything-client-side, or a stream-processing job writing a derived topic. Plus the sting: **regex topic subscription doesn't work with the share consumer.**

Also here: 5 protocols in one broker (AMQP 1.0/0-9-1, MQTT, STOMP, stream) vs 1 — the IoT/browser/JMS edge cases where Kafka means deploying three more components (MQTT broker, WebSocket proxy, JMS bridge). The RabbitMQ JMS queue type is Tanzu (commercial).

### 5. The PHP reality check ← **this is what makes it *our* talk**
- `php-amqplib`: pure PHP + `ext-sockets`. Symfony's built-in AMQP transport needs `ext-amqp`, a C extension. `php-rdkafka`: a C extension plus a matching `librdkafka` on every image and laptop; last stable release Nov 2024.
- **No official PHP stream-protocol client** (one young community client). You consume RabbitMQ streams over AMQP with `x-queue-type: stream` and give up native throughput, server-side offset tracking, single active consumer, and publishing dedup. Native performance from PHP = the community client or a Go/Java sidecar.
- **No PHP AMQP 1.0 client** — so RabbitMQ's newest per-message features (modified outcome, per-message delay) are out of reach too.
- **The parallelism ceiling.** In PHP's default one-process-one-consumer model, Kafka consumer groups cap you at the partition count — 12 partitions means worker #13 does nothing. Java has the same ceiling but cheap in-process fan-out; PHP's escape hatches (RoadRunner, Swoole, AMPHP) cost per-key ordering or a new runtime. "Just add partitions": fixed up front, can't shrink, increasing remaps keys. RabbitMQ: boot 200 workers on one queue; autoscale on queue depth with KEDA. Share groups would fix this, but librdkafka's share consumer is Preview and php-rdkafka has no binding.
- Symfony Messenger ships an AMQP transport; Kafka needs third-party. Laravel: both third-party; Horizon is Redis-only.

---

## Demos (2, both in PHP)

| # | Demo | What it proves |
|---|---|---|
| **A** (4 min) | **"Retry this one job"** — publish 5 jobs to a quorum queue declared with `x-delayed-retry-type=failed`, `x-delayed-retry-min=3000`, `x-delayed-retry-max=10000`, `x-delivery-limit=3`, `x-dead-letter-exchange`. Job #3 fails → consumer calls `$msg->reject(true)` → retried with backoff while 4 and 5 sail through → dead-lettered after the limit. Then route by reason: the consumer republishes with an `x-failure-reason` header to a headers exchange (bind with `x-match: all-with-x`: plain `all` ignores `x-` headers and every DLQ gets everything) → reason-specific DLQ, then acks (publisher confirms on). Label it app-level: native consumer annotation needs AMQP 1.0, which PHP doesn't have. No live Kafka half — slide 8 shows the retry-topic pattern instead (built in [demo 03](../demo/03-retry-dlq/README.md): 3 topics, 2 consumers, ~95 lines). | Points 2 and 5, viscerally. This is the slide people screenshot. |
| **B** (6 min) | **Scale the consumers** — 200 jobs on one RabbitMQ queue; every worker sets `basic_qos(0, 10, false)` and simulates an I/O-bound job with `usleep(50_000)`; `bench.php 1 → 20` forks the workers and prints a per-worker table (measured: 10.3 s → 0.52 s). Same on a 4-partition Kafka topic with KIP-848 (`group.protocol=consumer`): 20 workers take 2.59 s with 16 idle, visible in `kafka-consumer-groups --describe --members`. Budget ~10 s of group settle per Kafka run. See [demo 02](../demo/02-scale-consumers/README.md). | Point 5. Hits every PHP dev where they live. |

**Demo A gotcha:** use `reject`, not `nack`. Since 4.3, `basic.nack` doesn't increment delivery-count, so job #3 would never back off or hit the delivery limit — it loops forever on stage **and starves every job behind it** (4 and 5 never run). Also: `x-delivery-limit=3` means 4 deliveries.

**Demo B gotcha:** without `basic_qos`, worker #1 grabs the whole backlog and RabbitMQ shows the same idle workers as Kafka — reliably only when workers start staggered (`bench.php 4 --backlog --prefetch=0`). CPU-bound jobs stop scaling at the laptop's core count.

*Q&A only:* `x-stream-offset` replay — reconsume yesterday's events from a RabbitMQ stream in php-amqplib, to bury "RabbitMQ can't replay." Pass `new \DateTimeImmutable('-1 day')` or `'1D'`; a PHP int is read as an offset, and a large one (e.g. a Unix timestamp) makes the consumer receive nothing at all, not even new messages. See [demo 07](../demo/07-replay/README.md).

**Setup:** `docker-compose` with pinned images — `rabbitmq:4.3.6-management`, `apache/kafka:4.3.1`, `kafbat/kafka-ui:v1.5.0` (see [compose.yaml](../compose.yaml)), and a PHP 8.4 CLI image with `install-php-extensions sockets pcntl rdkafka`. Pre-pull all images (venue Wi-Fi). Pre-record both demos as a fallback.

---

## Slide-by-slide skeleton (45 min)

| # | Slide | Min |
|---|---|---|
| 1 | Title + "what you'll leave with" + "the source is RabbitMQ's own comparison page; I'll flag what I verified" | 1 |
| 2 | **The rule you believe has been eroding since 2021 and died in Feb 2026** — 3.9 streams / 4.2 share groups | 2 |
| 3 | Smart broker vs smart consumer — the two-sentence mental model | 2 |
| 4 | Where they're the *same* + throughput reality (streams vs AMQP vs quorum, the benchmark warning) | 4 |
| 5 | **Position vs work item** — the core slide | 4 |
| 6 | The RabbitMQ per-message toolkit (table, with AMQP 1.0-only / Tanzu-only marked) | 1 |
| 7 | **Demo A** — retry one job, dead-letter by reason | 4 |
| 8 | Share groups: what they fixed, what they didn't (HOL window) + the Kafka retry-topic pattern | 3 |
| 9 | What "acknowledged" means — fsync, multi-Raft shared WAL, and the AZ caveat | 4 |
| 10 | `orders.#` — who decides | 2 |
| 11 | **Demo B** — the partition ceiling vs N workers | 6 |
| 12 | PHP client reality: php-amqplib vs php-rdkafka, no official stream client, no AMQP 1.0 | 2 |
| 13 | **Where Kafka genuinely wins**: log compaction, tiered storage, Flink/Spark/CDC ecosystem | 3 |
| 14 | **What RabbitMQ costs you**: quorum-queue consensus cost, streams lack DLX/TTL/priority → hybrid topology, delayed-message plugin gone | 2 |
| 15 | **Decision checklist** — the takeaway slide | 2 |
| 16 | Sources + what I verified | 1 |

*(43 min + 2 min buffer for switching between slides, terminals and the management UI.)*

---

## The takeaway slide (draft)

**Reach for RabbitMQ when:**
job queues & background workers · retry/backoff/DLQ · priorities, TTL · routing that changes at runtime · payments/orders (fsync) · MQTT/IoT · thousands of per-tenant queues · your workers are PHP

**Reach for Kafka when:**
log compaction (keyed changelog / state rebuild / CDC) · tiered storage (months-years of retention) · you're feeding Flink / Spark / Iceberg / ksqlDB · the data platform already runs on it

**Either:**
event streaming with replay · effectively-once pipelines · activity tracking / metrics firehose / log aggregation — **from PHP, lean Kafka**: RabbitMQ's effectively-once and native stream throughput need the stream protocol, which has no official PHP client

> Closing line: *"Start with one. Add the second when you hit something only it does — because the real cost isn't the broker, it's the second distributed system you now have to staff, secure, and upgrade."*

---

## Risks / things to prepare for

| Risk | Mitigation |
|---|---|
| "You're quoting a RabbitMQ marketing page" | Say it on slide 1. Show which claims came from **Kafka's own docs** (fsync default, Windows support, JMX). Slides 13-14 are the balance: what Kafka wins, what RabbitMQ costs. |
| Someone runs Kafka in production here and feels attacked | Frame as "different defaults, not better/worse." Lead the Kafka-wins slide with the page's own line: "These are real, they are not small, and if you need one of them then you need Kafka." |
| "We run Kafka across 3 AZs with `min.insync.replicas=2`" | Agree — that's a reasonable trade, and the page says so. The fsync point matters for single-AZ / on-prem, and RabbitMQ lets you choose per destination. |
| "Demo A is a strawman" | Slide 8 shows the Kafka retry-topic pattern. The point is who builds it — the broker or you — not that Kafka can't. |
| "Just add more partitions" | Fixed up front, can't shrink, increasing remaps keys → breaks per-key ordering. |
| "Swoole / RoadRunner / AMPHP get around the process model" | Yes — at the cost of per-key ordering or a new runtime. Most PHP shops run plain CLI workers. |
| "We already use Redis / Horizon / SQS — why either?" | If it works, stay. Reach for a broker when you need routing, fan-out, replay, or durability Redis doesn't give you. |
| Managed-service cost (MSK, Confluent Cloud, Amazon MQ, CloudAMQP) | Have the smallest HA tier price of each ready — check before the talk. |
| Benchmark argument in Q&A | Have the benchmark table ready ([02-related-research.md](02-related-research.md) §1) and the "tuning effort, not systems" line. |
| Demo fails live | Pre-record both; pre-pull images. |
| "What about ordering?" | Neither gives global ordering. Kafka = per-partition; share groups give that up. RabbitMQ = single queue + single active consumer; competing consumers, requeues and priorities all reorder. |

---

## Open questions for you (answer before I build the deck)

1. **Talk date?** Claims are pinned to Kafka 4.3 / RabbitMQ 4.3 — Kafka 4.4 (share-group DLQs) may ship before then.
2. **Do we already run either broker in production?** If yes, the framing should shift from "which to choose" to "what we're leaving on the table / what we should stop doing."
3. **Slide format** — Markdown (reveal.js / Marp) that I generate here, or Google Slides content that you paste in?
4. **Share the demo repo with attendees?** (docker-compose + PHP consumers/producers)
5. **Laravel or Symfony in the room?** The framework bullet in Point 5 depends on it.
