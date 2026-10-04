# Sharing session — topic proposal (DRAFT, awaiting approval)

## Proposed title

> ### "Smart Broker vs Smart Consumer — what RabbitMQ and Kafka actually disagree about in 2026"
> *and why the answer is different when your workers are PHP*

**Audience:** senior-ish PHP devs
**Length:** 45 min talk + 15 min Q&A
**Format:** slides + 2 live demos in PHP

---

## The one-sentence takeaway

> They converged on streaming. The real difference is **how much the broker knows about each individual message** — and in PHP, the second difference is **client maturity and the process model**.

If people remember only that, the session worked.

---

## Why this framing (and not "RabbitMQ vs Kafka: which is better")

1. **It kills a dead rule of thumb.** Everyone in the room believes "Kafka = streaming, RabbitMQ = queueing." That was true in 2019 and is false now — RabbitMQ 3.9 added streams (2021), Kafka 4.2 added share groups (Feb 2026). Correcting a belief the audience holds is the strongest possible opener.
2. **It's decision-useful.** People leave with a checklist for the next architecture review, not trivia.
3. **It's honest.** The source is a vendor page. Presenting it *as* an argument — and then stress-testing it — is more credible and more interesting than reading it out.
4. **It lands in PHP.** The parallelism ceiling, `php-rdkafka` vs `php-amqplib`, and the missing PHP stream client are things this room will hit personally.

---

## Main points (5)

### 1. They converged — and that's the news
Same playbook on both sides: batching as the unit of work, one binary format end-to-end, `sendfile()` zero-copy, page cache over heap, publisher-side dedup, and Raft for metadata (KRaft vs Khepri). **Throughput is not the differentiator.** Kill the "RabbitMQ is slow" myth here with the numbers (single stream: millions/sec) *and* immediately caveat that every public benchmark is really measuring tuning effort.

### 2. A message is a *position* vs a message is a *work item* ← **the core of the talk**
Kafka: a message is a position in a shared log; its fate is coupled to its neighbours; the broker never opens the envelope.
RabbitMQ: a message is an independent item of work; the broker parses the envelope.

Everything downstream follows: TTL, 32 priorities, delays, delayed retry with per-message override, deferral by token, dead-letter **routing** through exchanges, consumer-annotated returns, and — the underrated one — **acking deletes, so disk comes back and the queue converges to empty**. Kafka sizes disk by retention window, not by backlog.

Then the fair part: **Kafka share groups (4.2 GA) close some of this** — per-message ack, delivery counting, acquisition locks. And the part they don't close: ack ≠ delete, no TTL/priorities/delays/DLQ routing, and head-of-line blocking **reduced, not eliminated** (the 2000-offset in-flight window pins on the oldest unfinished message and the partition stops being fetched).

### 3. "Acknowledged" doesn't mean the same thing on both sides
Kafka `acks=all` = in the **page cache** of every in-sync replica. Kafka's own docs recommend disabling application fsync. Lose power across correlated replicas → confirmed messages are gone.
RabbitMQ quorum queues = Raft majority has **written and flushed** before the confirm.
And the *why*: Kafka's log is per-partition so each fsync is separate; RabbitMQ's Ra is multi-Raft over a **shared WAL** — thousands of queues, one batched fsync.

This is the slide for anyone doing payments or orders.

### 4. Who decides what a consumer gets — the broker or the producer
The `orders.#` worked example. RabbitMQ: one binding, declared at runtime, nobody else touched. Kafka: topic sprawl, or filter-everything-client-side, or a stream-processing job writing a derived topic. Plus the sting: **regex topic subscription doesn't work with the share consumer.**

Also here: 5 protocols in one broker (AMQP 1.0/0-9-1, MQTT, STOMP, stream) vs 1 — the IoT/browser/JMS edge cases where Kafka means deploying two more components.

### 5. The PHP reality check ← **this is what makes it *our* talk**
- `php-amqplib`: pure PHP, `composer require`, done. `php-rdkafka`: a C extension on every image and laptop, with multi-year open issues about long-running consumers.
- **No official PHP stream-protocol client.** You consume RabbitMQ streams over AMQP with `x-queue-type: stream` and give up throughput, server-side offset tracking, and super-stream partitioning. Native performance from PHP = a Go/Java sidecar.
- **The parallelism ceiling.** PHP is one process, one consumer. Kafka classic consumer groups cap you at the partition count — 12 partitions means worker #13 does nothing, and repartitioning changes key→partition mapping. RabbitMQ: boot 200 workers on one queue; autoscale on queue depth. Share groups fix this in Java first, PHP much later.
- Symfony Messenger ships an AMQP transport; Kafka needs third-party.

---

## Demos (2, both in PHP, ~6 min each)

| # | Demo | What it proves |
|---|---|---|
| **A** | **"Retry this one job in 90 seconds"** — publish 5 jobs to a quorum queue; job #3 fails. Show it delayed + retried with backoff while 4 and 5 sail through, then dead-lettered via a headers exchange into a *specific* DLQ based on the annotation the consumer added. Then the Kafka version: the same failure pins the start offset and the partition stalls. | Point 2, viscerally. This is the slide people screenshot. |
| **B** | **Scale the consumers** — one RabbitMQ queue, boot 1 → 20 PHP workers with supervisord, watch throughput scale linearly on the management UI. Same with a 4-partition Kafka topic: workers 5-20 idle. | Point 5. Hits every PHP dev where they live. |

*Optional 3rd if time:* `x-stream-offset` replay — reconsume yesterday's events from a RabbitMQ stream in php-amqplib, to bury "RabbitMQ can't replay."

**Setup:** `docker-compose` with `rabbitmq:4-management` + a KRaft-mode Kafka, and a small PHP 8.3 image. Pre-record both demos as a fallback.

---

## Slide-by-slide skeleton (45 min)

| # | Slide | Min |
|---|---|---|
| 1 | Title + "what you'll leave with" | 1 |
| 2 | **The rule you believe is 5 years out of date** — 2019 vs 2026 timeline (3.9 streams / 4.2 share groups) | 3 |
| 3 | Smart broker vs smart consumer — the two-sentence mental model | 2 |
| 4 | Where they're the *same*: 6 shared techniques | 4 |
| 5 | Throughput myth + the benchmark warning | 3 |
| 6 | **Position vs work item** — the core slide | 4 |
| 7 | The RabbitMQ per-message toolkit (table) | 3 |
| 8 | **Demo A** — retry one job, dead-letter by reason | 6 |
| 9 | Share groups: what they fixed, what they didn't (HOL blocking window) | 4 |
| 10 | What "acknowledged" means — fsync, and the multi-Raft shared WAL | 4 |
| 11 | `orders.#` — who decides | 3 |
| 12 | **Demo B** — the partition ceiling vs N workers | 6 |
| 13 | PHP client reality: php-amqplib vs php-rdkafka, no stream client | 3 |
| 14 | **Where Kafka genuinely wins**: log compaction, tiered storage, Flink/Spark/CDC ecosystem | 3 |
| 15 | **Decision checklist** — the takeaway slide | 2 |
| 16 | Sources + "the page is written by RabbitMQ; here's what I verified" | 1 |

*(Sums over 45 — cut slide 4 to two bullets and drop the optional demo if running long.)*

---

## The takeaway slide (draft)

**Reach for RabbitMQ when:**
job queues & background workers · retry/backoff/DLQ · priorities, TTL, scheduled delivery · RPC · routing that changes at runtime · payments/orders (fsync) · MQTT/IoT · thousands of per-tenant queues · your workers are PHP

**Reach for Kafka when:**
log compaction (keyed changelog / state rebuild / CDC) · tiered storage (months-years of retention) · you're feeding Flink / Spark / Iceberg / ksqlDB · the data platform already runs on it

**Either:**
event streaming with replay · effectively-once pipelines · activity tracking / metrics firehose / log aggregation

> Closing line: *"Start with one. Add the second when you hit something only it does — because the real cost isn't the broker, it's the second distributed system you now have to staff, secure, and upgrade."*

---

## Risks / things to prepare for

| Risk | Mitigation |
|---|---|
| "You're quoting a RabbitMQ marketing page" | Say it on slide 2. Show which claims came from **Kafka's own docs** (fsync default, Windows support, JMX). Have slide 14 ready — the honest Kafka-wins list. |
| Someone runs Kafka in production here and feels attacked | Frame as "different defaults, not better/worse." Lead the Kafka-wins slide with "these are real and substantial." |
| Benchmark argument in Q&A | Have the three conflicting benchmark tables ready (`02-related-research.md` §1) and the "tuning effort, not systems" line. |
| Demo fails live | Pre-record both. |
| "What about ordering?" | Have the answer ready: neither gives global ordering. Kafka = per-partition. RabbitMQ = single queue + single consumer; competing consumers break order. |

---

## Open questions for you (answer before I build the deck)

1. **Talk vs workshop** — 45-min talk with 2 demos, or 90-min hands-on with a repo they clone?
2. **Do we already run either broker in production?** If yes, the framing should shift from "which to choose" to "what we're leaving on the table / what we should stop doing."
3. **Slide format** — Markdown (reveal.js / Marp) that I generate here, or Google Slides content that you paste in?
4. **Do you want the demo repo built** (docker-compose + PHP consumers/producers), or just the deck?
