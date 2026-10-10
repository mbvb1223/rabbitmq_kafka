# News & research pack — RabbitMQ vs Kafka (as of 2026-10-04)

Pinned to `rabbitmq:4.3.6-management` / `apache/kafka:4.3.1`. Every file has its own `## Sources` with URLs + dates; `(unverified)` marks anything that couldn't be fetched.

## Read in this order

| # | File | Why read it |
|---|------|-------------|
| 1 | [03-comparison-posts.md](03-comparison-posts.md) | 25 comparison posts, rated ★1-5, bias + "outdated?" per post. Start with **Top 5** |
| 2 | [05-convergence-share-groups-vs-streams.md](05-convergence-share-groups-vs-streams.md) | Feature matrix: Kafka consumer group / share group / quorum queue / stream — the core of the talk |
| 3 | [09-community-qa-myths.md](09-community-qa-myths.md) | 22 audience Q&As, 18 myths vs facts, quotable hot takes, timeouts cheat-sheet |
| 4 | [08-php-clients.md](08-php-clients.md) | Library health tables + verified php-amqplib / php-rdkafka snippets + gotchas |
| 5 | [04-benchmarks.md](04-benchmarks.md) | 17 benchmarks, fairness checklist, perf-test commands for our `compose.yaml` |
| 6 | [06-case-studies.md](06-case-studies.md) | 20 company stories (6 PHP shops) + 3 stage-worthy anecdotes |
| 7 | [01-rabbitmq-news.md](01-rabbitmq-news.md) | RabbitMQ 4.1 → 4.4 timeline, Broadcom licensing, 2026 CVEs |
| 8 | [02-kafka-news.md](02-kafka-news.md) | Kafka 4.0 → 4.4 timeline, KIPs, IBM–Confluent, CVEs |
| 9 | [07-ecosystem-managed-alternatives.md](07-ecosystem-managed-alternatives.md) | Managed offerings + cost at our scale, licensing, Redpanda/NATS/SQS etc. |

## Fix in the demo code

| # | Severity | Issue | Fix | Ref |
|---|----------|-------|-----|-----|
| 1 | Blocker | php-amqplib default `queue_declare('jobs')` (non-durable, non-exclusive) closes the connection on 4.3 — `transient_nonexcl_queues` denied by default | Declare durable quorum: `queue_declare('jobs', false, true, false, false, false, new AMQPTable(['x-queue-type' => 'quorum']))` | 01, 08 |
| 2 | Major | librdkafka defaults: idempotence off (retries can reorder), auto offset store marks messages before processing | `enable.idempotence=true`; `enable.auto.offset.store=false` + store/commit after processing | 09 |
| 3 | Major | `php:8.4-cli` (Debian trixie) ships librdkafka 2.8.0 — too old for KIP-848 (GA 2.12) | Use `php:8.4-cli-alpine` (2.14.1) if showing KIP-848; Demo B (classic) is fine | 08 |
| 4 | Minor | In a local test, `nack` with requeue didn't count toward `x-delivery-limit`, `reject` did | Use `basic_reject` in the poison-message demo. Verified in demo 03: `nack` also starves the jobs behind it | 01 |
| 5 | Minor | `basic_qos(global=true)` silently per-consumer on 4.3 | Don't rely on global prefetch | 08 |

## Corrections to `../03-topic-proposal.md` / `../02-related-research.md`

- **Kafka DLQ (KIP-1191)** ships in 4.4 — RC3 vote closes 2026-10-05. If released before the talk, "no DLQ routing" becomes "opt-in DLQ, topic must start with `dlq.`, headers only by default (no payload)".
- **RabbitMQ delayed publish:** Tanzu-only on 4.3, but 4.4 (unreleased) adds `x-opt-delivery-time` to OSS quorum queues — settable from PHP. "Deferral by token" is 4.4, not 4.3.
- **"No PHP client" → "no mature PHP client":** `sigbits/php-amqp-client` (AMQP 1.0, no `modified` outcome) and `lisachenko/kafka-client` (pure PHP, claims share consumer) exist, both <5 stars.
- **php-rdkafka isn't dead:** 7.0.0alpha1 is out (breaking: `consume()` returns null on timeout); 6.0.5 (Nov 2024) still latest stable.
- **Symfony AMQP transport** uses `basic.get` (no prefetch) until `prefetch_count` lands in 8.2.

## Headline facts for slides

- **PHP can't use share groups:** librdkafka share consumer is Preview (2.15.0) with no lock renew; php-rdkafka has no binding (issue #616). Jobs >60 s get redelivered (Kafka default lock) vs RabbitMQ's 30 min `consumer_timeout`.
- **Share-group head-of-line blocking is real:** 2000-offset in-flight window measured from the oldest unacked record; retention can delete unprocessed records with no DLQ entry.
- **No fair current benchmark exists.** "Kafka 15x faster" = 2020, RabbitMQ 3.8.5 mirrored queues (removed in 4.0). OpenMessaging Benchmark's RabbitMQ driver uses auto-ack + no persistence.
- **Laptop numbers (RabbitMQ 4.3.6, 1 KB):** quorum queue ~45K msg/s, stream via AMQP ~62K, stream native ~281K.
- **Cost at 100 msg/s/month:** SQS/Pub/Sub ~$30, managed RabbitMQ $150–300, managed Kafka $145–640 (MSK Express ~$900).
- **"Everyone who left RabbitMQ rebuilt RabbitMQ":** DoorDash, Uber, Sentry built per-message acks on Kafka.

## Before the talk — re-check

- Kafka 4.4.0 released? Kafka 4.3.2 shipped?
- RabbitMQ 4.4 released? 4.3 community support ends **2026-11-30**.
- Skim r/PHP and r/laravel by hand (Reddit couldn't be fetched).
- Prepare for Q10 in 09: Vanlightly, "Kafka doesn't need fsync to be safe" — strongest counter to the durability slide.
