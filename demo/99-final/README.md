# Demo 99: the comparison, and what goes on stage

Read this after 01-10. It folds every demo's result into one matrix, scores each demo for the stage, and lists what the demos proved wrong in the proposal.

> They converged on streaming. The real difference is **how much the broker knows about each individual message**, and in PHP, **client maturity and the process model**.

## 1. The matrix (every number measured in these demos)

| # | Question | RabbitMQ 4.3 | Kafka 4.3 | Verdict |
|---|---|---|---|---|
| 01 | What is a message? | queue: a work item, ack deletes it. Stream: a position | always a position; a group's commit moves a cursor | RabbitMQ offers both models |
| 02 | 20 PHP workers, 200 × 50 ms jobs | **0.52 s**, 20 busy | **2.59 s**, 16 idle on 4 partitions; 0.52 s only after `--alter` to 20 (can't shrink back) | **RabbitMQ**, for PHP |
| 03 | Retry one job with backoff, then DLQ | 5 queue arguments + `reject(true)`; backoff 3/6/9 s by the broker | 3 topics, 2 consumers, ~95 lines; fixed delay per topic | **RabbitMQ** |
| 04 | A new team wants `orders.*.vn` | one `queue_bind` at runtime, publisher untouched | filter client-side (read 8, kept 3), or topic per slice + regex (new topic seen after 7-11 s) | **RabbitMQ** |
| 05 | TTL and priority | per message, dead-lettered with reason `expired`; 32 priority levels | `retention.ms=10000` still readable at 4.5 min; priority = one topic per level + your loop | **RabbitMQ** |
| 06 | One slow / poison job | other workers route around it (order lost); single active consumer = order + blocking | blocks its partition (5, 7, 9 waited 8 s); poison = crash loop forever | **trade-off**: ordering and HOL are one coin |
| 07 | Reprocess since 10:00 | stream replays, but from PHP **you** store the offset (no server-side tracking over AMQP 0-9-1) | broker stores offsets; `--reset-offsets`, `offsetsForTimes()`, exact | **Kafka**, from PHP |
| 08 | Kafka's real queue? | quorum queue: per-message ack, delivery limit, routable DLQ | share groups: 3 members on 1 partition all busy (uneven, e.g. 2/5/2), lock 30 s, archive after 5, no DLQ until 4.4, **no PHP client** | gap closing, not from PHP |
| 09 | Latest value per key | no: a stream keeps every version | compaction: 7 records → latest per key ~23 s later; rebuild state in ~500 ms | **Kafka** |
| 10 | Leader dies mid-publish | quorum queue: LOST 0 in every scenario; without a majority, confirms just stop | `acks=1` + followers lagging: **LOST ~61,000 with 0 producer errors**; `acks=all` + `min.insync.replicas=2`: LOST 0 | **tie when configured**; Kafka's defaults (`min.insync.replicas=1`) bite. fsync vs page cache can't be shown in Docker |
| — | PHP client | php-amqplib: pure PHP | php-rdkafka: C extension + librdkafka in every image | **RabbitMQ** for setup; both work |

## 2. Score each demo for the stage

Fill **Your notes** while you learn. The scores are my starting point.

| # | Demo | Stage time | Wow | Dead air | Risk | My pick | Your notes |
|---|---|---|---|---|---|---|---|
| 01 | queue vs stream | 8 min | medium | ~6 s Kafka settle | low | slide, not live | |
| 02 | scale consumers | 6 min | **high**: 0.52 s vs 2.59 s, 16 idle | ~20 s per Kafka run | low | **live (Demo B)** | |
| 03 | retry + DLQ | 4-6 min | **high**: 5 args vs 95 lines | ~20 s of backoff | medium: `all-with-x`, `reject` not `nack` | **live (Demo A)** | |
| 04 | routing | 3-6 min | medium | ~5-10 s regex pickup | low | slide; RabbitMQ Act 2 if time | |
| 05 | TTL + priority | 7 min | medium | Kafka side ~5 min | Kafka side unusable live | RabbitMQ priority only, if time | |
| 06 | head-of-line | 8 min | **high**: prefetch causes HOL too | 8 s per slow job | low | **backup / 3rd demo** | |
| 07 | replay | 6 min | low-medium | short | low | Q&A pocket | |
| 08 | share groups | 8 min | medium | 31 s lock wait | **high**: orphan CLI consumers, batching skew | Q&A pocket | |
| 09 | log compaction | 6 min | medium | ~23 s cleaner wait | medium | slide 13, Q&A pocket | |
| 10 | durability | 6 min | **high**: 61k lost, 0 errors | ~10 s failover | **high**: 3-node clusters, freeze choreography, ~1.5 GB extra RAM | pre-recorded, Q&A pocket | |

## 3. Recommended run-of-show (~12 min of the 45)

1. **Demo A = 03 Act 1** (4 min): job #3 retried at +3 / +9 / +18 s while 4 and 5 sail through, then `retry.dead`. Show the Kafka side as a slide: the Compare table from 03 ("5 arguments vs ~95 lines").
2. **Demo B = 02** (6 min): RabbitMQ 1 → 20, then Kafka 20 → 16 idle in `--describe --members`. Pre-run the Kafka 20-worker bench and keep the output on screen if the settle wait drags.
3. **If time: 06 RabbitMQ Act 2** (2 min): prefetch 10 makes jobs 5, 7, 9 wait behind the slow job while B idles. It sets up the honest point that ordering and HOL come together.

Pocket for Q&A: 07 ("RabbitMQ can't replay"), 08 ("Kafka has queues now"), 09 ("so when is Kafka better?"), 10 ("is RabbitMQ really safer?").

## 4. What the demos proved wrong in the proposal

| Claim in `docs/03-topic-proposal.md` | What happened | Demo |
|---|---|---|
| Route DLQ by an `x-failure-reason` header to a headers exchange | `x-match: all` ignores `x-` headers, so both DLQs got everything. Use `x-match: all-with-x` | 03 |
| `nack` loops forever | also **starves every job behind it** (4 and 5 never ran) | 03 |
| `x-delivery-limit=3` | means 4 deliveries. Without it, quorum queues default to 20, then **drop silently** without a DLX | 03, 06 |
| Without `basic_qos`, worker #1 grabs the backlog | only when workers start staggered; all at once, it sometimes doesn't | 02 |
| Demo B uses `cooperative-sticky` | the demos use KIP-848 (`group.protocol=consumer`); budget ~10 s settle per run | 02 |
| A large PHP int as `x-stream-offset` "silently attaches past the end" | the consumer receives **nothing**, not even new messages | 07 |

Also learned (not in the proposal):

- Quorum-queue priorities on 4.3: 32 levels, no queue argument; values above 31 count as 31 (05).
- Kafka 4.3.1 time retention deletes the active segment too; the 5-minute `log.retention.check.interval.ms` is what keeps "expired" messages readable (05).
- Share groups default to `share.auto.offset.reset=latest`, and `batch_optimized` acquire mode lets one member take a whole batch; `record_limit` + `max.poll.records=1` behaves like prefetch 1 (08).
- RabbitMQ `x-delivery-limit=5` = 6 deliveries; Kafka `share.delivery.count.limit=5` = 5 (08).
- Empty KIP-848 groups survive topic deletion; `reset.sh` scripts delete their own groups (04, 07).

## 5. Before going on stage

- [ ] `docker compose pull` and `docker build -t demo-kafka-php lib` at home, not on venue Wi-Fi
- [ ] Port 5672 free (or `RABBITMQ_PORT=5673` everywhere)
- [ ] `./reset.sh` in each live demo; check no leftover Kafka CLI consumer: `docker compose exec kafka ps aux | grep -c Console`
- [ ] Terminals pre-opened per act, font size up, RabbitMQ UI and kafka-ui logged in
- [ ] Pre-record every live demo as a fallback
- [ ] Optional, untested: `KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS=1000` + `KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS=1000` in `compose.yaml` to shorten the KIP-848 settle wait. Re-measure 02 if you try it.

## Checklist

- [ ] I finished demos 01-10 and filled **Your notes**
- [ ] I can defend every **Verdict** in the matrix, including the two where Kafka wins
- [ ] I picked the live demos and timed them end to end
- [ ] I updated the slide skeleton in `docs/03-topic-proposal.md` with the picks
