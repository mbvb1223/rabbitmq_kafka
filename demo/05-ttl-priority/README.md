# Demo 05: TTL and priority (~7 min)

A RabbitMQ queue knows every message, so expiry and priority are properties **of the message**. Kafka only knows the log: its "TTL" is topic retention, applied to whole segment files on a timer, and priority is something you build out of topics.

## You'll learn

- Per-message `expiration` vs queue `x-message-ttl` (the shorter wins), and that expired messages are dead-lettered with `reason=expired`
- Quorum queues expire messages only at the head: a short TTL waits behind a long one
- RabbitMQ 4.3 quorum queues have 32 strict priorities with no queue argument, and prefetch decides how much priority can still reorder
- Kafka `retention.ms` deletes whole segments, judged by their newest record, on a 5-minute timer, and tells no one
- How to fake priority in Kafka with two topics, and why one consumer on both topics doesn't work

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| TTL | `expiration` per message, `x-message-ttl` per queue | `retention.ms` per topic |
| What expires | one message | a whole segment, once its **newest** record is older than `retention.ms` |
| After expiry | dead-lettered via `ttl.dlx`, `x-death` reason `expired` | deleted; nobody is told |
| Priority | `priority` 0-31 per message, built into quorum queues | none: two topics + a worker that checks high first |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh   # deletes ttl.jobs, ttl.expired, ttl.prio; recreates ttl.events (retention.ms=10000), ttl.jobs.high, ttl.jobs.low
```

---

## RabbitMQ (`cd rabbitmq`)

`ttl.jobs` is a quorum queue with `x-message-ttl=10000` and dead-letter exchange `ttl.dlx` → `ttl.expired`. Nobody consumes `ttl.jobs`, so everything published there expires.

### Act 1: TTL per message

```bash
php watch-expired.php             # terminal 1: consumes the dead-letter queue
php publish-ttl.php 3000 3000 -   # terminal 2: two with expiration=3000, one without
```

- Jobs 1 and 2 are dead-lettered after 3 s, job 3 after the queue's 10 s:
  `[dlq] job 1 (ttl 3000 ms), published 05:49:36 -> dead-lettered 05:49:39, reason=expired, from ttl.jobs`
- `php publish-ttl.php 20000` is dead-lettered after **10 s**. A per-message TTL can only shorten the queue's.
- Nothing is lost silently: the expired message carries `x-death` to wherever the DLX routes it. Demo 03 builds retry on the same mechanism.

### Act 2: expiry happens at the head only

```bash
php publish-ttl.php 8000 2000
```

- Both are dead-lettered **in the same second, 8 s later**. Job 2 (2 s TTL) sat expired behind job 1 until job 1 left the head.
- Fix: don't mix TTLs in one queue. Use one queue per TTL, or only `x-message-ttl`, where head-first is oldest-first anyway.

### Act 3: priority

```bash
php publish-prio.php 10 0         # 10 normal jobs, nobody consuming yet
php publish-prio.php 1 31         # 1 urgent job
php consume-prio.php              # prefetch 1
```

- `p31 job 1` comes out first, then `p0 job 1` … `p0 job 10`.
- 32 strict levels: one message each at 0, 4, 5, 20, 30, 31, 50, 255 comes out as 31, 50, 255, 30, 20, 5, 4, 0. Anything above 31 counts as 31.
- Prefetch matters. Start the consumer first, publish 10 × p0, then 1 × p31 half a second later: with `php consume-prio.php 1` the urgent job runs **3rd**, with `php consume-prio.php 10` it runs **11th**. The 10 low jobs had already been handed to the consumer. Priority only reorders what is still in the queue.

---

## Kafka (`cd kafka`)

### Act 1: retention isn't TTL

```bash
./run publish.php events 5        # ttl.events has retention.ms=10000
sleep 30; ./run peek.php          # reads partition 0 from the start, prints, exits
```

- After 30 s all 5 are still there: `5 message(s) still in ttl.events, log start offset 0, end offset 5`.
- Run `./run peek.php` every minute. They disappear only when the broker's retention check runs (`log.retention.check.interval.ms`, every 5 min), so up to ~5 min late: `0 message(s) still in ttl.events, log start offset 5, end offset 5`.
- The broker log says why: `docker compose logs kafka | grep "ttl.events.*retention"` →
  `Deleting segment LogSegment(baseOffset=0, ...) due to log retention time 10000ms breach based on the largest record timestamp in the segment`

### Act 2: one fresh message keeps old ones alive

```bash
../reset.sh
./run publish.php events 5
while true; do ./run publish.php events 1; sleep 5; done   # terminal 2: keep the topic busy
./run peek.php | sed -n '1p;$p'                             # after the next retention check: first + last line
```

- Past the next check, the first message is still readable at **~5 min old** with a 10 s retention (276 s and 314 s in two runs): `offset 0: events 1, published 05:55:42  (age 276 s)`, `log start offset 0`.
- Retention deletes a segment only when its **newest** record is older than `retention.ms`. The loop keeps the newest record fresh, so the whole segment and every old record in it stays. With the defaults (`segment.ms` 7 days, `segment.bytes` 1 GB) a busy topic keeps "expired" data for days. Smaller segments get you closer to the retention you asked for.
- Stop the loop: the first check after the newest record turns 10 s old deletes everything, as in Act 1.

### Act 3: priority you build

```bash
./run publish.php low 10
./run publish.php high 1
./run consume.php                 # waits for its high partition, then polls high before every low message
```

- `high 1` first, then `low 1` … `low 10`.
- Live: leave `consume.php` running, then `./run publish.php low 10; sleep 1; ./run publish.php high 2`. The two high jobs run right after `low 5`, before `low 6`.
- It takes two consumers in one process (~50 lines). One consumer subscribed to both topics doesn't work: we saw it run all 10 low jobs (already fetched into its buffer) before `high 1`.

---

## Compare

| | RabbitMQ | Kafka |
|---|---|---|
| TTL is set on | each message, or the queue | the topic |
| Expires | one message, once it reaches the head | a segment, at the next retention check after its newest record expires |
| Expired message | dead-lettered with a reason, routable | deleted; a consumer lagging behind silently skips it |
| Priority | 32 levels per message (4.3), in one queue | one topic per level + your own polling loop |
| Priority granularity | per message | per topic; nothing within a partition |

## Talking points

- **TTL is a business rule, retention is disk management.** "This OTP is useless after 60 s" is a RabbitMQ TTL. "Keep 7 days on disk" is Kafka retention. Don't use one as the other.
- **Priority is decided when the broker hands a message out**, so a large prefetch quietly turns it off.
- **Kafka's two-topic pattern has limits:** a burst of high jobs starves low ones (no weighting), ordering between levels is gone, and with N workers each one only checks the high partitions it was assigned.
- Retention also deletes **unprocessed** messages. No DLQ, no warning (see demo 08 for share groups).

## Gotchas (hit while building this)

- Quorum queues expire only at the head (Act 2), and the queue depth keeps counting expired-but-stuck messages.
- Priorities above 31 aren't rejected, they're treated as 31. Before 4.3, quorum queues had only 2 levels (0-4 normal, 5+ high), per the RabbitMQ docs.
- Kafka deleted the **active** segment too: once every record in it was older than `retention.ms`, the retention check deleted it and rolled a new one. So `segment.ms` doesn't decide *whether* old data goes, only how finely it's grouped.
- One Kafka consumer on `ttl.jobs.high` + `ttl.jobs.low` gives no priority: librdkafka prefetches, and `consume()` returns messages in fetch order.
- `peek.php` uses `assign()`, not `subscribe()`: no group join (KIP-848 takes a few seconds) and no commits.

## Checklist

- [ ] RabbitMQ Act 1: saw 3 s and 10 s expiries arrive in `ttl.expired` with `reason=expired`; I know which TTL wins
- [ ] RabbitMQ Act 2: saw the 2 s message wait 8 s behind the head
- [ ] RabbitMQ Act 3: saw p31 jump the queue, and saw prefetch 10 cancel it
- [ ] Kafka Acts 1-2: messages outlived `retention.ms`; found the `largest record timestamp` log line
- [ ] Kafka Act 3: high jumped ahead; I can name two limits of the pattern
- [ ] I can explain in one sentence: TTL is a business rule, retention is disk management
