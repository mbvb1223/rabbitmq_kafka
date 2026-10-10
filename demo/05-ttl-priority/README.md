# Demo 05: TTL and priority (~20 min, mostly waiting on Kafka's 5-min retention check)

A RabbitMQ queue knows every message, so expiry and priority are properties **of the message**. Kafka only knows the log: its "TTL" is topic retention, applied to whole segment files on a timer, and priority is something you build out of topics.

## You'll learn

- Per-message `expiration` vs queue `x-message-ttl` (the shorter wins), and that expired messages are dead-lettered (if a DLX is set) with `reason=expired`
- RabbitMQ queues (classic and quorum) expire a message only at the head, so a short TTL waits behind a long one (in 4.3 quorum queues: behind a long one at the same priority)
- RabbitMQ 4.3 quorum queues have 32 strict priorities with no queue argument, and prefetch decides how much priority can still reorder
- Kafka `retention.ms` deletes whole segments, judged by their newest record, on a 5-minute timer, and tells no one
- How to fake priority in Kafka with two topics, and why a naive consumer on both topics doesn't work

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| TTL | `expiration` per message, `x-message-ttl` per queue | `retention.ms` per topic |
| What expires | one message | a whole segment, once its **newest** record is older than `retention.ms` |
| After expiry | dead-lettered via `ttl.dlx`, `x-death` reason `expired` | deleted; nobody is told |
| Priority | `priority` 0-31 per message, built into quorum queues | none: two topics + a worker that checks high first |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh   # deletes ttl.jobs, ttl.expired, ttl.prio, ttl.dlx, group ttl.workers; recreates ttl.events (retention.ms=10000), ttl.jobs.high, ttl.jobs.low
```

UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.

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
- With a DLX configured (opt-in, as here), the expired message carries `x-death` to wherever the DLX routes it. Without one, it is dropped silently. Quorum queues dead-letter at-most-once by default; at-least-once needs `x-dead-letter-strategy=at-least-once` + `x-overflow=reject-publish`. Demo 03 builds retry on the same mechanism.

### Act 2: expiry happens at the head only

```bash
php publish-ttl.php 8000 2000   # terminal 2; watch-expired.php still running in terminal 1
```

- Both are dead-lettered **in the same second, 8 s later**. Job 2 (2 s TTL) sat expired behind job 1 until job 1 left the head.
- The UI shows `ttl.jobs` at 2 for the full 8 s, though job 2 expired at 2 s.
- Fix: don't mix TTLs in one queue. Use one queue per TTL, or only `x-message-ttl`, where head-first is oldest-first anyway.

### Act 3: priority

```bash
php publish-prio.php 10 0         # 10 low jobs (p0), nobody consuming yet
php publish-prio.php 1 31         # 1 urgent job
php consume-prio.php              # prefetch 1
```

- `p31 job 1` comes out first, then `p0 job 1` … `p0 job 10`.

```bash
# Ctrl+C consume-prio.php first: a running consumer takes each message as it arrives, so nothing waits to be reordered
for p in 0 4 5 20 30 31 50 255; do php publish-prio.php 1 $p; done
php consume-prio.php              # p31, p50, p255, p30, p20, p5, p4, p0; Ctrl+C

php consume-prio.php 1                                             # terminal 1, start it first
php publish-prio.php 10 0; sleep 0.5; php publish-prio.php 1 31   # terminal 2
# Ctrl+C terminal 1 and repeat with: php consume-prio.php 10
```

- 32 strict levels; anything above 31 counts as 31, and equal levels keep publish order.
- With prefetch 1, p31 runs about **3rd**: job 1 was done and job 2 was already in flight when it arrived. With prefetch 10 it runs **11th**: the 10 low jobs had already been handed to the consumer. Priority only reorders what is still in the queue.

Ctrl+C `watch-expired.php` and `consume-prio.php` before moving on: `reset.sh` deletes their queues.

---

## Kafka (`cd kafka`)

A partition's log is a chain of segment files. Only the newest one, the *active* segment, is written to. It rolls to a new file after `segment.ms` (7 days) or `segment.bytes` (1 GB). Retention deletes whole segments from the front of the log. The first offset still stored is the *log start offset*, which `peek.php` prints.

### Act 1: retention isn't TTL

```bash
./run publish.php events 5        # ttl.events has retention.ms=10000
sleep 30; ./run peek.php          # reads partition 0 from the start, prints, exits
```

- After 30 s all 5 are usually still there: `5 message(s) still in ttl.events, log start offset 0, end offset 5`. (If you already see 0, a retention check happened to fire in those 30 s. Run `../reset.sh` and try again.)
- Run `./run peek.php` every minute. They disappear only when the broker's retention check runs (`log.retention.check.interval.ms`, every 5 min), so up to ~5 min late: `0 message(s) still in ttl.events, log start offset 5, end offset 5`.
- The broker log says why: `docker compose logs kafka | grep "ttl.events.*retention time" | tail -1` →
  `Deleting segment LogSegment(baseOffset=0, ...) due to log retention time 10000ms breach based on the largest record timestamp in the segment`

### Act 2: one fresh message keeps old ones alive

```bash
../reset.sh                                                  # also deletes the RabbitMQ queues
./run publish.php events 5                                   # terminal 1
while true; do ./run publish.php events 1; sleep 5; done     # terminal 2: keep the topic busy
sleep 310; ./run peek.php | sed -n '1p;$p'                   # terminal 1: 310 s always spans one retention check after the first 5 expired
```

- The first message is still readable at **over 300 s old** with a 10 s retention: `offset 0: events 1, ... (age 3xx s)`, `log start offset 0`.
- Retention deletes a segment only when its **newest** record is older than `retention.ms`. The loop keeps the newest record fresh, so the whole segment and every old record in it stays. With the defaults (`segment.ms` 7 days, `segment.bytes` 1 GB), a topic that gets a steady trickle never fills a segment, so it keeps "expired" data for up to 7 days. Smaller segments get you closer to the retention you asked for.
- Stop the loop: the first check after the newest record turns 10 s old deletes everything, as in Act 1.

### Act 3: priority you build

```bash
./run publish.php low 10
./run publish.php high 1
./run consume.php                 # waits for its high partition, then polls high before every low message
```

- `high 1` first, then `low 1` … `low 10`.
- Live: leave `consume.php` running, then `./run publish.php low 10; sleep 1; ./run publish.php high 2`. The two high jobs cut in partway through the low ones (after `low 5` in our runs; it depends on `./run` startup).
- A naive consumer on both topics doesn't work: it ran all 10 low jobs (already fetched) before `high 1` (seen while building this; no script reproduces it). Two consumers in one process (~50 lines) is the simplest fix. One consumer that `pausePartitions()` low while it checks high also works, at the cost of refetching low.

---

## Compare

| | RabbitMQ | Kafka |
|---|---|---|
| TTL is set on | each message, or the queue | the topic (retention); a per-message TTL is your consumer comparing the record timestamp and skipping stale ones |
| Expires | one message, once it reaches the head | a segment, at the next retention check after its newest record expires |
| Expired message | dead-lettered with a reason if the queue has a DLX, otherwise dropped | deleted; a lagging consumer is moved by `auto.offset.reset`, silently by default (`latest` also skips everything still retained; `error` makes it loud) |
| Priority | 32 levels per message (4.3), in one queue | one topic per level + your own polling loop |
| Priority granularity | per message | per topic; nothing within a partition |

## Talking points

- **TTL is a business rule, retention is disk management.** "This OTP is useless after 60 s" is a RabbitMQ TTL. "Keep 7 days on disk" is Kafka retention. Don't use one as the other. In Kafka the OTP rule lives in the consumer: if now - timestamp > 60 s, skip it (it still sits in the log until retention).
- **The split is queue vs log, not RabbitMQ vs Kafka:** a RabbitMQ *stream* has no per-message TTL, priority or DLX, and `x-max-age` deletes whole segments (but never the last one): talk slide 14.
- **Priority is decided when the broker hands a message out**, so a large prefetch quietly turns it off.
- **Starvation is on both sides:** RabbitMQ 4.3 priorities are strict too, so a steady high stream starves low (its docs say to use one queue per class if low must progress). Kafka-specific: you write the polling loop, and with N workers each one only checks the high partitions it was assigned.
- Retention also deletes **unprocessed** messages, with no DLQ and no warning. Share groups (demo 08) don't change that, and they add no TTL or priority.

## Gotchas (hit while building this)

- RabbitMQ expires only at the head (Act 2; per priority level in 4.3 quorum queues), and the queue depth keeps counting expired-but-stuck messages.
- Priorities above 31 aren't rejected, they're treated as 31. Before 4.3, quorum queues had 2 levels (0-4 normal, 5+ high), served 2:1 so normal never starved. 4.3 is strict, so an upgrade can starve low-priority work.
- A message with no `priority` counts as 4 on quorum queues (0 on classic), so it outranks p0-p3.
- Kafka deleted the **active** segment too: on an idle topic, once every record in it was older than `retention.ms`, the retention check deleted it and rolled a new one. On a steadily written one, `segment.ms`/`segment.bytes` decide how long old records stay (Act 2).
- A naive single Kafka consumer on `ttl.jobs.high` + `ttl.jobs.low` gives no priority: librdkafka prefetches, and `consume()` returns messages in fetch order.
- `peek.php` uses `assign()`, not `subscribe()`: no group join (KIP-848 takes a few seconds) and no commits.

## Checklist

- [ ] RabbitMQ Act 1: saw 3 s and 10 s expiries arrive in `ttl.expired` with `reason=expired`; I know which TTL wins
- [ ] RabbitMQ Act 2: saw the 2 s message wait 8 s behind the head
- [ ] RabbitMQ Act 3: saw p31 jump the queue, and saw prefetch 10 cancel it
- [ ] Kafka Acts 1-2: messages outlived `retention.ms`; found the `largest record timestamp` log line
- [ ] Kafka Act 3: high jumped ahead; I can name two limits of the pattern
- [ ] I can explain in one sentence: TTL is a business rule, retention is disk management
