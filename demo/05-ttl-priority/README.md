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
# host (repo root)
demo/05-ttl-priority/reset.sh   # deletes ttl.jobs, ttl.expired, ttl.prio, ttl.dlx, group ttl.workers; recreates ttl.events (retention.ms=10000), ttl.jobs.high, ttl.jobs.low
```

UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.

### The scripts

| Script | What it does | Arguments |
|---|---|---|
| `rabbitmq/watch-expired.php` | Consumes the dead-letter queue `ttl.expired` and prints each job as it arrives, with why and from where, until Ctrl-C | none |
| `rabbitmq/publish-ttl.php <ttl-ms>...` | Publishes one job per argument to `ttl.jobs` (which nobody consumes), then exits | each argument is that job's `expiration` in ms; `-` = none, the queue's 10 s applies. `3000 3000 -` = jobs 1-2 expire after 3 s, job 3 after 10 s |
| `rabbitmq/publish-prio.php <count> <priority>` | Publishes `<count>` jobs, all with the same priority, to `ttl.prio`, then exits | `10 0` = 10 jobs at priority 0. Priority 0-31; above 31 counts as 31 |
| `rabbitmq/consume-prio.php [prefetch]` | Consumes `ttl.prio` and prints each job, 0.3 s per job, until Ctrl-C | prefetch: how many unacked jobs the broker may hand it at once, default 1 |
| `kafka/publish.php <which> <count>` | Produces `<count>` messages to one topic, then exits | `events` = topic `ttl.events` (retention 10 s), `high` = `ttl.jobs.high`, `low` = `ttl.jobs.low` |
| `kafka/peek.php [topic]` | Reads partition 0 from the start, prints every message still stored with its age, then the log start and end offsets, then exits | topic, default `ttl.events` |
| `kafka/consume.php` | The worker (group `ttl.workers`): waits until it owns the `ttl.jobs.high` partition, then checks high before every low job, 0.3 s per job, until Ctrl-C | none |

Jobs do no real work: both priority workers print each job and sleep 0.3 s, which leaves time for a later urgent job to cut in.

Reading a dead-letter line, e.g. `[dlq] job 1 (ttl 3000 ms), published 05:49:36 -> dead-lettered 05:49:39, reason=expired, from ttl.jobs`:

- `job 1 (ttl 3000 ms)`: the 1st argument of `publish-ttl.php` and its expiration; `from queue: 10000` means it had none.
- `published` / `dead-lettered`: when it was published and when it reached `ttl.expired`. The gap is how long it really lived.
- `reason`, `from`: its `x-death` header: why it was dead-lettered, and from which queue.
- `p31 job 1` (`consume-prio.php`): priority 31, the 1st job of that `publish-prio.php` run.
- `offset 0: events 1, published 05:49:36  (age 312 s)` (`peek.php`): the record's offset, its body (Kafka bodies are `<which> <n>` plus publish time) and seconds since it was produced.

---

## RabbitMQ (`rabbitmq/`)

`ttl.jobs` is a quorum queue with `x-message-ttl=10000` and dead-letter exchange `ttl.dlx` → `ttl.expired`. Nobody consumes `ttl.jobs`, so everything published there expires.

### Act 1: TTL per message

```bash
# php container (/demo)

# terminal 1: consume the dead-letter queue ttl.expired and print each expired job as it arrives
php 05-ttl-priority/rabbitmq/watch-expired.php

# terminal 2: one job per argument to ttl.jobs: jobs 1-2 with expiration 3000 ms, job 3 "-" = none (the queue's 10 s).
# Wait ~10 s and watch terminal 1
php 05-ttl-priority/rabbitmq/publish-ttl.php 3000 3000 -
```

- Jobs 1 and 2 are dead-lettered after 3 s, job 3 after the queue's 10 s:
  `[dlq] job 1 (ttl 3000 ms), published 05:49:36 -> dead-lettered 05:49:39, reason=expired, from ttl.jobs`
- `php 05-ttl-priority/rabbitmq/publish-ttl.php 20000` is dead-lettered after **10 s**. A per-message TTL can only shorten the queue's.
- With a DLX configured (opt-in, as here), the expired message carries `x-death` to wherever the DLX routes it. Without one, it is dropped silently. Quorum queues dead-letter at-most-once by default; at-least-once needs `x-dead-letter-strategy=at-least-once` + `x-overflow=reject-publish`. Demo 03 builds retry on the same mechanism.

### Act 2: expiry happens at the head only

```bash
# php container (/demo)

# terminal 2 (watch-expired.php still running in terminal 1): job 1 with expiration 8000 ms, then job 2 with 2000 ms. Wait ~8 s
php 05-ttl-priority/rabbitmq/publish-ttl.php 8000 2000
```

- Both are dead-lettered **in the same second, 8 s later**. Job 2 (2 s TTL) sat expired behind job 1 until job 1 left the head.
- The UI shows `ttl.jobs` at 2 for the full 8 s, though job 2 expired at 2 s.
- Fix: don't mix TTLs in one queue. Use one queue per TTL, or only `x-message-ttl`, where head-first is oldest-first anyway.

### Act 3: priority

```bash
# php container (/demo)

# terminal 2: "10 0" = 10 low jobs at priority 0 to ttl.prio; nobody is consuming yet
php 05-ttl-priority/rabbitmq/publish-prio.php 10 0

# terminal 2: "1 31" = 1 urgent job at priority 31, the highest
php 05-ttl-priority/rabbitmq/publish-prio.php 1 31

# terminal 2: consume ttl.prio with prefetch 1 (no argument = 1) and print the jobs in the order they come out
php 05-ttl-priority/rabbitmq/consume-prio.php
```

- `p31 job 1` comes out first, then `p0 job 1` … `p0 job 10`.

```bash
# php container (/demo)

# Ctrl+C consume-prio.php first: a running consumer takes each message as it arrives, so nothing waits to be reordered

# terminal 2: 1 job at each priority, published in this order: 0, 4, 5, 20, 30, 31, 50, 255
for p in 0 4 5 20 30 31 50 255; do php 05-ttl-priority/rabbitmq/publish-prio.php 1 $p; done

# terminal 2: consume them with prefetch 1. Expect p31, p50, p255, p30, p20, p5, p4, p0, then Ctrl+C
php 05-ttl-priority/rabbitmq/consume-prio.php

# terminal 3, start it first: consume ttl.prio with prefetch "1" (the broker hands it 1 unacked job at a time)
php 05-ttl-priority/rabbitmq/consume-prio.php 1

# terminal 2: 10 jobs at priority 0, then 0.5 s later 1 job at priority 31. Watch where p31 lands in terminal 3
php 05-ttl-priority/rabbitmq/publish-prio.php 10 0; sleep 0.5; php 05-ttl-priority/rabbitmq/publish-prio.php 1 31

# Ctrl+C terminal 3 and repeat both, with prefetch 10 in terminal 3: php 05-ttl-priority/rabbitmq/consume-prio.php 10
```

- 32 strict levels; anything above 31 counts as 31, and equal levels keep publish order.
- With prefetch 1, p31 runs about **3rd**: job 1 was done and job 2 was already in flight when it arrived. With prefetch 10 it runs **11th**: the 10 low jobs had already been handed to the consumer. Priority only reorders what is still in the queue.

Ctrl+C `watch-expired.php` and `consume-prio.php` before moving on: `reset.sh` deletes their queues.

---

## Kafka (`kafka/`)

A partition's log is a chain of segment files. Only the newest one, the *active* segment, is written to. It rolls to a new file after `segment.ms` (7 days) or `segment.bytes` (1 GB). Retention deletes whole segments from the front of the log. The first offset still stored is the *log start offset*, which `peek.php` prints.

### Act 1: retention isn't TTL

```bash
# php container (/demo)

# terminal 1: "events 5" = 5 messages to topic ttl.events, which has retention.ms=10000 (10 s)
05-ttl-priority/kafka/run publish.php events 5

# terminal 1: wait 30 s, then print what ttl.events still stores (reads partition 0 from the start, prints, exits)
sleep 30; 05-ttl-priority/kafka/run peek.php
```

- After 30 s all 5 are usually still there: `5 message(s) still in ttl.events, log start offset 0, end offset 5`. (If you already see 0, a retention check happened to fire in those 30 s. Run `demo/05-ttl-priority/reset.sh` on the host and try again.)
- Run `05-ttl-priority/kafka/run peek.php` every minute. They disappear only when the broker's retention check runs (`log.retention.check.interval.ms`, every 5 min), so up to ~5 min late: `0 message(s) still in ttl.events, log start offset 5, end offset 5`.
- The broker log says why (host): `docker compose logs kafka | grep "ttl.events.*retention time" | tail -1` →
  `Deleting segment LogSegment(baseOffset=0, ...) due to log retention time 10000ms breach based on the largest record timestamp in the segment`

### Act 2: one fresh message keeps old ones alive

```bash
# host (repo root)

# a host terminal: start from an empty ttl.events (this also deletes the RabbitMQ queues)
demo/05-ttl-priority/reset.sh

# php container (/demo)

# terminal 1: 5 messages to ttl.events
05-ttl-priority/kafka/run publish.php events 5

# terminal 2: keep the topic busy: 1 more message to ttl.events every 5 s, until Ctrl+C
while true; do 05-ttl-priority/kafka/run publish.php events 1; sleep 5; done

# terminal 1: wait 310 s (always spans one retention check after the first 5 expired), then peek and print only
# its first line (the oldest message still stored) and last line (the count and offsets)
sleep 310; 05-ttl-priority/kafka/run peek.php | sed -n '1p;$p'
```

- The first message is still readable at **over 300 s old** with a 10 s retention: `offset 0: events 1, ... (age 3xx s)`, `log start offset 0`.
- Retention deletes a segment only when its **newest** record is older than `retention.ms`. The loop keeps the newest record fresh, so the whole segment and every old record in it stays. With the defaults (`segment.ms` 7 days, `segment.bytes` 1 GB), a topic that gets a steady trickle never fills a segment, so it keeps "expired" data for up to 7 days. Smaller segments get you closer to the retention you asked for.
- Stop the loop: the first check after the newest record turns 10 s old deletes everything, as in Act 1.

### Act 3: priority you build

```bash
# php container (/demo)

# terminal 1: "low 10" = 10 jobs to topic ttl.jobs.low
05-ttl-priority/kafka/run publish.php low 10

# terminal 1: "high 1" = 1 job to topic ttl.jobs.high
05-ttl-priority/kafka/run publish.php high 1

# terminal 1: the worker waits until it owns the ttl.jobs.high partition, then checks high before every low job
05-ttl-priority/kafka/run consume.php
```

- `high 1` first, then `low 1` … `low 10`.
- Live: leave `consume.php` running, then `05-ttl-priority/kafka/run publish.php low 10; sleep 1; 05-ttl-priority/kafka/run publish.php high 2`. The two high jobs cut in partway through the low ones (after `low 5` in our runs; it depends on `kafka/run` startup).
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
