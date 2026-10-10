# Demo 07: replay (~6 min)

Both brokers can replay a stream. The difference is **who remembers where you were**. Kafka stores each group's committed offset on the broker. A RabbitMQ stream consumed over AMQP 0-9-1, the only mature path from PHP, doesn't: server-side offset tracking is a stream-protocol feature, so the PHP consumer has to store its own offset.

Scenario: *we shipped a bug in the billing consumer at 10:00. Fix it, then reprocess everything since 10:00.*

## You'll learn

- How to resume a RabbitMQ stream consumer when the broker doesn't track its offset, and what `next` / `first` cost you without one
- Replay by time on both sides, and why RabbitMQ can hand you a few messages from before the time you asked for
- The `x-stream-offset` trap: a PHP int is always an offset, so a unix time silently gets you nothing, forever
- Kafka's three replay tools: committed offsets, `kafka-consumer-groups --reset-offsets`, `offsetsForTimes()` + `assign()`
- Retention is the replay limit on both sides. A quorum queue can't replay at all (demo 01)

| | RabbitMQ stream (`rabbitmq/`) | Kafka topic (`kafka/`) |
|---|---|---|
| Who stores the position | your consumer (here a file, `rabbitmq/billing.offset`) | the broker, per group (`__consumer_offsets`) |
| Resume after restart | pass `x-stream-offset` = saved + 1 | automatic |
| Replay by time | `x-stream-offset` = a timestamp, or `30s` / `5m` | `--reset-offsets --to-datetime`, or `offsetsForTimes()` |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh      # deletes stream replay.events + billing.offset, recreates topic replay.events with 1 partition
```

All output is UTC (PHP and the containers). On stage, prefer relative times: `-2 minutes`, `30s`, `PT10M`.

---

## RabbitMQ (`cd rabbitmq`)

### Act 1: resume after a restart (your offset, your file)

```bash
php publish.php 5
php resume.php           # Ctrl-C while it's on event 4 or 5
php publish.php 3        # these arrive while billing is down
php resume.php           # resumes from the saved offset + 1
```

- First run: `no saved offset, starting from 'first'`, then offsets 0, 1, 2, ...
- Second run: `saved offset 3, resuming from 4`, then **event 5 (offset 4) again**, then the 3 new ones. The kill landed after the print but before the save: at-least-once, it replays one and never skips one.
- Without the file: `php consume.php next` prints nothing, so the 3 events that arrived during the downtime are missed. `php consume.php first` redoes all 8.
- `billing.offset` is the whole state. Lose it (new container, new pod) and you're back to choosing between missing and redoing. In real code, save it in the same DB transaction as the work, or make the work idempotent.

### Act 2: replay by time

```bash
php consume.php '@-2 minutes'   # '@...' becomes a DateTimeImmutable
php consume.php 30s              # interval string: the last 30 seconds
php consume.php '@-1 day'        # everything still retained
php consume.php 3                # from offset 3
php consume.php 1791610000       # the trap
```

- php-amqplib's `AMQPTable` encodes a `DateTimeInterface` as an AMQP timestamp (seconds), so `new DateTimeImmutable('-1 day')` works as the proposal says.
- **Chunk-granular:** during a 4 s burst (240K messages) we attached at `05:48:17` and the first message delivered had been published at `05:48:16.999`. The broker starts at the chunk that contains the time, so the consumer must tolerate a few older messages.
- **The trap:** `1791610000` is a unix time passed as an int. RabbitMQ reads every int as an **offset**, attaches far past the end, and receives **nothing, not even new messages** (it waits for offset 1.79 billion). There's no error.

---

## Kafka (`cd kafka`)

### Act 1: resume after a restart (the broker remembers)

```bash
./run publish.php 5
./run consume.php        # group replay.billing; Ctrl-C after a few
./run publish.php 3
./run consume.php        # resumes, no local file
```

- First run: `committed offset on the broker: none`, so `auto.offset.reset=earliest` applies: offsets 0-4.
- Second run: `committed offset on the broker: 5`, then only offsets 5, 6, 7. Same result as `resume.php`, but the state lives in the broker.
- Kafka commits the **next** offset to read (5). Our RabbitMQ file stores the **last processed** one (4). Pick one convention, or you'll skip or replay one message.

### Act 2: rewind an existing group (`kafka-consumer-groups`)

```bash
K=../../bin/kafka
# while consume.php is still running:
$K kafka-consumer-groups --group replay.billing --topic replay.events --reset-offsets --to-earliest --execute
# after Ctrl-C:
$K kafka-consumer-groups --describe --group replay.billing
$K kafka-consumer-groups --group replay.billing --topic replay.events --reset-offsets --by-duration PT10M --dry-run
# pick a time between your two publish runs (UTC with Z, or your zone with +07:00)
$K kafka-consumer-groups --group replay.billing --topic replay.events --reset-offsets --to-datetime 2026-10-10T10:00:00.000+07:00 --execute
./run consume.php
```

- While a member is connected: `Error: Assignments can only be reset if the group 'replay.billing' is inactive, but the current state is Stable.` Rewinding is a deploy step (stop every consumer), not an API call.
- `--dry-run` prints the `NEW-OFFSET` table and changes nothing. `--execute` writes it.
- `--to-datetime` accepts `Z` or `+07:00`. Without a suffix it uses the container's zone (UTC). `--by-duration PT10M` means "10 minutes ago" and needs no zone.
- `--shift-by -2` moves back 2 offsets (we saw 8 become 6). There's also `--to-offset N`, `--to-earliest` and `--to-latest`.
- After `--to-datetime 2026-10-10T05:49:10.000Z`, the restarted consumer printed `committed offset on the broker: 5` and reprocessed only offsets 5-7.

### Act 3: one-off reprocessing from PHP (no group, no CLI)

```bash
./run reprocess.php '-2 minutes'
./run reprocess.php '2026-10-10 03:00 UTC'
```

- `offsetsForTimes()` returns the first offset per partition whose timestamp is ≥ the given time: `partition 0: since 05:49:12 = offset 5, end = 8`, then offsets 5-7 and `done, no group offsets touched`.
- **Exact per message**, unlike RabbitMQ's chunk start: messages published at 05:49:03 were not included.
- `assign()` instead of `subscribe()`: no group membership and no commit. `replay.oneoff` never shows up in `kafka-consumer-groups --list`, and `replay.billing` keeps its offset. That's what you want for "reprocess into a new table" scripts that must not disturb the live consumer.
- A time in the future returns offset `-1`, so there's nothing to read.

---

## Compare

| | RabbitMQ stream (AMQP 0-9-1, from PHP) | Kafka |
|---|---|---|
| Where the offset lives | in your app (a file, a DB row) | the broker, per group |
| Resume after restart | you pass saved + 1 | automatic |
| Replay by offset | `x-stream-offset: 3` | `--reset-offsets --to-offset 3`, or `seek()` |
| Replay by time | timestamp or `30s`, starts at the chunk (a few older messages) | `--to-datetime`, `--by-duration`, `offsetsForTimes()`, exact |
| Rewind a live consumer | restart it with a new offset argument | stop the whole group, reset, restart |
| Tooling | none: it's your consumer's argument | `kafka-consumer-groups`, kafka-ui |
| Replay limit | `x-max-age` / `x-max-length-bytes` | `retention.ms` / `retention.bytes` |
| PHP caveat | broker-side offset tracking needs the stream protocol, which has no official PHP client | none, php-rdkafka covers all of it |

## Talking points

- "RabbitMQ can't replay" is wrong for streams (since 3.9, 2021) and right for queues, where ack deletes (demo 01).
- From PHP, the real gap is **bookkeeping**, not replay: you store the offset yourself.
- Kafka's offset tooling is better: dry-run, by-duration, per-partition view, a UI. The price is that you can only rewind a stopped group.
- Retention is the replay window on both sides. Past `x-max-age` / `retention.ms` the data is gone, read or not.

## Gotchas (hit while building this)

- `x-stream-offset` with an int that's really a unix time gives no error and no messages, ever. Use a `DateTimeImmutable` or an interval string.
- Timestamp attach in RabbitMQ is chunk-granular. Kafka's `offsetsForTimes` is exact.
- Kafka commits `last + 1`. Use the same convention in your own offset store.
- `kafka-consumer-groups --reset-offsets` refuses while any member is connected (`current state is Stable`).
- php-rdkafka requires a `group.id` even for an `assign()`-only consumer. Without a commit, the group is never created.
- When you script these demos, `php ... &` in a non-interactive shell ignores SIGINT. Stop it with `kill` (TERM).

## Checklist

- [ ] RabbitMQ Act 1: killed `resume.php` mid-job, restarted it, saw it resume from the file and replay the in-flight event
- [ ] I can explain why `next` misses events and `first` redoes them when nobody stores the offset
- [ ] RabbitMQ Act 2: replayed by `@time` and by `30s`, and hit the int trap
- [ ] Kafka Acts 1-2: the restart resumed from the broker; the reset was refused while active, then worked with `--dry-run` and `--execute`
- [ ] Kafka Act 3: `reprocess.php` read from a time without touching `replay.billing`
- [ ] I can say who stores the offset on each side, and what limits how far back you can replay
