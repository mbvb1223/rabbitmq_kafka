# Demo 07: replay (~6 min)

Both brokers can replay a stream. The difference is **who remembers where you were**. Kafka stores each group's committed offset on the broker. A RabbitMQ stream consumed over AMQP 0-9-1, the only mature path from PHP, doesn't: server-side offset tracking belongs to RabbitMQ's native stream protocol (a separate binary protocol on port 5552 with no official PHP client), where a named consumer stores its offset on the broker. AMQP 0-9-1 has no such call, so the PHP consumer has to store its own.

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
| Resume after restart | pass `x-stream-offset` = saved + 1 | automatic, unless the group was empty longer than `offsets.retention.minutes` (7 d default); then `auto.offset.reset` decides |
| Replay by time | `x-stream-offset` = a timestamp, or `30s` / `5m` | `--reset-offsets --to-datetime`, or `offsetsForTimes()` |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
# host (repo root)
demo/07-replay/reset.sh      # deletes stream replay.events + billing.offset + group replay.billing, recreates topic replay.events with 1 partition
```

All output is UTC (PHP and the containers). On stage, prefer relative times: `-2 minutes`, `30s`, `PT10M`.

Every consumer keeps running until you Ctrl-C it. Stop it before the next command unless a step says to leave it running.

### The scripts

| Script | What it does | Arguments |
|---|---|---|
| `rabbitmq/publish.php <count>` | Publishes `event 1 @ HH:MM:SS` … `event <count> @ HH:MM:SS` to the stream `replay.events` (HH:MM:SS = when the run started), then exits | `<count>`: how many events, default 3 |
| `rabbitmq/resume.php` | The billing consumer: reads `replay.events` until Ctrl-C, 1.5 s per event, and after each one saves its offset in `rabbitmq/billing.offset`. Starts at the saved offset + 1, or at `first` when there's no file | none |
| `rabbitmq/consume.php [from]` | The audit consumer: attaches to `replay.events` at `[from]` (sent as `x-stream-offset`) and prints until Ctrl-C. Saves nothing | `next` (default): only new messages. `first`: the oldest retained one. Digits: an offset (`3`). An interval (`30s`, `5m`, `1h`): that long ago. `@<time>`: anything PHP's `DateTimeImmutable` reads (`'@-2 minutes'`, `@05:40`) |
| `kafka/publish.php <count>` | Same as the RabbitMQ one, to topic `replay.events` (keys `order-1` …), and prints the run's time: `at HH:MM:SS UTC` | same |
| `kafka/consume.php [group]` | The billing consumer: prints the group's committed offset on the broker, then reads `replay.events` until Ctrl-C, 0.5 s per event, committing each one | consumer group, default `replay.billing` |
| `kafka/reprocess.php <since>` | Reads `replay.events` from `<since>` to the current end (`offsetsForTimes()` + `assign()`), then exits. Joins no group, commits nothing | anything PHP's `DateTimeImmutable` reads (`'05:40:12 UTC'`, `'-1 day'`), default `-5 minutes` |

No real billing happens: the consumers just print and sleep, so you have time to Ctrl-C one mid-stream.

Reading a consumer line, e.g. `[billing] event 5 @ 05:40:12  (offset 4, published 05:40:12)`:

- `[billing]`: who printed it. `[billing]` = `resume.php`, `[audit]` = RabbitMQ `consume.php`, `[replay.billing]` = Kafka `consume.php` (its group), `[oneoff]` = `reprocess.php`.
- `event 5 @ 05:40:12`: the message body: its number within its publish run, and when that run started.
- `offset 4`: the message's position in the stream or topic, counted from 0 across all runs.
- `published 05:40:12`: the message's own timestamp (RabbitMQ: set by `publish.php`; Kafka: the record timestamp), UTC, whole seconds.

---

## RabbitMQ (`rabbitmq/`)

### Act 1: resume after a restart (your offset, your file)

```bash
# php container (/demo)

# terminal 1: publish event 1..5 ("5") to the stream replay.events
php 07-replay/rabbitmq/publish.php 5

# terminal 1: start billing; no billing.offset yet, so it starts at 'first'. Ctrl-C as soon as event 5 appears (each event takes 1.5 s)
php 07-replay/rabbitmq/resume.php

# terminal 1: publish 3 more ("3"); these arrive while billing is down
php 07-replay/rabbitmq/publish.php 3

# terminal 1: restart billing: it resumes from the offset saved in billing.offset + 1. Ctrl-C once it goes quiet
php 07-replay/rabbitmq/resume.php

# terminal 1: publish 3 more while billing is down again, and pretend billing.offset is lost
php 07-replay/rabbitmq/publish.php 3

# terminal 1: the audit consumer at "next" (only new messages): just the attaching line, those 3 are never delivered. Ctrl-C
php 07-replay/rabbitmq/consume.php next

# terminal 1: the audit consumer at "first" (the oldest retained message): all 11 again, including the 8 billing already processed
php 07-replay/rabbitmq/consume.php first
```

Each publish run numbers its events from 1, so use the offset and the `@ HH:MM:SS` to spot a replay.

- First run: `no saved offset, starting from 'first'`, then offsets 0, 1, 2, ...
- Second run: `saved offset 3, resuming from 4`, then **event 5 (offset 4) again**, then the 3 new ones. The kill landed after the print but before the save: at-least-once, it replays one and never skips one.
- Without the file you only have `next` (attaches at the end, so whatever arrived while billing was down is lost) or `first` (redoes everything billing already processed).
- `billing.offset` is the whole state. Lose it (new container, new pod) and you're back to choosing between missing and redoing. In real code, save it in the same DB transaction as the work, or make the work idempotent. A stale file is just as bad: if the stream is recreated, saved + 1 is past the end, and every new message is silently skipped until the stream grows past it. Reset the file together with the stream (as `reset.sh` does), or check it against the stream before you attach.

### Act 2: replay by time

```bash
# php container (/demo)

# terminal 1: publish 3 fresh events, so the next two windows aren't empty
php 07-replay/rabbitmq/publish.php 3

# terminal 1: "30s" is an interval string: everything from the last 30 seconds. Ctrl-C after each of these
php 07-replay/rabbitmq/consume.php 30s

# terminal 1: a leading "@" turns the rest into a DateTimeImmutable, sent as a timestamp: from 2 minutes ago
php 07-replay/rabbitmq/consume.php '@-2 minutes'

# terminal 1: from 1 day ago: everything still retained
php 07-replay/rabbitmq/consume.php '@-1 day'

# terminal 1: digits only = an offset: from offset 3
php 07-replay/rabbitmq/consume.php 3

# terminal 1: the trap: a unix time as digits, also read as an offset.
# Leave it running, then in terminal 2: php 07-replay/rabbitmq/publish.php 1
php 07-replay/rabbitmq/consume.php 1791610000
```

- `30s` / `@-2 minutes`: the events published in that window, at least the 3 you just sent. `@-1 day`: all 14 (offsets 0-13). `3`: offsets 3-13.
- php-amqplib's `AMQPTable` encodes a `DateTimeInterface` as an AMQP timestamp (seconds), so `new DateTimeImmutable('-1 day')` works as the [talk proposal](../../docs/03-topic-proposal.md) says (the Q&A answer to "RabbitMQ can't replay").
- **Chunk-granular** (a chunk is a batch of messages the broker writes together; the time index points at chunks, not messages): during a 4 s burst (240K messages) we attached at `05:48:17` and the first message delivered had been published at `05:48:16.999`. The broker starts at the chunk that contains the time, so the consumer must tolerate a few older messages. You won't see this with this lesson's 3-5 message publishes: each run is one chunk, so `@<time between runs>` starts exactly at the next run. Older messages only appear when you attach mid-burst (we used 240K messages in 4 s and read millisecond timestamps outside `describe()`, which prints whole seconds).
- **The trap:** `1791610000` is a unix time passed as an int. RabbitMQ reads every int as an **offset**. The reader clamps to the end of the log (as the docs say), but the AMQP 0-9-1 consumer then drops every message below offset 1,791,610,000, so you get **nothing, not even new messages**, and no error. You see only the attaching line, and the event from terminal 2 never arrives.

---

## Kafka (`kafka/`)

### Act 1: resume after a restart (the broker remembers)

```bash
# php container (/demo)

# terminal 1: publish event 1..5 ("5") to topic replay.events
07-replay/kafka/run publish.php 5

# terminal 1: billing in group replay.billing (the default); prints the group's committed offset first. Ctrl-C once offset 4 is printed
07-replay/kafka/run consume.php

# terminal 1: publish 3 more while billing is down. Note the "at HH:MM:SS UTC" it prints: Acts 2 and 3 use it
07-replay/kafka/run publish.php 3

# terminal 1: restart billing: it resumes from the broker's committed offset, no local file. Leave it running for Act 2
07-replay/kafka/run consume.php
```

- First run: `committed offset on the broker: none`, so `auto.offset.reset=earliest` applies: offsets 0-4.
- Second run: `committed offset on the broker: 5`, then only offsets 5, 6, 7. The state lives in the broker. No duplicate here, but not because of the broker: `consume.php` traps Ctrl-C, commits the in-flight message, then `close()`s, while `resume.php` dies mid-job. Kill the Kafka consumer hard instead (from a second php container shell: `pkill -9 -f 'php consume.php'`, which kills every `consume.php` in the container) and, once its 45 s session times out, the uncommitted offset comes back too. Both sides are at-least-once: commit after the work.
- Kafka commits the **next** offset to read (5). Our RabbitMQ file stores the **last processed** one (4). Pick one convention, or you'll skip or replay one message.

### Act 2: rewind an existing group (`kafka-consumer-groups`)

```bash
# host (repo root)

# K = the Kafka CLI wrapper, to keep the lines below short
K=demo/bin/kafka

# while consume.php is still running: try to rewind group replay.billing on replay.events to the oldest offset
# (--to-earliest) and write it (--execute). It's refused
$K kafka-consumer-groups --group replay.billing --topic replay.events --reset-offsets --to-earliest --execute

# Ctrl-C consume.php, then show the group's committed offset, end of the log and LAG per partition
$K kafka-consumer-groups --describe --group replay.billing

# preview (--dry-run changes nothing) a rewind to 10 minutes ago (PT10M)
$K kafka-consumer-groups --group replay.billing --topic replay.events --reset-offsets --by-duration PT10M --dry-run

# preview moving the committed offset back 2 (--shift-by -2)
$K kafka-consumer-groups --group replay.billing --topic replay.events --reset-offsets --shift-by -2 --dry-run

# rewind for real (--execute) to the time your 2nd `07-replay/kafka/run publish.php 3` printed ("... at HH:MM:SS UTC"):
# replace <HH:MM:SS>; $(date -u +%F) fills in today's date, and the .000 is required
$K kafka-consumer-groups --group replay.billing --topic replay.events --reset-offsets --to-datetime "$(date -u +%F)T<HH:MM:SS>.000Z" --execute

# php container (/demo)

# terminal 1: restart billing: it starts from the offset you just set. Ctrl-C once offset 7 is printed
07-replay/kafka/run consume.php
```

- While a member is connected: `Error: Assignments can only be reset if the group 'replay.billing' is inactive, but the current state is Stable.` An external reset (CLI or Admin API) needs an empty group, so stop every consumer first. A consumer can still move itself from code: `seek()` in Java, `assign()` with explicit offsets in php-rdkafka (Act 3).
- `--describe`: CURRENT-OFFSET is the committed position, LAG = LOG-END-OFFSET minus CURRENT-OFFSET, and CONSUMER-ID `-` means no member, so a reset is allowed.
- `--dry-run` prints the `NEW-OFFSET` table and changes nothing. `--execute` writes it.
- `--to-datetime` accepts `Z` or `+07:00`. Without a suffix it uses the container's zone (UTC). The `.000` milliseconds are required: `...T10:00:00Z` fails with `Unparseable date`. `--by-duration PT10M` means "10 minutes ago" and needs no zone.
- `--shift-by -2` moves back 2 offsets (8 becomes 6). There's also `--to-offset N`, `--to-earliest` and `--to-latest`.
- After that reset the restarted consumer prints `committed offset on the broker: 5` and reprocesses only offsets 5-7.

### Act 3: one-off reprocessing from PHP (no group, no CLI)

```bash
# php container (/demo)

# terminal 1: read from a time to the current end, then exit. Replace <HH:MM:SS> with the time your
# 2nd 07-replay/kafka/run publish.php printed ("... at HH:MM:SS UTC")
07-replay/kafka/run reprocess.php '<HH:MM:SS> UTC'

# terminal 1: the same from 1 day ago: everything still retained
07-replay/kafka/run reprocess.php '-1 day'
```

- `offsetsForTimes()` returns the first offset per partition whose timestamp is ≥ the given time: `partition 0: since <HH:MM:SS> = offset 5, end = 8`, then offsets 5-7 and `done, no group offsets touched`. `-1 day` gives offset 0 and all 8.
- Relative times work too, but `-2 minutes` lands on offset 5 only if it falls between your two batches.
- **Exact per message** against the record timestamp (CreateTime, the producer's clock, by default), unlike RabbitMQ's chunk start, which uses the broker's write time: the first batch, published seconds earlier, is not included.
- `assign()` instead of `subscribe()`: no group membership and no commit. `replay.oneoff` never shows up in `kafka-consumer-groups --list`, and `replay.billing` keeps its offset. That's what you want for "reprocess into a new table" scripts that must not disturb the live consumer.
- Any time after the newest message, not only a future one, returns offset `-1`, so there's nothing to read.

---

## Compare

| | RabbitMQ stream (AMQP 0-9-1, from PHP) | Kafka |
|---|---|---|
| Where the offset lives | in your app (a file, a DB row) | the broker, per group |
| Resume after restart | you pass saved + 1 | automatic, unless the group was empty longer than `offsets.retention.minutes` (7 d default); then `auto.offset.reset` decides |
| Replay by offset | `x-stream-offset: 3` | `--reset-offsets --to-offset 3`, or `assign()` with an offset (php-rdkafka has no `seek()`) |
| Replay by time | timestamp or `30s`, starts at the chunk (a few older messages) | `--to-datetime`, `--by-duration`, `offsetsForTimes()`, exact |
| Rewind a live consumer | re-consume with a new `x-stream-offset` (cancel + consume, or restart) | from outside: stop the group, reset, restart; from inside: the consumer assigns/seeks itself |
| Tooling | none: it's your consumer's argument | `kafka-consumer-groups`, kafka-ui |
| Replay limit | `x-max-age` / `x-max-length-bytes` | `retention.ms` / `retention.bytes` |
| PHP caveat | broker-side offset tracking needs the stream protocol, which has no official PHP client | php-rdkafka covers replay (`assign()` with offsets, `offsetsForTimes()`, `commit()`), but has no `seek()` and no Admin API, so group resets go through the CLI. The cost is the C extension + librdkafka (demo 01) |

## Talking points

- "RabbitMQ can't replay" is wrong for streams (since 3.9, 2021) and right for queues, where ack deletes (demo 01).
- From PHP, the real gap is **bookkeeping**, not replay: you store the offset yourself.
- Kafka's broker-side commit isn't atomic with your DB either. For exactly-once into a table, both sides store the offset in the DB transaction, and RabbitMQ's bookkeeping gap disappears.
- Kafka's offset tooling is better: dry-run, by-duration, per-partition view, a UI. The price is that an external reset only works on a stopped group.
- Retention is the replay window on both sides. Past `x-max-age` / `retention.ms` the data can be deleted at any time, a whole segment at a time, read or not. Don't count on it being there, or on it being gone.

## Gotchas (hit while building this)

- `x-stream-offset` with an int that's really a unix time gives no error and no messages, ever. Use a `DateTimeImmutable` or an interval string.
- Timestamp attach in RabbitMQ is chunk-granular. Kafka's `offsetsForTimes` is exact.
- Kafka commits `last + 1`. Use the same convention in your own offset store.
- `kafka-consumer-groups --reset-offsets` refuses while any member is connected (`current state is Stable`).
- A Kafka group that stays empty for 7 days loses its committed offsets. With `earliest`, the next start redoes everything still retained.
- `RdKafka\KafkaConsumer` requires a `group.id` even for `assign()`-only use (the low-level `RdKafka\Consumer` doesn't). Without a commit, the group is never created.
- When you script these demos, `php ... &` in a non-interactive shell ignores SIGINT. Stop it with `kill` (TERM).

## Checklist

- [ ] RabbitMQ Act 1: killed `resume.php` mid-job, restarted it, saw it resume from the file and replay the in-flight event
- [ ] I can explain why `next` misses events and `first` redoes them when nobody stores the offset
- [ ] RabbitMQ Act 2: replayed by `@time` and by `30s`, and hit the int trap
- [ ] Kafka Acts 1-2: the restart resumed from the broker; the reset was refused while active, then worked with `--dry-run` and `--execute`
- [ ] Kafka Act 3: `reprocess.php` read from a time without touching `replay.billing`
- [ ] I can say who stores the offset on each side, and what limits how far back you can replay
