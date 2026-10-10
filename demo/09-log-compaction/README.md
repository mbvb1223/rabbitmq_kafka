# Demo 09: log compaction, where Kafka genuinely wins (~6 min)

A compacted Kafka topic keeps **the latest value per key**, forever. Older versions are removed in the background, so a service can rebuild its state on boot by reading the topic from offset 0. RabbitMQ has no equivalent: a stream keeps **every** version until retention drops whole segments.

## You'll learn

- `cleanup.policy=compact`: what is removed (older records with the same key), what is kept (the latest per key, offsets, order)
- Why nothing happens until the segment rolls, and how long the log cleaner takes
- Tombstones: a `null` value deletes a key, and the tombstone itself disappears later
- The rebuild-on-boot pattern: `assign()` from the beginning, read to the end, never commit
- That Kafka keeps committed offsets in a compacted topic: `__consumer_offsets` is `cleanup.policy=compact`

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Storage | stream `compact.users` | topic `compact.users`, 1 partition, `cleanup.policy=compact` |
| Same key updated 3 times | 3 messages, forever (until `x-max-age` / `x-max-length-bytes` drops a whole segment) | 1 record after the cleaner runs |
| Delete a key | no concept; an empty body is just another message | tombstone (`null` value), removed after `delete.retention.ms` |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh      # deletes the compact.users stream, recreates topic compact.users (compacted, 1 partition)
```

`reset.sh` tunes compaction to show up within a minute: `segment.ms=10000`, `min.cleanable.dirty.ratio=0.01`, `min.compaction.lag.ms=0`, `delete.retention.ms=10000`. The defaults are 7-day segments, a 50% dirty ratio and 1 day of tombstone retention.

---

## RabbitMQ (`cd rabbitmq`)

```bash
php publish.php      # user-1 v1..v3, user-2 v1..v2, user-3 v1 then "deleted"
php read.php         # replay from 'first', exits after 2 s idle
```

- All 7 records come back, at offsets 0-6: `user-1 name=An v1`, `An v2`, `An v3`, ..., `user-3 (empty)`.
- Run it again tomorrow: still 7. There is no per-key cleanup. To get "latest per key", the consumer replays everything and keeps a map, or you keep the state in a DB / Redis.
- Plugins don't close the gap: `rabbitmq_recent_history_exchange` (bundled, disabled) replays the last N messages of a whole exchange, not the latest per key. The community last-value-cache exchange isn't bundled with 4.3.6 (unverified on 4.3).

## Kafka (`cd kafka`)

### Act 1: before compaction, every version is there

```bash
./run publish.php    # the same 7 updates, keyed; user-3's last one is a tombstone (null value)
./run read.php       # assign() from offset 0, stops at the end of the partition
```

```
offset  0  user-1  name=An v1
offset  1  user-2  name=Binh v1
...
offset  6  user-3  (tombstone)
7 records, next offset 7
```

### Act 2: roll the segment, wait for the cleaner

The cleaner never touches the **active** segment. A segment rolls when a new record arrives after `segment.ms` (10 s here).

```bash
sleep 10 && ./run publish.php tick              # one more record rolls the segment
while sleep 3; do ./run read.php | tail -1; done   # Ctrl-C once it says "compacted away"
./run read.php
```

- Compaction landed **12-13 s after the tick** in our runs (23 s after the first publish). The cleaner wakes every 15 s (`log.cleaner.backoff.ms`), so expect 0-15 s.
- Only the latest per key is left, and **offsets have gaps**: records are removed, offsets never change.

```
offset  4  user-1  name=An v3
offset  5  user-2  name=Binh v2
offset  6  user-3  (tombstone)
offset  7  tick    tick @ 05:46:10
4 records, next offset 8, 4 offsets compacted away
```

- Proof from the broker: `docker compose exec kafka grep 'cleaned log compact.users' /opt/kafka/logs/log-cleaner.log | tail -1` prints `Log cleaner thread 0 cleaned log compact.users-0 (dirty section = [0, 7])`.

### Act 3: the tombstone goes away, on the next pass

The tombstone survives the first pass on purpose: consumers that are behind must still see "user-3 was deleted". It is dropped on a **later** pass, at least `delete.retention.ms` (10 s here) after the first one. A later pass needs new dirty data, so tick again:

```bash
./run publish.php tick
while sleep 3; do ./run read.php | tail -1; done   # wait for "5 offsets compacted away"
```

- We waited 60 s without a tick and the tombstone stayed. After the tick it was gone within 4-19 s. Offset 6 is now a gap too.

### Act 4: rebuild state on boot

```bash
./run rebuild.php
```

```
rebuilt from 4 records in 502 ms:
  user-1 => name=An v3
  user-2 => name=Binh v2
```

- `assign()` at `RD_KAFKA_OFFSET_BEGINNING`, read until `PARTITION_EOF`, apply `null` as delete. No group cursor, no commit: a rebuild always starts from 0.
- The topic size tracks the number of **keys**, not the number of updates. A billion updates to a million users rebuild from about a million records.

---

## Compare

| | RabbitMQ stream | Kafka compacted topic |
|---|---|---|
| Keeps latest per key | no, every version | yes, after the cleaner runs |
| Deletes data when | `x-max-age` / `x-max-length-bytes` drops a whole segment | an older record with the same key exists (plus normal retention if `compact,delete`) |
| Rebuild state from the broker | replay everything, keep a map (cost grows with updates) | replay the topic (cost grows with keys) |
| Delete a key | no concept | tombstone, removed after `delete.retention.ms` |
| From PHP | php-amqplib, `x-stream-offset: first` | php-rdkafka, `null` payload = tombstone |

## Talking points

- This is the one feature in the series that RabbitMQ doesn't have at all. Say it plainly; it makes the rest of the talk credible.
- Real users: CDC changelogs (Debezium), Kafka Streams state stores, and Kafka itself (`__consumer_offsets` is compacted: the latest committed offset per group+partition).
- Compaction is **eventual**: a reader can still see old versions and duplicates. Consumers must be last-write-wins.
- The other genuine Kafka win, talking point only: tiered storage (early access in 3.6, production-ready in 3.9) keeps months or years of history on object storage.

## Gotchas (hit while building this)

- Nothing is compacted until the segment rolls. With the default `segment.ms` (7 days) a quiet topic shows every version for a week.
- Tombstones need two cleaner passes and new dirty data in between. The first pass logs `discarding tombstones prior to ... 1970`, i.e. it keeps them.
- The newest record is always in the active segment, so the last two `tick` records both stay. That's expected, not a bug.
- `log-cleaner.log` survives `reset.sh` (it's the broker's log), so always `tail` it.
- php-rdkafka's `KafkaConsumer` requires a `group.id` even when you only `assign()`. The readers set one and never commit.
- `kafka-topics --create` warns that `.` and `_` in topic names can collide in metric names. Harmless here; every demo uses `.`.

## Checklist

- [ ] Act 1: 7 records before compaction, including the tombstone
- [ ] Act 2: after the tick, 4 records, offsets with gaps, and the `cleaned log` line in `log-cleaner.log`
- [ ] Act 3: the tombstone is gone only after a second tick
- [ ] Act 4: `rebuild.php` prints user-1 v3 and user-2 v2, no user-3
- [ ] RabbitMQ: the stream still returns all 7 versions
- [ ] I can explain why the active segment is never compacted and why tombstones stay for `delete.retention.ms`
