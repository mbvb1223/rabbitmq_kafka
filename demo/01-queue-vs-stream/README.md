# Demo 01: queue vs stream (~8 min)

A queue treats a message as a **work item**: ack deletes it. A stream treats a message as a **position** in a log: ack deletes nothing, and each reader chooses where to (re)start.

- **RabbitMQ** has both as separate types, picked with `x-queue-type`: `quorum` (RabbitMQ 4's replicated, durable queue; `classic`, the default, would behave the same here) or `stream`.
- **Kafka** has only the log: a topic is N **partitions**, each an independent ordered log. The `group.id` decides the behaviour: consumers in the same group split the log, different groups each read all of it.

## You'll learn

- Why "ack" deletes a message in a RabbitMQ queue but deletes nothing in a stream or a Kafka topic
- How a Kafka consumer group imitates a queue, and why its parallelism is capped by the partition count
- How a late consumer chooses where to start: `x-stream-offset` in RabbitMQ, `auto.offset.reset` + committed offsets in Kafka
- That Kafka has a real queue now (share groups), but not from PHP

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Client | php-amqplib 3.7.5, pure PHP, runs on the host | php-rdkafka 6.0.5 + librdkafka 2.14.1, a C extension, runs in a container via `./run` |
| Queue | quorum queue: consumers split **per message** | same `group.id`: consumers split **per partition**, so the number of useful workers is capped by the partition count |
| Stream | stream: every consumer gets everything (over AMQP: no groups, no stored offset) | a different `group.id` per reader: each group gets everything |

## Setup

One-time setup is in [../README.md](../README.md). Ctrl-C every consumer first (a group with live members can't be deleted). Then, before each run:

```bash
./reset.sh      # deletes qs.queue + qs.stream, recreates topic qs.events with 2 partitions
```

UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.

---

## RabbitMQ (`cd rabbitmq`)

### Act 1: queue (consumers compete)

```bash
php consume.php queue A        # terminal 1
php consume.php queue B        # terminal 2
php publish.php queue 6        # terminal 3
```

- A and B **split** the 6 events per message, to whoever is free (typically A 1, 3, 5 and B 2, 4, 6). Each event is processed once.
- The UI shows `qs.queue` at **0** messages: acked events are deleted, and their disk space is reclaimed in the background. Disk tracks the backlog, not a retention window.
- `php consume.php queue C` started late gets **nothing**. A queue can't replay.

### Act 2: stream (everyone reads everything)

```bash
php consume.php stream A       # terminal 1
php consume.php stream B       # terminal 2
php publish.php stream 6       # terminal 3
```

- A **and** B each get all 6 events, at offsets 0-5.
- After the next refresh (~5-10 s) the UI still shows **6** messages. The ack only gave the consumer **credit**: permission to receive the next message (prefetch is 1). Retention (`x-max-age` / `x-max-length-bytes`, unlimited by default) decides when data is deleted.
- Late consumers choose where to start:

| Command | Reads |
|---|---|
| `php consume.php stream C first` | everything (replay) |
| `php consume.php stream C 3` | from offset 3 |
| `php consume.php stream C 5m` | the last 5 minutes (in a short run, the same as `first`) |
| `php consume.php stream C` | only new events (`next` is the default) |

Now Ctrl-C A, wait until the 6 events are over a minute old, run `php publish.php stream 3`, and within 30 s:

- `php consume.php stream C 30s` gets only the new 3 (offsets 6-8), while `first` still starts at 0. A stream is stored in chunks (batches of messages), so a time offset starts at a chunk boundary and can include a few older events.
- `php consume.php stream A` gets none of the 3: it starts at `next`, not where it stopped. Over AMQP the broker stores no position for A.

---

## Kafka (`cd kafka`)

`./run consume.php <group> <name> [first]` joins the group `qs.<group>` (every name in this demo starts with `qs.`). `first` sets `auto.offset.reset=earliest`, the default `next` sets `latest`. Kafka uses it only when the group has **no committed offset**; otherwise the group resumes where it committed.

Before publishing, wait until the assignment (the broker deciding which member reads which partition) settles, about 7 s: in Act 1, two workers print a partition number and the third stays `none (idle)`. The first worker gets both partitions at once. Later joiners print `partitions: none (idle)` and wait ~5 s for one partition to move over, so publishing earlier sends everything to the first worker. A consumer that gets its partitions after the publish skips those events (it starts at `latest`, the end of the partition).

### Act 1: same group = "queue" (split per partition)

```bash
./run consume.php workers A    # terminal 1
./run consume.php workers B    # terminal 2
./run consume.php workers C    # terminal 3
./run publish.php 6            # terminal 4
```

- Two workers split the topic: one gets partition 0 (events 1, 3, 5), the other partition 1 (events 2, 4, 6). Which one varies per run.
- publish.php pins event i to partition (i-1) % 2 so the split is predictable. Real producers pass a key; Kafka hashes it, so every event for one key lands in one partition, in order.
- The third prints `partitions: none (idle)`. A partition goes to at most one member of a group, so 2 partitions means at most 2 busy workers, in any client. That is the price of per-partition ordering. PHP feels it most because it runs one consumer per process (demo 02).
- kafka-ui still shows **6** messages. Commit moved the `qs.workers` cursor and deleted nothing.
- Ctrl-C all three, run `./run publish.php 6`, then restart `./run consume.php workers D first`. D gets **only the new 6**: the group resumes from its committed offset and `first` is ignored. That's how Kafka imitates a queue backlog.

### Act 2: different groups = stream (everyone reads everything)

```bash
./run consume.php analytics A  # terminal 1
./run consume.php billing B    # terminal 2
./run publish.php 6            # terminal 3
```

- A **and** B each get all 6 events, but not as 1-6. They arrive one partition at a time (e.g. 2, 4, 6, 1, 3, 5), and the interleaving isn't defined. **Ordering is per partition only**; the RabbitMQ stream gave 1-6 in order.
- Replay with a new group: `./run consume.php audit C first` reads everything still in the topic (here, everything since the last `./reset.sh`).
- To rewind an **existing** group by time, Ctrl-C D (a group with live members can't be reset), then
  `../../bin/kafka kafka-consumer-groups --group qs.workers --topic qs.events --reset-offsets --by-duration PT10M --execute`
  and restart `./run consume.php workers D`. D re-reads the last 10 minutes even though the group had committed past them. In code, the same is `offsetsForTimes()` + `assign()`. Demo 07 goes deeper.

### Bonus: share group (KIP-932, Kafka's real queue, Java-only)

```bash
../../bin/kafka kafka-console-share-consumer --topic qs.events --group qs.share   # terminals 1-3
../../bin/kafka kafka-share-groups --describe --group qs.share --members          # terminal 4
./run publish.php 6                                                               # terminal 4, once all 3 members are listed
```

- The `--members` table shows **3 members on 2 partitions, all assigned**. Compare with C idle in Act 1.
- Don't promise an even split on stage. By default a share consumer acquires whole record batches (`share.acquire.mode=batch_optimized`), and a 6-event burst is one batch per partition, so expect something like 6 / 0 / 0. Per-record spreading needs `record_limit` + `max.poll.records=1` (demo 08).
- There is **no PHP client**: librdkafka's share consumer is Preview and php-rdkafka has no binding. That's why this step uses the Java CLI. Demo 08 goes deeper.

---

## Compare

| | RabbitMQ | Kafka |
|---|---|---|
| After ack / commit | queue: deleted. stream: nothing deleted | the group's cursor moves, nothing deleted |
| Disk | queue: shrinks back as the backlog drains. stream: grows until retention | grows until retention |
| Late reader | queue: gets nothing. stream: picks `x-stream-offset` | `auto.offset.reset`, only when the group has no committed offset |
| Who stores a reader's position | nobody over AMQP: the consumer passes `x-stream-offset` on every start | the broker, per group + partition |
| Max useful workers | queue: any number | the partition count, per group |
| Order seen | queue: none across workers. stream: publish order | per partition only |

## Talking points

- **RabbitMQ queue:** the broker tracks every message, which gives you retry, DLQ, TTL and priority, and lets you add any number of workers.
- **Kafka:** always a log. The "queue" is a group cursor, so parallelism ≤ partitions (the price of per-partition ordering), nothing is deleted on commit, and there is no per-message retry or DLQ (share groups close part of this gap, but not from PHP).
- **Streams on both sides:** replay and fan-out. Over AMQP (the only PHP option) RabbitMQ doesn't store a reader's position: the consumer passes `x-stream-offset` on every start and keeps its own offset (demo 07). Kafka stores a committed offset per group and partition, which is why D resumes in Act 1.

## Gotchas (all hit while building this)

- php-amqplib's default `queue_declare` (`durable=false, auto_delete=true`) fails: 4.3 refuses non-durable non-exclusive queues, and quorum/stream queues also refuse `auto_delete`. The declares in `rabbitmq/bootstrap.php` set `durable: true, auto_delete: false`.
- A RabbitMQ stream consumer over AMQP 0-9-1 must set `basic_qos` and use manual ack.
- `x-stream-offset`: passing `"3"` instead of `3` fails with `PRECONDITION_FAILED ... invalid_stream_offset_arg`.
- A Kafka consumer that is killed without `close()` keeps its partitions for the 45 s session timeout, and new consumers sit idle in the meantime. `kafka/consume.php` traps SIGINT/SIGTERM for that reason.
- Use the alpine PHP image for Kafka. Debian trixie ships librdkafka 2.8.0, too old for `group.protocol=consumer`.
- Message counts in both `rabbitmqctl list_queues` and the UI come from stats emitted every 5 s. Wait ~5-10 s before reading or pointing at a count.

## Checklist

- [ ] RabbitMQ Act 1: two consumers split 6 events, the UI shows 0 messages, a late consumer gets nothing
- [ ] RabbitMQ Act 2: both consumers get all 6, the UI still shows 6; replayed with `first`, an offset and `30s`
- [ ] Kafka Act 1: the third worker is idle; D resumes from the committed offset and ignores `first`
- [ ] Kafka Act 2: two groups each get everything, in partition order, not publish order
- [ ] I can explain in one sentence: work item vs position
