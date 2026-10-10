# Demo 01: queue vs stream (~8 min)

A queue treats a message as a **work item**: ack deletes it. A stream treats a message as a **position** in a log: ack only moves a cursor.

- **RabbitMQ** has both as separate types: `x-queue-type` is `quorum` or `stream`.
- **Kafka** has only the log. The `group.id` decides the behaviour: consumers in the same group split the log, different groups each read all of it.

## You'll learn

- Why "ack" deletes a message in a RabbitMQ queue but only moves a cursor in a stream or a Kafka topic
- How a Kafka consumer group imitates a queue, and why its parallelism is capped by the partition count
- How a late consumer chooses where to start: `x-stream-offset` in RabbitMQ, `auto.offset.reset` + committed offsets in Kafka
- That Kafka has a real queue now (share groups), but not from PHP

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Client | php-amqplib 3.7.5, pure PHP, runs on the host | php-rdkafka 6.0.5 + librdkafka 2.14.1, a C extension, runs in a container via `./run` |
| Queue | quorum queue: consumers split **per message** | same `group.id`: consumers split **per partition**, so the number of useful workers is capped by the partition count |
| Stream | stream: every consumer gets everything | a different `group.id` per reader: each group gets everything |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh      # deletes demo.queue + demo.stream, recreates topic demo.events with 2 partitions
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

- A and B **split** the 6 events (e.g. A gets 1, 4, 5 and B gets 2, 3, 6). Each event is processed once.
- The UI shows `demo.queue` at **0** messages: acked events are deleted and the disk space is freed.
- `php consume.php queue C` started late gets **nothing**. A queue can't replay.

### Act 2: stream (everyone reads everything)

```bash
php consume.php stream A       # terminal 1
php consume.php stream B       # terminal 2
php publish.php stream 6       # terminal 3
```

- A **and** B each get all 6 events, at offsets 0-5.
- The UI still shows **6** messages. The ack only gave the consumer credit for the next message. Retention decides when data is deleted.
- Late consumers choose where to start:

| Command | Reads |
|---|---|
| `php consume.php stream C first` | everything (replay) |
| `php consume.php stream C 3` | from offset 3 |
| `php consume.php stream C 5m` | the last 5 minutes (chunk-granular, so it may start a little earlier) |
| `php consume.php stream C` | only new events (`next` is the default) |

---

## Kafka (`cd kafka`)

Before publishing, wait until the assignment settles (about 7 s): in Act 1, two workers print a partition number and the third stays `none (idle)`. Every consumer prints `partitions: none (idle)` first, and the first to join briefly owns both partitions, so publishing earlier sends everything to one worker. A consumer that gets its partitions after the publish skips those events (`latest`).

### Act 1: same group = "queue" (split per partition)

```bash
./run consume.php workers A    # terminal 1
./run consume.php workers B    # terminal 2
./run consume.php workers C    # terminal 3
./run publish.php 6            # terminal 4
```

- Two workers split the topic: one gets partition 0 (events 1, 3, 5), the other partition 1 (events 2, 4, 6). Which one varies per run.
- The third prints `partitions: none (idle)`. The topic has 2 partitions, so a third worker can't help. **This is the PHP parallelism ceiling.**
- kafka-ui still shows **6** messages. Commit moved the `workers` cursor and deleted nothing.
- Ctrl-C all three, run `./run publish.php 6`, then restart `./run consume.php workers D first`. D gets **only the new 6**: the group resumes from its committed offset and `first` is ignored. That's how Kafka imitates a queue backlog.

### Act 2: different groups = stream (everyone reads everything)

```bash
./run consume.php analytics A  # terminal 1
./run consume.php billing B    # terminal 2
./run publish.php 6            # terminal 3
```

- A **and** B each get all 6 events, but the order differs: A may read partition 0 first and B partition 1 first. **Ordering is per partition only.**
- Replay with a new group: `./run consume.php audit C first` reads everything ever published.
- To replay an **existing** group from an offset or time, use the CLI (the group must be stopped):
  `../../bin/kafka kafka-consumer-groups --group workers --topic demo.events --reset-offsets --to-datetime 2026-10-05T00:00:00.000 --execute` (demo 07 goes deeper)

### Bonus: share group (KIP-932, Kafka's real queue, Java-only)

```bash
# 3 terminals, then publish from a 4th
../../bin/kafka kafka-console-share-consumer --topic demo.events --group share-workers
../../bin/kafka kafka-share-groups --describe --group share-workers --members
```

- The `--members` table shows **3 members on 2 partitions, all assigned**. Compare with C idle in Act 1.
- Don't promise an even split on stage: share consumers fetch in batches, so with a fast console consumer one member can take a whole batch (we saw 15 / 15 / 0). The split only shows up with slow jobs that hold their locks.
- There is **no PHP client**: librdkafka's share consumer is Preview and php-rdkafka has no binding. That's why this step uses the Java CLI. Demo 08 goes deeper.

---

## Talking points

- **RabbitMQ queue:** the broker tracks every message, which gives you retry, DLQ, TTL and priority, and lets you add any number of workers.
- **Kafka:** always a log. The "queue" is a group cursor, so parallelism ≤ partitions, nothing is deleted on commit, and there is no per-message retry or DLQ (share groups close part of this gap, but not from PHP).
- **Streams on both sides:** same model, with replay and fan-out. RabbitMQ offsets are per stream, Kafka offsets are per partition.

## Gotchas (all hit while building this)

- RabbitMQ 4.3 refuses php-amqplib's default `queue_declare` (`durable=false`). The declares in `rabbitmq/bootstrap.php` are durable.
- A RabbitMQ stream consumer over AMQP 0-9-1 must set `basic_qos` and use manual ack.
- `x-stream-offset`: passing `"3"` instead of `3` fails with `PRECONDITION_FAILED ... invalid_stream_offset_arg`.
- A Kafka consumer that is killed without `close()` keeps its partitions for the 45 s session timeout, and new consumers sit idle in the meantime. `kafka/consume.php` traps SIGINT/SIGTERM for that reason.
- Use the alpine PHP image for Kafka. Debian trixie ships librdkafka 2.8.0, too old for `group.protocol=consumer`.
- `rabbitmqctl list_queues` message counts lag by about 5 s, so use the UI on stage.

## Checklist

- [ ] RabbitMQ Act 1: two consumers split 6 events, the UI shows 0 messages, a late consumer gets nothing
- [ ] RabbitMQ Act 2: both consumers get all 6, the UI still shows 6; replayed with `first`, an offset and `5m`
- [ ] Kafka Act 1: the third worker is idle; D resumes from the committed offset and ignores `first`
- [ ] Kafka Act 2: two groups each get everything, in a different order
- [ ] I can explain in one sentence: work item vs position
