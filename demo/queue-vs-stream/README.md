# Demo: queue vs stream, RabbitMQ and Kafka (~8 min)

A queue treats a message as a **work item**: ack deletes it. A stream treats a message as a **position** in a log: ack only moves a cursor.

- **RabbitMQ** has both as separate types: `x-queue-type` is `quorum` or `stream`.
- **Kafka** has only the log. The `group.id` decides the behaviour: consumers in the same group split the log, different groups each read all of it.

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Client | php-amqplib 3.7.5, pure PHP, runs on the host | php-rdkafka 6.0.5 + librdkafka 2.14.1, a C extension, runs in a container via `./run` |
| Queue | quorum queue: consumers split **per message** | same `group.id`: consumers split **per partition**, so the number of useful workers is capped by the partition count |
| Stream | stream: every consumer gets everything | a different `group.id` per reader: each group gets everything |

## Setup

```bash
docker compose up -d                                   # from repo root
cd demo/queue-vs-stream
(cd rabbitmq && composer install)
docker build -t demo-kafka-php kafka                   # ./run builds it if missing; pre-build before going on stage
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --if-not-exists --topic demo.events --partitions 2 --replication-factor 1
```

Reset between runs:

```bash
docker compose exec rabbitmq rabbitmqctl delete_queue demo.queue
docker compose exec rabbitmq rabbitmqctl delete_queue demo.stream
# deleting the topic also drops every group's committed offsets
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --delete --topic demo.events
# then re-run the create command above
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

Wait until every consumer prints `partitions: ...` before publishing. KIP-848 assignment takes about 6 s, and a consumer that gets its partitions after the publish skips those events (`latest`).

### Act 1: same group = "queue" (split per partition)

```bash
./run consume.php workers A    # terminal 1
./run consume.php workers B    # terminal 2
./run consume.php workers C    # terminal 3
./run publish.php 6            # terminal 4
```

- A gets partition 0 (events 1, 3, 5) and B gets partition 1 (events 2, 4, 6).
- C prints `partitions: none (idle)`. The topic has 2 partitions, so a third worker can't help. **This is the PHP parallelism ceiling.**
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
  `kafka-consumer-groups.sh --group workers --topic demo.events --reset-offsets --to-datetime 2026-10-05T00:00:00.000 --execute`

### Bonus: share group (KIP-932, Kafka's real queue, Java-only)

```bash
# 3 terminals, then publish from a 4th
docker compose exec kafka /opt/kafka/bin/kafka-console-share-consumer.sh \
  --bootstrap-server kafka:19092 --topic demo.events --group share-workers
docker compose exec kafka /opt/kafka/bin/kafka-share-groups.sh \
  --bootstrap-server kafka:19092 --describe --group share-workers --members
```

- The `--members` table shows **3 members on 2 partitions, all assigned**. Compare with C idle in Act 1.
- Don't promise an even split on stage: share consumers fetch in batches, so with a fast console consumer one member can take a whole batch (we saw 15 / 15 / 0). The split only shows up with slow jobs that hold their locks.
- There is **no PHP client**: librdkafka's share consumer is Preview and php-rdkafka has no binding. That's why this step uses the Java CLI.

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
