# Demo 08: share groups, Kafka's real queue (~8 min)

Since Kafka 4.2 (KIP-932, GA Feb 2026) a **share group** lets many consumers read one partition, acknowledge **each record**, and count deliveries. That closes the partition ceiling from demo 02 and gives Kafka a poison-message limit. It does **not** make ack delete anything, it has no DLQ in 4.3, its lock is time-based (30 s), and **PHP can't use it**.

## You'll learn

- How a share group differs from a consumer group: 3 members on 1 partition, all busy
- The record lifecycle: acquired → acknowledged, released (try again) or rejected (archived)
- The acquisition lock: a stuck worker's record comes back after 30 s, while the rest keep flowing
- Where the poison job goes after the delivery limit: archived, nowhere you can read it (RabbitMQ: a DLQ)
- Why this is Java-only today: librdkafka 2.14.1 in our image has no share API, php-rdkafka has no binding

| | RabbitMQ quorum queue (`rabbitmq/`) | Kafka share group (`kafka/`) |
|---|---|---|
| Client | php-amqplib, 3 PHP consumers | Java console tool `kafka-console-share-consumer`; PHP only publishes |
| One job at a time per worker | `basic_qos(0, 1)` | `share.acquire.mode=record_limit` + `max.poll.records=1` |
| Failed job | `reject(requeue: true)` | `--release` (try again) or `--reject` (give up) |
| Poison limit | `x-delivery-limit` 5 → dead-lettered to `share.dead` | `share.delivery.count.limit` 5 → archived |
| Stuck worker | message held until the connection dies or `consumer_timeout` (30 min) | record held until the lock expires (30 s, max 60 s per group) |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh   # deletes share.jobs + share.dead, recreates topic share.jobs (1 partition),
             # deletes groups share.workers + share.classic, sets share.auto.offset.reset=earliest
```

---

## RabbitMQ (`cd rabbitmq`)

### Act 1: three workers, one poison job

```bash
php consume.php A          # terminals 1-3
php consume.php B
php consume.php C
php dead.php               # terminal 4
php publish.php 9 3        # terminal 5: 9 jobs, job 3 always fails
```

- The good jobs split across workers (we saw A: 2, 5, 7, 9 and B: 1, 4, 6, 8).
- Job 3 is rejected and redelivered: `(delivery 1)` … `(delivery 6)`, then `[dead] job 3 (poison) reason=delivery_limit from=share.jobs`. A limit of 5 allows 5 **returns**, so 6 deliveries.

### Act 2: a stuck worker, then a crashed one

```bash
php consume.php A                    # terminal 1
php consume.php B & B=$!; sleep 2; kill -STOP $B   # terminal 2: B joins, then freezes
php publish.php 4                    # terminal 3
kill -9 $B                           # terminal 2, after a few seconds
```

- While B is frozen, A processes the other three jobs and the UI shows **1 unacked**: B holds one job (job 1 or 2, whichever round-robin handed it). RabbitMQ would wait 30 min (`consumer_timeout`) for a worker that is alive but slow.
- `kill -9` closes B's connection and A prints that job with `(delivery 2)` **immediately**. Redelivery follows the connection, not a clock.

## Kafka (`cd kafka`)

### Act 1: consumer group vs share group on 1 partition

```bash
./run consume.php A        # terminals 1-3: a classic consumer group
./run consume.php B
./run consume.php C
./run publish.php 9        # terminal 4
```

- One worker gets all 9; the other two print `partitions: none (idle)`. Ctrl-C all three.

```bash
./share-consumer.sh        # terminals 1-3: one share group
../../bin/kafka kafka-share-groups --describe --group share.workers --members
./run publish.php 9        # once all 3 members are listed
```

- `--members` shows **3 members, each assigned `share.jobs:0`**. Same partition, all three.
- The first members drain the 9 jobs from the consumer-group run right away (earliest), then the new 9 spread over **all three** (we saw 3/3/3, 2/5/2, 2/4/3, 4/2/3: the console tool has no work to slow it down, so the split is never exact). Each line shows `Offset:12 Delivery:1 job 4`.
- Without `record_limit` (the default `batch_optimized`), one member acquired a whole batch: we saw 0 / 9 / 9.

### Act 2: release vs reject (Ctrl-C the three members first)

```bash
./share-consumer.sh --release   # terminal 1: "failed, try again"
./run publish.php 1             # terminal 2
../../bin/kafka kafka-share-groups --describe --group share.workers --offsets
```

- `Offset:0 Delivery:1 job 1` … `Delivery:5`, then nothing: after `share.delivery.count.limit` (5) the record is **archived**. `--offsets` shows `LAG 0`.
- Repeat with `./share-consumer.sh --reject`: one `Delivery:1` line, archived at once.
- There is no DLQ to look in. KIP-1191 adds one in Kafka 4.4 (unreleased; headers only by default).

### Act 3: a stuck worker (the lock)

```bash
./stuck-worker.sh               # terminal 1: a member in its own container, then docker pause
./share-consumer.sh             # terminal 2
./run publish.php 3             # terminal 3
./stuck-worker.sh stop          # afterwards
```

- Jobs 2 and 3 print at once. Job 1 prints **~31 s later** as `Delivery:2`: the frozen member held it until `share.record.lock.duration.ms` (30 s) expired. Head-of-line blocking is reduced, not gone.
- `--members` still lists the frozen member until the 45 s session timeout drops it (after ~48 s frozen it was gone). When unpaused it printed nothing in our runs.
- This is the PHP problem: a job longer than the lock (60 s max per group by default) is redelivered **while the first worker is still running it**, and only RENEW (not in librdkafka) extends the lock.

### Act 4: ack ≠ delete

```bash
../../bin/kafka kafka-get-offsets --topic share.jobs --time -2   # share.jobs:0:0
../../bin/kafka kafka-get-offsets --topic share.jobs --time -1   # share.jobs:0:23 after Acts 1-3
../../bin/kafka kafka-console-consumer --topic share.jobs --from-beginning --max-messages 1
```

- Every record is accepted, released or archived, yet the topic still holds them all from offset 0, including the jobs archived in Act 2. Retention deletes, acks never do. A RabbitMQ queue would be at 0.

---

## Compare

| | Kafka consumer group | Kafka share group (4.2+) | RabbitMQ quorum queue |
|---|---|---|---|
| Ack unit | offset (cumulative) | per record: accept / release / reject | per message: ack / reject / nack |
| Consumers > partitions | idle | all busy | n/a, any number |
| Ordering | per partition | none | none with competing consumers |
| Delivery limit | none (app) | 5 (2-10), then archived | `x-delivery-limit` (default 20), then dead-lettered |
| DLQ | build it yourself | 4.4 (unreleased) | DLX: routable, `x-death` says why |
| Stuck worker | partition blocked | record back after 30 s lock (max 60 s) | held until connection dies / 30 min |
| Ack deletes the record | no | no | yes |
| Retry with backoff | build it yourself | no (release = immediate) | `x-delayed-retry-*` (4.3, demo 03) |
| PHP client | php-rdkafka | **none** | php-amqplib |

## Talking points

- Kafka fixed the partition ceiling, **for Java**. librdkafka's share consumer is Preview since 2.15.0 (our image has 2.14.1: `php --re rdkafka` shows no share API), and php-rdkafka has no binding (#616).
- Share groups are a queue *on top of a log*: per-record state lives beside the log, the data stays until retention.
- The 30 s lock is the new `max.poll.interval.ms`: fine for 50 ms jobs, dangerous for a PDF render.

## Gotchas (hit while building this)

- `share.auto.offset.reset` defaults to **latest**: members started after the publish see nothing. `reset.sh` sets `earliest` per group with `kafka-configs --entity-type groups`; the config survives deleting the group.
- Default `share.acquire.mode=batch_optimized` lets one fast member take a whole batch. `record_limit` + `max.poll.records=1` is the prefetch-1 equivalent.
- RabbitMQ `x-delivery-limit=5` means 6 deliveries; Kafka's `share.delivery.count.limit=5` means 5.
- Killing a **non-TTY** `bin/kafka` client (background job, script) leaves the Java consumer running inside the kafka container. It stays a member and steals records. Ctrl-C in a terminal is fine; `reset.sh` pkills leftovers.
- The stuck worker needs its own container (`docker pause` on the kafka container would freeze the broker) and `fetch.max.wait.ms=60000`, so its long-poll is still waiting at the broker when the job is published.
- `kafka-console-share-consumer --timeout-ms` exits with an `ERROR ... TimeoutException` stack: harmless.

## Checklist

- [ ] RabbitMQ: the poison job hit `share.dead` with `reason=delivery_limit` after 6 deliveries
- [ ] RabbitMQ: a frozen worker held a job; `kill -9` got it redelivered at once
- [ ] Kafka: a consumer group left 2 of 3 workers idle; a share group kept all 3 busy on 1 partition
- [ ] Kafka: `--release` showed `Delivery:1` to `5`, then the record was archived with no DLQ
- [ ] Kafka: the stuck worker's record came back ~30 s later as `Delivery:2`, and the topic still held every record
- [ ] I can explain why a PHP shop can't adopt share groups today
