# Demo 08: share groups, Kafka's real queue (~8 min)

Since Kafka 4.2 (KIP-932, GA Feb 2026) a **share group** lets many consumers read one partition, acknowledge **each record**, and count deliveries. That closes the partition ceiling from demo 02 and gives Kafka a poison-message limit. It does **not** make ack delete anything, it has no DLQ in 4.3, its lock is time-based (30 s), and **PHP can't use it**.

## You'll learn

- How a share group differs from a consumer group: 3 members on 1 partition, all busy
- The record lifecycle: acquired → accepted, released (try again) or rejected (archived)
- The acquisition lock: a stuck worker's record comes back after 30 s, while the rest keep flowing
- Where the poison job goes after the delivery limit: archived, still in the log, but nothing tells you which offsets or why (RabbitMQ: a DLQ with `x-death`)
- Why this is Java-only today: librdkafka 2.14.1 in our image has no share API, php-rdkafka has no binding

| | RabbitMQ quorum queue (`rabbitmq/`) | Kafka share group (`kafka/`) |
|---|---|---|
| Client | php-amqplib, 3 PHP consumers | Java console tool `kafka-console-share-consumer`; PHP only publishes |
| One job at a time per worker | `basic_qos(0, 1)` | `share.acquire.mode=record_limit` + `max.poll.records=1` |
| Failed job | `reject(requeue: true)` counted retry, `nack(requeue: true)` uncounted retry, `reject(requeue: false)` → DLX at once | `--release` counted retry, `--reject` → archived at once |
| Poison limit | `x-delivery-limit` 5 → dead-lettered to `share.dead` | `share.delivery.count.limit` 5 → archived |
| Stuck worker | message held until the connection dies (crash or missed heartbeats) or `consumer_timeout` (30 min default, settable per queue in 4.3) | record held until the lock expires (30 s; groups capped at 60 s by default, broker can raise to 1 h) |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh   # deletes share.jobs + share.dead, recreates topic share.jobs (1 partition),
             # deletes groups share.workers + share.classic, sets share.auto.offset.reset=earliest
```

UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.

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

- The good jobs split across two workers while the third keeps getting job 3 (we saw A: 2, 5, 7, 9; B: 1, 4, 6, 8 plus job 3's delivery 5; C: job 3 deliveries 1-4 and 6).
- Job 3 is rejected and redelivered: `(delivery 1)` … `(delivery 6)`, then terminal 4 prints `[dead] job 3 … (poison)  reason=delivery_limit from=share.jobs deliveries=6`. A limit of 5 allows 5 **returns**, so 6 deliveries.

### Act 2: a stuck worker, then a crashed one (Ctrl-C the Act 1 workers first)

```bash
php consume.php A                    # terminal 1
php consume.php B & B=$!; sleep 2; kill -STOP $B   # terminal 2: B joins, then freezes
php publish.php 4                    # terminal 3
kill -9 $B                           # terminal 2, after a few seconds
```

- While B is frozen, A processes the other three jobs and the RabbitMQ UI (Queues → `share.jobs`) shows **Unacked 1**: B holds one job (job 1 or 2, whichever round-robin handed it). B never disconnected: php-amqplib turns heartbeats off by default (`heartbeat: 0`), so only `consumer_timeout` would reclaim the job. That is 30 min by default; in 4.3 a quorum queue can lower it with `x-consumer-timeout` or a `consumer-timeout` policy (min 1 min), and the job is returned.
- `kill -9` closes B's connection and A prints that job with `(delivery 2)` **immediately**. Here redelivery followed the connection. With heartbeats on, a PHP worker blocked in a long job loses its connection after ~2 missed heartbeats, and the job is redelivered while it still runs: the same hazard as Kafka's lock, with longer defaults.

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
- The first member prints jobs 1-9 from the consumer-group run as soon as it starts: the share group keeps its own state from offset 0, and the classic group's commits deleted nothing (Act 4). Then the new 9 spread over **all three** (we saw 3/3/3, 2/5/2, 2/4/3, 4/2/3: the console tool has no work to slow it down, so the split is never exact). Each line shows `Offset:12 Delivery:1 job 4`.

### Act 2: release vs reject (Ctrl-C the three members first)

```bash
./share-consumer.sh --release   # terminal 1: "failed, try again"
./run publish.php 1             # terminal 2
../../bin/kafka kafka-share-groups --describe --group share.workers --offsets
```

- `Offset:18 Delivery:1 job 1` … `Delivery:5`, then nothing: after `share.delivery.count.limit` (5) the record is **archived**. `--offsets` shows `LAG 0`: the archived poison job counts as done, so lag-based monitoring never sees it.
- Repeat with `./share-consumer.sh --reject`: one `Offset:19 Delivery:1` line, archived at once.
- There is no DLQ to look in. KIP-1191 adds one in Kafka 4.4 (unreleased; by default the DLQ record carries headers, not the payload).

### Act 3: a stuck worker, the lock (Ctrl-C the --reject member first)

```bash
./stuck-worker.sh               # terminal 1: a member in its own container, then docker pause
./share-consumer.sh             # terminal 2
./run publish.php 3             # terminal 3
../../bin/kafka kafka-share-groups --describe --group share.workers --members   # terminal 3, repeat while you wait
./stuck-worker.sh stop          # afterwards
```

- Two jobs print at once. One job (job 1 in our runs) prints **~31 s later** as `Delivery:2`: the frozen member held it until `share.record.lock.duration.ms` (30 s) expired. Head-of-line blocking is reduced, not gone.
- Not gone: a share partition keeps at most `share.partition.max.record.locks` (2000) records in flight, counted from the oldest unfinished one. A record stuck at the head pins that window: once the 2000 records behind it are done, the partition stops being fetched until the stuck lock expires (up to 5 × 30 s before it is archived). The window is wider than a consumer group's 1 record, but the blocking is the same.
- `--members` still lists the frozen member until the 45 s session timeout drops it (within ~45 s of the freeze it was gone).
- Why it matters for long PHP-style jobs: a job longer than the lock is handed to a second worker while the first is still running it. The lock is the group config `share.record.lock.duration.ms`, capped by the broker (60 s by default). The only way to extend it is a RENEW acknowledgement ("still working", Kafka 4.2+), which librdkafka doesn't offer.

### Act 4: ack ≠ delete

```bash
../../bin/kafka kafka-get-offsets --topic share.jobs --time -2   # -2 = earliest offset still stored: share.jobs:0:0, nothing was deleted
../../bin/kafka kafka-get-offsets --topic share.jobs --time -1   # -1 = next offset to write: share.jobs:0:23 after Acts 1-3
../../bin/kafka kafka-console-consumer --topic share.jobs --from-beginning --max-messages 1   # prints "job 1 @ …" from Act 1's consumer-group run
./run --re rdkafka | grep -ci share   # 0: php-rdkafka exposes no share consumer
```

- Every record is accepted or archived, yet the topic still holds them all from offset 0, including the jobs archived in Act 2. Retention deletes, acks never do. A RabbitMQ queue would be at 0.

---

## Compare

| | Kafka consumer group | Kafka share group (4.2+) | RabbitMQ quorum queue |
|---|---|---|---|
| Ack unit | offset (cumulative) | per record: accept / release / reject / renew | per message: ack / reject / nack |
| Consumers > partitions | idle | all busy (up to `group.share.max.size`, 200 by default) | n/a, any number |
| Ordering | per partition | none | none with competing consumers |
| Delivery limit | none (app) | 5 (2-10 by default, broker can allow up to 25), then archived | `x-delivery-limit` (default 20), then dropped, or dead-lettered if a DLX is set |
| DLQ | build it yourself | 4.4 (unreleased) | DLX: routable, `x-death` says why (at-most-once by default; at-least-once needs `dead-letter-strategy` + `x-overflow=reject-publish`) |
| Stuck worker | partition blocked until the member is dropped: 45 s session timeout if frozen, `max.poll.interval.ms` (5 min) if alive but slow | record back after 30 s lock (cap 60 s by default, up to 1 h) | held until the connection dies (crash or missed heartbeats) or `consumer_timeout` (30 min default, settable per queue in 4.3) |
| Ack deletes the record | no | no | yes |
| Retry with backoff | build it yourself | no (release = immediate) | `x-delayed-retry-*` (4.3, demo 03) |
| PHP client | php-rdkafka | **none** | php-amqplib |

## Talking points

- Kafka fixed the partition ceiling, **for Java**. librdkafka's share consumer is Preview since 2.15.0 (our image has 2.14.1: `./run --re rdkafka | grep -ci share` prints 0), and php-rdkafka has no binding (#616).
- Share groups are a queue *on top of a log*: per-record state lives beside the log, the data stays until retention.
- The 30 s lock is the new `max.poll.interval.ms` (the consumer-group limit on time between polls, 5 min; demo 03): fine for 50 ms jobs, dangerous for a PDF render unless the operator raises the broker cap.

## Gotchas (hit while building this)

- `share.auto.offset.reset` defaults to **latest**: members started after the publish see nothing. `reset.sh` sets `earliest` per group with `kafka-configs --entity-type groups`; the config survives deleting the group.
- Default `share.acquire.mode=batch_optimized` lets one fast member take a whole batch. `record_limit` + `max.poll.records=1` is the prefetch-1 equivalent. Without it, each of our two 9-job publishes went to a single member (0 / 9 / 9).
- RabbitMQ `x-delivery-limit=5` means 6 deliveries; Kafka's `share.delivery.count.limit=5` means 5. RabbitMQ 4.3 counts only reject and crashes, not nack or consumer timeout. Kafka counts every acquisition, including releases and lock expiries, so a job that always outlives the lock is archived after 5 tries.
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
