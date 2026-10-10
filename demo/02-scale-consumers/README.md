# Demo 02: scale the consumers (~6 min)

PHP runs one consumer per process, so "go faster" means "start more processes". A RabbitMQ queue hands out **messages**, so every extra worker helps. A Kafka consumer group hands out **partitions**, so on a 4-partition topic worker #5 has nothing to do, however many you start.

## You'll learn

- Why adding workers scales linearly on a RabbitMQ queue and stops at the partition count on Kafka
- What prefetch (`basic_qos`) does, and how an unlimited prefetch lets one worker hoard the whole backlog
- How long a KIP-848 group takes to settle, and how to see who owns which partition
- Why "just add partitions" is not a free fix

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| A worker gets | the next message(s), up to `prefetch` unacked | whole partitions |
| Useful workers | as many as there are jobs in flight | at most the partition count |
| Watch it | UI → Queues → `scale.jobs`: Consumers, Unacked | `../../bin/kafka kafka-consumer-groups --describe --group scale.workers --members` |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh      # deletes queue scale.jobs, recreates topic scale.jobs with 4 partitions
```

Both sides use `bench.php <workers>`: it forks N worker processes ([fleet.php](fleet.php)), waits until they're ready, publishes 200 jobs that each `usleep(50_000)` (I/O-bound, like an HTTP call), waits until all 200 are done, then prints jobs per worker, idle workers and wall time.

---

## RabbitMQ (`cd rabbitmq`)

### Act 1: 1 → 4 → 20 workers

```bash
php bench.php 1
php bench.php 4
php bench.php 20
```

| Workers | Jobs per worker | Wall time | Jobs/s |
|---|---|---|---|
| 1 | 200 | 10.29 s | 19 |
| 4 | 50 each | 2.57 s | 77 |
| 20 | 10 each | 0.52 s | 383 |

- Time drops linearly. Workers are ready in milliseconds: no rebalance, no settle time.
- I/O-bound jobs scale far past the CPU count (8 here). CPU-bound jobs would stop at the core count.

### Act 2: the prefetch trap

```bash
php bench.php 4 --backlog --prefetch=0    # publish first, then start workers 100 ms apart, no prefetch limit
php bench.php 4 --backlog                 # same, with prefetch 10
```

- Prefetch 0: `w01 200, w02-w04 0`, **10.41 s**, as slow as one worker. The first worker gets all 200 pushed into its buffer before the second one connects.
- During a `20 --backlog --prefetch=0` run, `../../bin/rabbit rabbitmqctl list_queues name messages_unacknowledged consumers` shows the backlog as unacked with 20 consumers (`199`, then `75` at 8 s as w01 works through it; the counts lag ~5 s). It looks healthy, but 19 workers are idle.
- Prefetch 10: 53 / 51 / 49 / 47, 2.76 s. **Always set `basic_qos`.**

---

## Kafka (`cd kafka`)

Each run first waits **~10-15 s** for the group to settle and prints its progress. That wait is part of the lesson: the first member grabs all 4 partitions, and the others get theirs on later heartbeats (every 5 s).

### Act 1: 1 → 4 → 20 workers

```bash
./run bench.php 1
./run bench.php 4
./run bench.php 20
```

| Workers | Busy | Wall time | Jobs/s |
|---|---|---|---|
| 1 | 1 (partitions 0, 1, 2, 3) | 10.57 s | 18 |
| 4 | 4 (one partition each) | 2.63 s | 76 |
| 20 | **4, 16 idle** | **2.59 s** | 77 |

- 20 workers are no faster than 4. The table shows `partitions: -` for the 16 idle ones.
- In a second terminal during the 20-worker run, `../../bin/kafka kafka-consumer-groups --describe --group scale.workers --members` lists 20 members, 16 of them with `#PARTITIONS 0`.

### Act 2: add partitions

```bash
../../bin/kafka kafka-topics --alter --topic scale.jobs --partitions 20
./run bench.php 20
```

- `busy 20/20`, 10 jobs each, **0.52 s**, same as RabbitMQ. So why not always do this?
  - **It only goes up.** `--partitions 2` fails: `The topic scale.jobs currently has 20 partition(s); 2 would not be an increase.`
  - **Keys move.** The partition is `hash(key) % count`, so after the change most keys map to a new partition. Old messages for key K are still queued in the old partition while new ones land in the new one, so two workers can process K at once. Per-key ordering breaks during the switch.
  - **So you plan it up front:** partitions = the most workers you will ever run, decided the day you create the topic.
- `./reset.sh` puts the topic back to 4 partitions.

Share groups (Kafka 4.2+) hand out records instead of partitions, which removes the ceiling, but php-rdkafka has no binding for them. See demo 08.

---

## Compare

| | RabbitMQ quorum queue | Kafka consumer group |
|---|---|---|
| 20 workers, 200 jobs × 50 ms | **0.52 s**, 20 busy | **2.59 s**, 4 busy, 16 idle (4 partitions) |
| Scaling knob | start more processes | partitions (fixed up front, can't shrink) **and** processes |
| A new worker starts working | immediately | after the group settles (~10 s here) |
| What you get for the limit | no ordering across workers | ordering per partition (per key) |
| Autoscale on | queue depth (e.g. KEDA RabbitMQ scaler) | consumer lag, capped at the partition count |

## Talking points

- **"Your partition count is your max worker count, and you pick it the day you create the topic."**
- The ceiling is the price of per-key ordering. That trade is fair. It just costs a PHP shop more: Java fans out with threads inside one consumer, and PHP's escape hatches (RoadRunner, Swoole, AMPHP) cost per-key ordering or a new runtime.
- RabbitMQ's flip side: competing consumers don't preserve order. Single active consumer (demo 06) gets ordering back, along with a ceiling of 1.
- The prefetch trap makes RabbitMQ look exactly like Kafka's idle workers. If a demo shows "RabbitMQ doesn't scale", check `basic_qos` first.

## Gotchas (hit while building this)

- **KIP-848 settle time.** With 4 workers, one member owned all 4 partitions at 0.1 s, and the rest only got theirs at ~10 s. Publishing before then skews the result, so `bench.php` waits until every partition has an owner and nothing has moved for 6 s.
- **Partition `"0"` is falsy in PHP.** `array_filter($assigned)` and `$p ?: '-'` silently dropped the worker that owned partition 0. Compare with `''` instead.
- **Ctrl-C in a container.** `docker run --init` forwards SIGINT to PID 1 only, so `Fleet` passes it on and the children `close()`. Afterwards `--describe` says `has no active members`. Without that, the dead members keep their partitions for the 45 s session timeout and the next run sits idle.
- **Declare the queue before forking.** Right after `reset.sh`, 20 workers racing to create the fresh quorum queue got `AMQPBasicCancelException: Channel was canceled` and the bench hung at 100/200. `bench.php` now declares it once in the parent.
- **Fork before creating any client.** librdkafka threads and AMQP sockets don't survive `pcntl_fork()`.
- **The prefetch trap is a race without the stagger.** Four workers forked at the same instant sometimes split the backlog evenly even with prefetch 0 (1 run in 5, right after `reset.sh`). In production the trap is reliable, because workers start one by one.
- **Leftover jobs.** Every run tags its jobs with a run id and skips any others, so an aborted run can't inflate the next count.

## Checklist

- [ ] RabbitMQ Act 1: 1 → 4 → 20 workers, and the time drops linearly
- [ ] RabbitMQ Act 2: reproduced the prefetch trap (1 busy, the rest idle) and fixed it with prefetch 10
- [ ] Kafka Act 1: 20 workers on 4 partitions → 16 idle, same time as 4; saw `#PARTITIONS 0` in `--describe --members`
- [ ] Kafka Act 2: added partitions and it scaled; I can give 2 reasons that isn't free
- [ ] I can explain why Kafka hands out whole partitions (ordering) and what share groups change
