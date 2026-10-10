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
| Watch it | UI → Queues → `scale.jobs`: Consumers, Unacked (only the ~10 s runs last long enough to see) | `../../bin/kafka kafka-consumer-groups --describe --group scale.workers --members` |

## Setup

One-time setup is in [../README.md](../README.md). Then, once before you start (and any time you want a clean slate; it also undoes Kafka Act 2's `--alter`):

```bash
./reset.sh      # deletes queue scale.jobs, recreates topic scale.jobs with 4 partitions
```

UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.

Both sides use `bench.php <workers>`: it forks N worker processes ([fleet.php](fleet.php)), waits until they're ready, publishes 200 jobs that each `usleep(50_000)` (I/O-bound, like an HTTP call), waits until all 200 are done, then prints jobs per worker, idle workers and wall time. The clock starts when the jobs are published, so Kafka's settle wait is not included (see Compare: "A new worker starts working").

`rabbitmq/bench.php` forks on the host, so it needs `ext-pcntl` and `ext-posix` (`php -m | grep -E 'pcntl|posix'`). Native Windows PHP has neither; use WSL.

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

- Throughput grows linearly (time ∝ 1/workers). Workers are ready in milliseconds: no rebalance (unlike Kafka), no settle time.
- I/O-bound jobs scale far past the CPU count (8 here). CPU-bound jobs would stop at the core count.

### Act 2: the prefetch trap

Prefetch (`$ch->basic_qos(0, N, false)`) is the most unacked messages the broker pushes to one consumer before it waits for an ack. 0 means unlimited, and so does never calling `basic_qos`. Act 1 ran with bench.php's default of 10.

```bash
php bench.php 4 --backlog --prefetch=0    # publish first, then start workers 100 ms apart, no prefetch limit
php bench.php 4 --backlog                 # same, with prefetch 10
php bench.php 20 --backlog --prefetch=0   # terminal 1, ~10 s
../../bin/rabbit rabbitmqctl list_queues name messages_unacknowledged consumers   # terminal 2, repeat while it runs
```

- Prefetch 0: `w01 200, w02-w04 0`, **10.41 s**, as slow as one worker. The first worker gets all 200 pushed into its buffer before the second one connects.
- Terminal 2: stats refresh every 5 s, so the first calls print stale numbers (`0 0`, or `200 1` before the others connect). Then the backlog shows as unacked with 20 consumers (~100-150, shrinking as w01 works through it). It looks healthy, but 19 workers are idle.
- Prefetch 10: 53 / 51 / 49 / 47, 2.76 s. **Always set `basic_qos`**: 1 gives the fairest split for slow jobs (RabbitMQ tutorial 2), higher values (tens to hundreds) save round trips on fast ones.

---

## Kafka (`cd kafka`)

The workers use Kafka's new consumer group protocol (KIP-848; bench.php opts in with `group.protocol=consumer`). The broker decides which member owns which partition, and each member picks up changes on its next heartbeat (every 5 s by default). Moving partitions between members is a *rebalance*.

Each run first waits **~7-18 s** for the group to settle and prints its progress. That wait is part of the lesson: the first member grabs all 4 partitions, and the others get theirs on later heartbeats (every 5 s, the broker default).

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
- In a second terminal, once the progress line reads `4/4 partitions owned by 4/20 workers` (you then have ~8 s before the run ends), run `../../bin/kafka kafka-consumer-groups --describe --group scale.workers --members`: 20 members, 16 with `#PARTITIONS 0`. Earlier, you see `Warning: ... is rebalancing` and one member holding all 4 partitions: that is the settle, not the ceiling.

### Act 2: add partitions

```bash
../../bin/kafka kafka-topics --alter --topic scale.jobs --partitions 20
./run bench.php 20
```

- `busy 20/20`, 10 jobs each, **0.52 s**, same as RabbitMQ. So why not always do this?
  - **It only goes up.** `--partitions 2` fails: `The topic scale.jobs currently has 20 partition(s); 2 would not be an increase.`
  - **Keys move.** The partition is `hash(key) % count`, so after the change most keys map to a new partition. Old messages for key K are still queued in the old partition while new ones land in the new one, so two workers can process K at once. Per-key ordering breaks during the switch.
  - **Late starts.** Consumers with `auto.offset.reset=latest` can miss the first records on new partitions, before they discover them (bench.php uses `earliest`).
  - **So you size it up front:** growing works online (you just did it) but remaps keys, and shrinking is impossible. Unkeyed jobs can simply over-partition.
- `../reset.sh` puts the topic back to 4 partitions.

Share groups (Kafka 4.2+) let many members read one partition and lock records one by one. That removes the partition ceiling (up to 200 members by default) but gives up per-partition ordering, the thing the ceiling paid for. php-rdkafka has no binding for them. See demo 08.

---

## Compare

| | RabbitMQ quorum queue | Kafka consumer group |
|---|---|---|
| 20 workers, 200 jobs × 50 ms | **0.52 s**, 20 busy | **2.59 s**, 4 busy, 16 idle (4 partitions) |
| Scaling knob | start more processes | partitions (grow-only, growing remaps keys) **and** processes |
| A new worker starts working | immediately | after up to 2 heartbeats (~10 s at the 5 s default); the members that already have partitions keep working meanwhile |
| Ordering (the trade-off) | none across workers | per partition (per key) |
| Autoscale on | queue depth (e.g. the KEDA RabbitMQ scaler for Kubernetes) | consumer lag, capped at the partition count |

## Talking points

- **"Your partition count is your max worker count, and it only goes up."**
- The ceiling is the price of per-key ordering. That trade is fair. It just costs a PHP shop more: Java fans out with threads inside one consumer, and PHP's escape hatches (RoadRunner, Swoole, AMPHP) cost per-key ordering or a new runtime.
- RabbitMQ's flip side: competing consumers don't preserve order. Single active consumer (demo 06) gets it back with a ceiling of 1; per-key order with N workers means a consistent-hash exchange into N queues, each with single active consumer. That is partitions by hand, with the same ceiling.
- The prefetch trap makes RabbitMQ look exactly like Kafka's idle workers. If a demo shows "RabbitMQ doesn't scale", check `basic_qos` first.

## Gotchas (hit while building this)

- **KIP-848 settle time.** With 4 workers, one member owned all 4 partitions at 0.1 s, and the rest only got theirs at ~10 s. Publishing before then skews the result, so `bench.php` waits until every partition has an owner and nothing has moved for 6 s.
- **Partition `"0"` is falsy in PHP.** `array_filter($assigned)` and `$p ?: '-'` silently dropped the worker that owned partition 0. Compare with `''` instead.
- **Ctrl-C in a container.** `docker run --init` forwards Ctrl-C to the PHP parent only, not to its forked workers, so `Fleet` passes it on and the children `close()`. Afterwards `--describe` says `has no active members`. Without that, the dead members keep their partitions for the 45 s session timeout and the next run sits idle.
- **Declare the queue before forking.** Right after `reset.sh`, 20 workers racing to create the fresh quorum queue got `AMQPBasicCancelException: Channel was canceled` and the bench hung at 100/200. `bench.php` now declares it once in the parent **and closes that connection before forking**.
- **Don't carry a client across `pcntl_fork()`.** librdkafka's background threads don't survive it, and an AMQP connection shared by two processes interleaves frames. Each worker creates its own.
- **The prefetch trap is a race without the stagger.** Four workers forked at the same instant sometimes split the backlog evenly even with prefetch 0 (1 run in 5, right after `reset.sh`). In production the trap is reliable, because workers start one by one.
- **Leftover jobs.** Every run tags its jobs with a run id and skips any others, so an aborted run can't inflate the next count.

## Checklist

- [ ] RabbitMQ Act 1: 1 → 4 → 20 workers, and throughput grows linearly
- [ ] RabbitMQ Act 2: reproduced the prefetch trap (1 busy, the rest idle) and fixed it with prefetch 10
- [ ] Kafka Act 1: 20 workers on 4 partitions → 16 idle, same time as 4; saw `#PARTITIONS 0` in `--describe --members`
- [ ] Kafka Act 2: added partitions and it scaled; I can give 2 reasons that isn't free
- [ ] I can explain why Kafka hands out whole partitions (ordering) and what share groups change
