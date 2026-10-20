# Demo 06: head-of-line blocking (~8 min)

In a Kafka partition a message's fate is coupled to its neighbours: the group commits **one offset per partition** ("everything before N is done"), so a worker that processes a partition in order (every PHP worker) is blocked by one slow or poison message. RabbitMQ acks **each message**, so competing consumers route around a slow job, at the price of ordering. Ask RabbitMQ for strict order (single active consumer) and the blocking comes back: **ordering and head-of-line blocking are the same coin.**

## You'll learn

- How to measure head-of-line blocking: every job prints `done N s after publish`
- That RabbitMQ prefetch is a head-of-line buffer too
- That `x-single-active-consumer` buys strict order, brings the blocking back, and gives you a hot standby
- Why a poison message stalls a Kafka partition forever, and that skip / retry / DLQ is code you write

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Progress is tracked as | an ack per message | one committed offset per partition |
| A slow job blocks | the worker holding it (and its prefetch buffer) | every message behind it in the partition |
| Order | none across competing consumers; strict with single active consumer | per partition |

Every job is `{"id":3,"at":<publish time>}` ([job.php](job.php)). Job 3 sleeps 8 s (or throws, in poison mode); every other job takes 200 ms.

## Setup

One-time setup is in [../README.md](../README.md). Then, before each act except Kafka Act 3 (it continues from Act 2), stop all workers and run:

```bash
# host (repo root)
demo/06-head-of-line/reset.sh      # deletes queues hol.jobs + hol.ordered, recreates topics hol.jobs (2 partitions) and hol.dlq
```

### The scripts

| Script | What it does | Arguments |
|---|---|---|
| `rabbitmq/consume.php <queue> <name> [prefetch]` | Consumes a queue until Ctrl-C: runs each job, then acks it | `<queue>`: `jobs` = `hol.jobs` (Acts 1-2), `ordered` = `hol.ordered`, the single-active-consumer queue (Act 3). `<name>`: the worker's label and consumer tag, e.g. `A`. `[prefetch]`: how many unacked jobs the broker pushes to it at once, default 1 |
| `rabbitmq/publish.php <queue> <count>` | Publishes job 1 … job `<count>` to that queue, then exits | `<queue>`: `jobs` or `ordered`, as above. `<count>`: how many jobs, default 10 |
| `kafka/consume.php <name> [mode]` | Consumes `hol.jobs` in group `hol.workers` until Ctrl-C: runs each job, then commits its offset | `<name>`: the worker's label, e.g. `A`. `[mode]`: `slow` (default, Act 1): job 3 sleeps 8 s. `poison` (Act 2): job 3 throws and the worker exits without committing. `dlq` (Act 3): job 3 throws, the worker copies it to `hol.dlq` and commits past it |
| `kafka/publish.php <count>` | Publishes job 1 … job `<count>` to `hol.jobs` with key `job-<n>`, odd jobs to partition 0 and even to partition 1, then exits | `<count>`: how many jobs, default 10 |

Jobs do no real work: `work()` in [job.php](job.php) just sleeps, and job 3 is hard-coded as the bad one, so every run blocks on the same job.

Reading a worker line, e.g. `[A] job  3  done  8.7 s after publish  (partition 0, offset 1)`:

- `[A]`: the worker's `<name>`.
- `done 8.7 s after publish`: from publish to the end of the job, waiting time included. A 200 ms job that shows ~8 s waited behind job 3.
- `(partition 0, offset 1)` (Kafka): where the job sits in `hol.jobs`.
- `(redelivered)` (RabbitMQ): the broker handed this job out before and it was never acked (Act 3).
- `partitions: 0` (Kafka): the partitions this worker owns now, printed whenever that changes; `none (idle)` = none.

---

## RabbitMQ (`rabbitmq/`)

### Act 1: competing consumers route around the slow job

```bash
# php container (/demo)

# terminal 1: worker "A" on hol.jobs ("jobs"); no prefetch argument = prefetch 1, so it holds one job at a time
php 06-head-of-line/rabbitmq/consume.php jobs A

# terminal 2: worker "B" on the same queue, competing with A
php 06-head-of-line/rabbitmq/consume.php jobs B

# terminal 3: publish job 1..10 ("10") to hol.jobs; job 3 is the 8 s one. Watch which worker finishes what
php 06-head-of-line/rabbitmq/publish.php jobs 10
```

- One worker gets job 1, then job 3 (`done 8.2 s after publish`). The other takes **everything else**: 2, 4, 5, ..., 10, the last one `done 1.6 s after publish`. Which of A and B gets job 1 depends on who registered first.
- Only job 3 is late. The price: completion order is 1, 2, 4, 5, ..., 10, **3**. No ordering across competing consumers.

### Act 2: prefetch is a head-of-line buffer too

Ctrl-C A and B, run `demo/06-head-of-line/reset.sh` on the host, then start both workers with prefetch 10: `php 06-head-of-line/rabbitmq/consume.php jobs A 10` and `php 06-head-of-line/rabbitmq/consume.php jobs B 10`. Publish as in Act 1.

- The broker pushes jobs round-robin up front: one worker holds 1, 3, 5, 7, 9 and the other 2, 4, 6, 8, 10.
- The even worker is done after 1.0 s and sits idle. 5, 7, 9 are stuck in the odd worker's buffer behind job 3 (`8.4 s`, `8.6 s`, `8.8 s`). **Exactly Kafka's partition 0 below.**
- For slow or uneven jobs keep prefetch low.

### Act 3: single active consumer = strict order, and the blocking is back

`x-single-active-consumer`: the queue delivers to one consumer at a time; the others wait as hot standbys and take over when it disconnects.

```bash
# php container (/demo)

# terminal 1: worker "A" on hol.ordered ("ordered"), the single-active-consumer queue; it attaches first, so it becomes the active one
php 06-head-of-line/rabbitmq/consume.php ordered A

# terminal 2: worker "B" on the same queue; it attaches second, so it waits as the standby
php 06-head-of-line/rabbitmq/consume.php ordered B

# host (repo root)

# show each consumer's queue, tag (A or B), whether it's active, and its status; grep keeps only hol.ordered
demo/bin/rabbit rabbitmqctl list_consumers queue_name consumer_tag active activity_status | grep hol.ordered

# php container (/demo)

# terminal 3: publish job 1..10 ("10") to hol.ordered; job 3 takes 8 s and every job behind it waits
php 06-head-of-line/rabbitmq/publish.php ordered 10
```

- `list_consumers` shows `A true single_active` and `B false waiting`.
- A does 1 to 10 in order. Job 3 is done at 8.4 s, and 4 to 10 follow at 8.6 to 9.9 s. B does nothing.
- Leave A and B running and re-publish (`php 06-head-of-line/rabbitmq/publish.php ordered 10`). As soon as A prints `job 2`, Ctrl-C A: you have ~8 s while it's on job 3. B then prints `job 3 ... (redelivered)` and 4 to 10 right after it, in order.

---

## Kafka (`kafka/`)

Wait until **each** worker prints its own partition before publishing (KIP-848 assignment took 7-12 s). The first worker briefly prints `partitions: 0, 1` and the second `partitions: none (idle)`; publish only after they settle on `partitions: 0` and `partitions: 1`.

### Act 1: a slow job blocks its partition

```bash
# php container (/demo)

# terminal 1: worker "A" in group hol.workers on topic hol.jobs; no mode = "slow": job 3 sleeps 8 s
06-head-of-line/kafka/run consume.php A

# terminal 2: worker "B" in the same group; the 2 partitions get split between A and B
06-head-of-line/kafka/run consume.php B

# terminal 3: once A and B print partitions: 0 and partitions: 1, publish job 1..10 ("10"): odd jobs -> partition 0, even -> partition 1
06-head-of-line/kafka/run publish.php 10
```

- The worker on partition 0 prints 1 (`0.7 s`), then 3 (`8.7 s`), then **5, 7, 9 at 8.9 to 9.3 s**: they waited behind job 3.
- The worker on partition 1 finishes 2 to 10 by `1.5 s` and then sits idle. It can't help, because partition 0 belongs to the other worker.
- Same picture as RabbitMQ Act 2, but there's no prefetch knob to turn: the partition *is* the buffer. Order per partition is kept.
- Ctrl-C both workers before Act 2: the poison runs need the group to themselves.

### Act 2: a poison job blocks its partition forever

```bash
# host (repo root)

# Ctrl-C the Act 1 workers first, then recreate the topics (this also drops group hol.workers' committed offsets)
demo/06-head-of-line/reset.sh

# php container (/demo)

# terminal 1: publish job 1..10 ("10") again: odd jobs -> partition 0, even -> partition 1
06-head-of-line/kafka/run publish.php 10

# terminal 1: run worker "A" in "poison" mode (job 3 throws, the worker exits without committing) 3 times in a row:
# a supervisor restarting a crashing worker, bounded to 3
for i in 1 2 3; do 06-head-of-line/kafka/run consume.php A poison; done

# host (repo root)

# show group hol.workers per partition: committed offset, end of the log, and LAG (messages not done yet)
demo/bin/kafka kafka-consumer-groups --describe --group hol.workers
```

- Run 1 finishes partition 1 and job 1, then `CRASH on job 3 is poison at partition 0, offset 1. Exit without commit`.
- Runs 2 and 3 resume from the committed offset, which is job 3 again, and crash again straight away. In production, supervisord would keep doing this forever.
- `--describe` shows partition 0 at **LAG 4** (log-end offset 5 minus committed offset 1: job 3 plus 5, 7, 9) and partition 1 at LAG 0. Jobs 5, 7 and 9 will never run.

### Act 3: the fix is code you write (no reset: continues from Act 2)

```bash
# php container (/demo)

# terminal 1: worker "A" in "dlq" mode: job 3 still throws, but the worker copies it to topic hol.dlq and commits past it.
# Ctrl-C once job 9 is done
06-head-of-line/kafka/run consume.php A dlq

# host (repo root)

# print hol.dlq from the start, with each message's key (print.key=true), and exit after 1 message
demo/bin/kafka kafka-console-consumer --topic hol.dlq --from-beginning --formatter-property print.key=true --max-messages 1

# show the group's committed offset and LAG per partition again
demo/bin/kafka kafka-consumer-groups --describe --group hol.workers
```

- `job 3 is poison: parked in hol.dlq, committing past offset 1`, then 5, 7 and 9 are done. LAG is 0 on both partitions.
- `hol.dlq` holds `job-3  {"id":3,...}`. Kafka's consumer API (the only one PHP has) gives you just the offset. Streams and Connect have a built-in DLQ, but from a PHP worker skip-and-log, a retry topic ([demo 03](../03-retry-dlq/README.md)), a DLQ topic and handling a failed DLQ produce are all yours to build.

---

## Compare

| | RabbitMQ quorum queue | Kafka consumer group |
|---|---|---|
| Ordering guarantee | none with competing consumers; strict with `x-single-active-consumer` (one worker at a time); per key with `x-modulus-hash` + N single-active-consumer queues | per partition, so per key |
| What a slow message blocks | competing: its worker + that worker's prefetch buffer; single active consumer: the whole queue | every message behind it in the partition |
| Other workers | competing: take the rest of the queue; single active consumer: idle standbys | idle: they own other partitions |
| Poison message | `reject(requeue)` or a worker crash: requeued at the head, dropped after `x-delivery-limit` (default 20) or dead-lettered with a DLX. `nack(requeue)` doesn't count on 4.3: it loops forever and starves a prefetch-1 worker ([demo 03](../03-retry-dlq/README.md) Act 2) | redelivered after every restart, forever |
| Knobs | prefetch, single active consumer, delivery limit + DLX | partition count (can grow, never shrink; growing remaps keys), key choice, your own skip/retry/DLQ code |

## Talking points

- Kafka commits an offset, not a message. That single integer is why it's cheap, and why a neighbour's fate is yours.
- The blocking lives in the client, not the log: Java's Confluent Parallel Consumer processes past a slow record and stores the completed-offset gaps in commit metadata. PHP has no such client.
- RabbitMQ acks a message. That's why a worker can skip ahead, and why order is gone.
- Need order in RabbitMQ? Single active consumer (global order, one worker), or an `x-modulus-hash` exchange in front of N single-active-consumer queues (per-key order, N workers). Either way you've rebuilt Kafka's partitions, blocking included. Ordering and head-of-line blocking are the same coin; you choose where to pay.
- Share groups ([demo 08](../08-share-groups/README.md)) **reduce** head-of-line blocking because any member can take any record, but don't **eliminate** it. The in-flight window (2000 offsets by default) is measured from the oldest unfinished record, so one stuck record stops the partition being fetched 2000 offsets later. And they buy it by giving up per-partition order: the same coin again.

## Gotchas (hit while building this)

- Prefetch decides RabbitMQ's head-of-line blocking: prefetch 10 recreates Kafka's picture (Act 2).
- A crashed Kafka worker that skips `close()` holds its partitions for the 45 s session timeout. `poison` mode calls `close()` before `exit(1)`, so the 3 restarts in Act 2 take ~7 s in total. With a real crash (OOM, `kill -9`) each one waits 45 s.
- Quorum queues in RabbitMQ 4.x default to a delivery limit of 20. A rejected poison message was delivered 21 times and then **dropped silently**, because there was no DLX. Set `x-dead-letter-exchange` ([demo 03](../03-retry-dlq/README.md)).
- Queue arguments are immutable: redeclaring `hol.jobs` with single active consumer fails with `PRECONDITION_FAILED - inequivalent arg 'x-single-active-consumer'`. That's why it gets its own queue, `hol.ordered`.
- Single active consumer keeps order across a crash only because the default delivery limit (20) requeues job 3 at the front. With `x-delivery-limit=-1` it would come back after job 10.
- Which worker gets partition 0 changes from run to run. Read the `partitions:` line.
- `kafka-console-consumer --property` is deprecated in 4.3: use `--formatter-property`. `--timeout-ms` ends with an ERROR line, so use `--max-messages`.

## Checklist

- [ ] RabbitMQ Act 1: only job 3 is late, and I can say why completion order no longer matches publish order
- [ ] RabbitMQ Act 2: with prefetch 10, jobs 5, 7, 9 wait behind job 3 while B is idle
- [ ] RabbitMQ Act 3: B is `waiting`; killing A mid-job-3 hands job 3 to B as redelivered, order kept
- [ ] Kafka Act 1: jobs behind job 3 on its partition are ~8 s late, and the other worker can't help
- [ ] Kafka Acts 2-3: the poison job leaves LAG stuck at 4; `dlq` mode parks it and LAG drops to 0
- [ ] I can explain "ordering and head-of-line blocking are the same coin" in one sentence
