# Demo 06: head-of-line blocking (~8 min)

In a Kafka partition a message's fate is coupled to its neighbours: the group commits **one offset per partition** ("everything before N is done"), so one slow or poison message blocks everything behind it. RabbitMQ acks **each message**, so competing consumers route around a slow job, at the price of ordering. Ask RabbitMQ for strict order (single active consumer) and the blocking comes back: **ordering and head-of-line blocking are the same coin.**

## You'll learn

- How to measure head-of-line blocking: every job prints `done N s after publish`
- That RabbitMQ prefetch is a head-of-line buffer too
- That `x-single-active-consumer` buys strict order, brings the blocking back, and gives you a hot standby
- Why a poison message stalls a Kafka partition forever, and that skip / retry / DLQ is code you write
- That a quorum queue drops a poison message after 20 redeliveries by default, silently unless there's a DLX

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Progress is tracked as | an ack per message | one committed offset per partition |
| A slow job blocks | the worker holding it (and its prefetch buffer) | every message behind it in the partition |
| Order | none across competing consumers; strict with single active consumer | per partition |

Every job is `{"id":3,"at":<publish time>}` ([job.php](job.php)). Job 3 sleeps 8 s (or throws, in poison mode); every other job takes 200 ms.

## Setup

One-time setup is in [../README.md](../README.md). Then, before each act:

```bash
./reset.sh      # deletes queues hol.jobs + hol.ordered, recreates topics hol.jobs (2 partitions) and hol.dlq
```

---

## RabbitMQ (`cd rabbitmq`)

### Act 1: competing consumers route around the slow job

```bash
php consume.php jobs A         # terminal 1 (prefetch 1)
php consume.php jobs B         # terminal 2
php publish.php jobs 10        # terminal 3
```

- One worker gets job 1, then job 3 (`done 8.2 s after publish`). The other takes **everything else**: 2, 4, 5, ..., 10, the last one `done 1.6 s after publish`. Which of A and B gets job 1 depends on who registered first.
- Only job 3 is late. The price: completion order is 1, 2, 4, 5, ..., 10, **3**. No ordering across competing consumers.

### Act 2: prefetch is a head-of-line buffer too

Same as Act 1, but start both workers with prefetch 10: `php consume.php jobs A 10` and `php consume.php jobs B 10`.

- The broker pushes jobs round-robin up front: one worker holds 1, 3, 5, 7, 9 and the other 2, 4, 6, 8, 10.
- The even worker is done after 1.0 s and sits idle. 5, 7, 9 are stuck in the odd worker's buffer behind job 3 (`8.4 s`, `8.6 s`, `8.8 s`). **Exactly Kafka's partition 0 below.**
- For slow or uneven jobs keep prefetch low.

### Act 3: single active consumer = strict order, and the blocking is back

```bash
php consume.php ordered A      # terminal 1
php consume.php ordered B      # terminal 2: standby
../../bin/rabbit rabbitmqctl list_consumers queue_name active activity_status
php publish.php ordered 10     # terminal 3
```

- `list_consumers` shows A `true single_active` and B `false waiting`.
- A does 1 to 10 in order. Job 3 is done at 8.4 s, and 4 to 10 follow at 8.6 to 9.9 s. B does nothing.
- Run it again and Ctrl-C A while it's on job 3. B takes over at `job 3 ... (redelivered)` and keeps the order, with 4 to 10 right after it.

---

## Kafka (`cd kafka`)

Wait until **each** worker prints its own partition before publishing (KIP-848 assignment took 7-12 s). The first worker briefly prints `partitions: 0, 1` and the second `partitions: none (idle)`; publish only after they settle on `partitions: 0` and `partitions: 1`.

### Act 1: a slow job blocks its partition

```bash
./run consume.php A            # terminal 1
./run consume.php B            # terminal 2
./run publish.php 10           # terminal 3: odd jobs -> partition 0, even -> partition 1
```

- The worker on partition 0 prints 1 (`0.7 s`), then 3 (`8.7 s`), then **5, 7, 9 at 8.9 to 9.3 s**: they waited behind job 3.
- The worker on partition 1 finishes 2 to 10 by `1.5 s` and then sits idle. It can't help, because partition 0 belongs to the other worker.
- Same picture as RabbitMQ Act 2, but there's no prefetch knob to turn: the partition *is* the buffer. Order per partition is kept.

### Act 2: a poison job blocks its partition forever

```bash
../reset.sh && ./run publish.php 10
for i in 1 2 3; do ./run consume.php A poison; done   # a supervisor restarting a crashing worker, bounded to 3
../../bin/kafka kafka-consumer-groups --describe --group hol.workers
```

- Run 1 finishes partition 1 and job 1, then `CRASH on job 3 is poison at partition 0, offset 1. Exit without commit`.
- Runs 2 and 3 resume from the committed offset, which is job 3 again, and crash again straight away. In production, supervisord would keep doing this forever.
- `--describe` shows partition 0 at **LAG 4** and partition 1 at LAG 0. Jobs 5, 7 and 9 will never run.

### Act 3: the fix is code you write

```bash
./run consume.php A dlq        # Ctrl-C once job 9 is done
../../bin/kafka kafka-console-consumer --topic hol.dlq --from-beginning --formatter-property print.key=true --max-messages 1
```

- `job 3 is poison: parked in hol.dlq, committing past offset 1`, then 5, 7 and 9 are done. LAG is 0 on both partitions.
- `hol.dlq` holds `job-3  {"id":3,...}`. Kafka only gives you the offset. Skip-and-log, a retry topic ([demo 03](../03-retry-dlq/README.md)), a DLQ topic, and handling a failed DLQ produce are all yours to build.

---

## Compare

| | RabbitMQ quorum queue | Kafka consumer group |
|---|---|---|
| Ordering guarantee | none with competing consumers; strict with `x-single-active-consumer` (one worker at a time) | per partition, so per key |
| What a slow message blocks | its worker + that worker's prefetch buffer | every message behind it in the partition |
| Other workers | take the rest of the queue | idle: they own other partitions |
| Poison message | `reject(requeue)` redelivers; after `x-delivery-limit` (default 20) dropped, or dead-lettered with a DLX | redelivered after every restart, forever |
| Knobs | prefetch, single active consumer, delivery limit + DLX | partition count (fixed up front), key choice, your own skip/retry/DLQ code |

## Talking points

- Kafka commits an offset, not a message. That single integer is why it's cheap, and why a neighbour's fate is yours.
- RabbitMQ acks a message. That's why a worker can skip ahead, and why order is gone.
- Need order in RabbitMQ? Use single active consumer (or a queue per key) and you get Kafka's blocking back. Ordering and head-of-line blocking are the same coin; you choose where to pay.
- Share groups ([demo 08](../08-share-groups/README.md)) **reduce** head-of-line blocking because any member can take any record, but don't **eliminate** it. The in-flight window (2000 offsets by default) is measured from the oldest unfinished record, so one stuck record stops the partition being fetched 2000 offsets later.

## Gotchas (hit while building this)

- Prefetch decides RabbitMQ's head-of-line blocking: prefetch 10 recreates Kafka's picture (Act 2).
- A crashed Kafka worker that skips `close()` holds its partitions for the 45 s session timeout. `poison` mode calls `close()` before `exit(1)`, so the 3 restarts in Act 2 take ~7 s in total. With a real crash (OOM, `kill -9`) each one waits 45 s.
- Quorum queues in RabbitMQ 4.x default to a delivery limit of 20. A rejected poison message was delivered 21 times and then **dropped silently**, because there was no DLX. Set `x-dead-letter-exchange` ([demo 03](../03-retry-dlq/README.md)).
- Queue arguments are immutable: redeclaring `hol.jobs` with single active consumer fails with `PRECONDITION_FAILED - inequivalent arg 'x-single-active-consumer'`. That's why it gets its own queue, `hol.ordered`.
- Which worker gets partition 0 changes from run to run. Read the `partitions:` line.
- `kafka-console-consumer --property` is deprecated in 4.3: use `--formatter-property`. `--timeout-ms` ends with an ERROR line, so use `--max-messages`.

## Checklist

- [ ] RabbitMQ Act 1: only job 3 is late, and I can say why completion order no longer matches publish order
- [ ] RabbitMQ Act 2: with prefetch 10, jobs 5, 7, 9 wait behind job 3 while B is idle
- [ ] RabbitMQ Act 3: B is `waiting`; killing A mid-job-3 hands job 3 to B as redelivered, order kept
- [ ] Kafka Act 1: jobs behind job 3 on its partition are ~8 s late, and the other worker can't help
- [ ] Kafka Acts 2-3: the poison job leaves LAG stuck at 4; `dlq` mode parks it and LAG drops to 0
- [ ] I can explain "ordering and head-of-line blocking are the same coin" in one sentence
