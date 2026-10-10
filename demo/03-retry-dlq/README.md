# Demo 03: retry one job, then dead-letter it (~6 min)

One job fails. The others shouldn't wait for it, it should be retried with backoff, and after a limit it should land somewhere a human can look at it. RabbitMQ does this with **5 queue arguments**. A Kafka consumer group can't skip one message, so you **build it yourself**: a retry topic, a DLQ topic and a second consumer.

## You'll learn

- Quorum-queue delayed retry (4.3): `x-delayed-retry-*`, backoff = `min(min × delivery-count, max)`
- `x-delivery-limit` counts failed attempts, then the job is dead-lettered with reason `delivery_limit`
- `reject` vs `nack` on 4.3: only `reject` counts. `nack` loops forever **and** starves the jobs behind it
- Routing dead jobs by reason through a headers exchange, and why `x-match: all` ignores `x-*` headers
- The Kafka retry-topic pattern, why one retry topic means one fixed delay, and what the pattern costs

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Retry + backoff | queue arguments `x-delayed-retry-type/min/max` | your code copies the job to `retry.jobs.retry` with an `x-due-at` header; a second consumer waits for it |
| Limit | `x-delivery-limit=3`, counted by the broker | an `x-attempt` header, counted by your code |
| DLQ | `x-dead-letter-exchange` → `retry.dead`, reason in `x-death` | your code produces to `retry.jobs.dlq` |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh      # deletes the retry.* queues, recreates topics retry.jobs, retry.jobs.retry, retry.jobs.dlq (1 partition each)
```

`publish.php 5 3` publishes 5 jobs, and job 3 carries an `x-fail: timeout` header, so every run fails the same way. Workers print the time since each job was published.

---

## RabbitMQ (`cd rabbitmq`)

### Act 1: retry with backoff, then dead-letter

```bash
php worker.php reject          # terminal 1
php publish.php 5 3            # terminal 2, then wait ~20 s
php dead.php                   # drains retry.dead
```

```
[+  0.2s] job 3  delivery-count=0 acquired-count=0  FAIL (timeout) -> reject(requeue)
[+  0.3s] job 4  delivery-count=0 acquired-count=0  ok -> ack
[+  0.4s] job 5  delivery-count=0 acquired-count=0  ok -> ack
[+  3.3s] job 3  delivery-count=1 acquired-count=1  FAIL (timeout) -> reject(requeue)
[+  9.4s] job 3  delivery-count=2 acquired-count=2  FAIL (timeout) -> reject(requeue)
[+ 18.5s] job 3  delivery-count=3 acquired-count=3  FAIL (timeout) -> reject(requeue)
[retry.dead] job 3  {"x-first-death-reason":"delivery_limit","x-death.reason":"delivery_limit","x-death.queue":"retry.jobs","x-death.count":1,"x-delivery-count":4}
```

- Jobs 4 and 5 finish straight away. The broker parks job 3 and doesn't block the others.
- The gaps are 3 s, 6 s, 9 s: `min(3000 × delivery-count, 10000)` ms.
- `x-delivery-limit=3` means 3 **retries**: 4 deliveries, then dead-lettered with reason `delivery_limit`.

### Act 2: the nack trap

```bash
php worker.php nack            # terminal 1
php publish.php 5 3            # terminal 2; Ctrl-C the worker after ~10 s, then ../reset.sh
```

```
[+  0.2s] job 3  delivery-count=0 acquired-count=0  FAIL (timeout) -> nack(requeue)
[+  0.8s] job 3  delivery-count=0 acquired-count=1  FAIL (timeout) -> nack(requeue)
...
[+  9.4s] job 3  delivery-count=0 acquired-count=15 FAIL (timeout) -> nack(requeue)
```

- `delivery-count` stays at 0. Only `acquired-count` grows, so there's no backoff and no dead-lettering, ever.
- The nacked job goes straight back out first, so with prefetch 1 **jobs 4 and 5 never run**: `retry.jobs` still holds 3 messages. (The worker sleeps 0.5 s per nack only to keep the output readable.)

### Act 3: dead-letter by reason (app-level)

```bash
php worker.php route                          # terminal 1
php publish.php 5 3:timeout,4:validation      # terminal 2, then wait ~20 s
php dead.php retry.dlq.validation
php dead.php retry.dlq.timeout
```

```
[+  0.3s] job 4  delivery-count=0 acquired-count=0          FAIL (validation) -> publish to retry.dlq (x-failure-reason=validation), then ack
[+ 18.5s] job 3  delivery-count=3 acquired-count=3          FAIL (timeout) -> publish to retry.dlq (x-failure-reason=timeout), then ack
[retry.dlq.validation] job 4  {"x-failure-reason":"validation"}
[retry.dlq.timeout] job 3  {"x-delivery-count":3,"x-failure-reason":"timeout"}
```

- A validation error will never succeed, so it skips the retries. A timeout gets its retries, and on the last one the worker routes it itself instead of letting the broker send it to `retry.dead`.
- The worker republishes with publisher confirms and acks the original only after the confirm. A crash in between gives a duplicate, never a lost job.
- This is **app-level**. The native way (the consumer annotates its rejection) needs AMQP 1.0, and PHP has no mature AMQP 1.0 client.

---

## Kafka (`cd kafka`)

Wait until both consumers print `waiting on` before publishing.

```bash
./run worker.php               # terminal 1: group retry.workers on retry.jobs
./run retrier.php              # terminal 2: group retry.retrier on retry.jobs.retry
./run publish.php 5 3          # terminal 3, then wait ~12 s
./run dlq.php                  # reads retry.jobs.dlq from the start
```

```
[+  0.7s] job 3  attempt=1 FAIL (timeout) -> retry.jobs.retry, then commit
[+  0.8s] job 4  attempt=1 ok -> commit
[+  0.9s] job 5  attempt=1 ok -> commit
[+  3.8s] job 3  attempt=2 FAIL (timeout) -> retry.jobs.retry (due in 3s), then commit
[+  6.9s] job 3  attempt=3 FAIL (timeout) -> retry.jobs.retry (due in 3s), then commit
[+ 10.0s] job 3  attempt=4 FAIL (timeout) -> retry.jobs.dlq, then commit
[retry.jobs.dlq] job 3  {"x-failure-reason":"timeout","x-attempt":"4"}  (offset 0)
```

- Jobs 4 and 5 still finish at once, but only because the worker **copied job 3 out and committed it**. Without the copy, the offset can't move past job 3 (demo 06).
- The gaps are a fixed 3 s, not a backoff. Every message in one retry topic has the same delay, so the topic stays in due order and the retrier can simply wait on the head. Real backoff takes one topic and one consumer per step (`retry-3s`, `retry-6s`, ...).
- The retrier sleeps. Production code `pause()`s the partition and keeps polling, so a long delay can't exceed `max.poll.interval.ms` (5 min).
- Attempt count, due time and reason are headers you invented. The broker knows nothing about them.

---

## Compare

| | RabbitMQ | Kafka |
|---|---|---|
| You declare | 1 queue with 5 arguments, 1 DLX exchange + queue | 3 topics |
| You run | 1 worker | 2 consumers (worker + retrier) |
| Retry code | `$msg->reject(true)` | ~95 lines (`common.php`, `worker.php`, `retrier.php`): copy, headers, wait, re-copy, DLQ |
| Backoff | 3 s, 6 s, 9 s, computed by the broker | fixed per topic; backoff means more topics |
| Who counts attempts | the broker (`x-delivery-count`) | your header (`x-attempt`) |
| DLQ by reason | headers exchange + bindings, the app republishes | an `if` that picks a topic name |
| Crash between copy and commit | n/a | the retry is duplicated (at-least-once); exactly-once needs transactions |

Kafka share groups count deliveries (default limit 5) but **archive** the record after the limit. A share-group DLQ (KIP-1191) is coming in Kafka 4.4, which isn't released yet, and PHP can't use share groups anyway (demo 08).

## Talking points

- "On RabbitMQ the retry policy is 5 queue arguments. On Kafka it's a small distributed system you own." Uber and DoorDash built exactly this.
- On both sides the failed job doesn't block the others. On Kafka that's only true because you moved it out of the way.
- `reject`, not `nack`: a one-word bug that loops forever and starves the queue.

## Gotchas (hit while building this)

- `basic.nack` on 4.3 doesn't increment `delivery-count`: no backoff, no limit, no dead-lettering. The job is redelivered before the ones behind it, so with prefetch 1 they starve (acquired-count reached 15 in 10 s; jobs 4 and 5 never ran).
- `x-delivery-limit=3` allows 4 deliveries. The dead job carries `x-delivery-count: 4`.
- A headers exchange with `x-match: all` **ignores headers starting with `x-`**. The binding is left with no criteria and matches everything: both DLQs got both jobs. Use `all-with-x`/`any-with-x`, or drop the `x-` prefix.
- Re-binding with different arguments adds a second binding instead of replacing the first. `./reset.sh` deletes the queues to clear them.
- `kafka-console-consumer --timeout-ms` exits with an ERROR and a stack trace, so `kafka/dlq.php` reads the DLQ instead.
- Kafka header values are strings: cast numbers with `(string)`.

## Checklist

- [ ] RabbitMQ Act 1: jobs 4 and 5 finished at once, job 3 retried after 3 s / 6 s / 9 s, then landed in `retry.dead` with reason `delivery_limit`
- [ ] Act 2: `delivery-count` stuck at 0 with `nack`, and jobs 4 and 5 starved; I can explain why `reject` fixes it
- [ ] Act 3: each failure reason landed in its own DLQ, and I know why the binding needs `all-with-x`
- [ ] Kafka: saw worker → retry topic → retrier → DLQ topic, and can explain why the delay is fixed per retry topic
- [ ] I can list what Kafka made me build that RabbitMQ gave me as queue arguments
