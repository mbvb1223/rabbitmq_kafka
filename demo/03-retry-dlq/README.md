# Demo 03: retry one job, then dead-letter it (~6 min)

One job fails. The others shouldn't wait for it, it should be retried with backoff, and after a limit it should land somewhere a human can look at it. RabbitMQ does this with **5 queue arguments**. A Kafka consumer group keeps one committed offset per partition, so it can't set one job aside and come back to it later: it either waits on the job or commits past it and loses it. So you **build it yourself**: a retry topic, a DLQ topic and a second consumer.

Dead-lettering: after the limit, the broker republishes the job to a dead-letter exchange (DLX), which feeds a dead-letter queue (DLQ), and adds `x-death` headers saying why.

## You'll learn

- Quorum-queue delayed retry (4.3): `x-delayed-retry-*`, a linear backoff `min(retry-min × delivery-count, retry-max)`
- `x-delivery-limit` counts failed attempts, then the job is dead-lettered with reason `delivery_limit`
- `reject` vs `nack` on 4.3: only `reject` counts. `nack` loops forever **and** starves the jobs behind it
- Routing dead jobs by reason through a headers exchange, and why `x-match: all` ignores `x-*` headers
- The Kafka retry-topic pattern, why one retry topic means one fixed delay, and what the pattern costs

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Retry + backoff | queue arguments `x-delayed-retry-type/min/max` | your code copies the job to `retry.jobs.retry` with an `x-due-at` header; a second consumer waits until it's due, runs the job again, and re-copies it or sends it to the DLQ |
| Limit | `x-delivery-limit=3`, counted by the broker | an `x-attempt` header, counted by your code |
| DLQ | `x-dead-letter-exchange` → `retry.dead`, reason in `x-death` | your code produces to `retry.jobs.dlq` |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh      # deletes the retry.* queues, recreates topics retry.jobs, retry.jobs.retry, retry.jobs.dlq (1 partition each)
```

UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.

`publish.php 5 3` publishes 5 jobs, and job 3 carries an `x-fail: timeout` header, so every run fails the same way. Workers print the time since each job was published.

---

## RabbitMQ (`cd rabbitmq`)

`bootstrap.php` declares `retry.jobs` as a quorum queue with these arguments:

| Argument | Meaning |
|---|---|
| `x-delivery-limit=3` | after 3 failed retries, dead-letter |
| `x-delayed-retry-type=failed` | delay only deliveries that counted as failed (`reject`) |
| `x-delayed-retry-min=3000`, `x-delayed-retry-max=10000` | linear: delay = min(3000 × delivery-count, 10000) ms = 3, 6, 9 s; the cap only matters from the 4th retry |
| `x-dead-letter-exchange=retry.dlx` | where the broker sends the job after the limit |

Each redelivery carries `x-delivery-count` (failed attempts: `reject` only) and `x-acquired-count` (every earlier hand-out to a consumer). On 4.3 a requeued `nack` means "give it back", not "failed", so it is neither counted nor delayed.

### Act 1: retry with backoff, then dead-letter

```bash
php worker.php reject          # terminal 1
php publish.php 5 3            # terminal 2, then wait ~20 s
php dead.php                   # drains retry.dead; then Ctrl-C the worker
```

```
# jobs 1-2: ok -> ack (omitted)
[+  0.2s] job 3  delivery-count=0 acquired-count=0          FAIL (timeout) -> reject(requeue)
[+  0.3s] job 4  delivery-count=0 acquired-count=0          ok -> ack
[+  0.4s] job 5  delivery-count=0 acquired-count=0          ok -> ack
[+  3.3s] job 3  delivery-count=1 acquired-count=1          FAIL (timeout) -> reject(requeue)
[+  9.4s] job 3  delivery-count=2 acquired-count=2          FAIL (timeout) -> reject(requeue)
[+ 18.5s] job 3  delivery-count=3 acquired-count=3          FAIL (timeout) -> reject(requeue)
[retry.dead] job 3  {"x-first-death-reason":"delivery_limit","x-death.reason":"delivery_limit","x-death.queue":"retry.jobs","x-death.count":1,"x-delivery-count":4}
[retry.dead] 1 message(s)
```

- Jobs 4 and 5 finish straight away. The broker parks job 3 and doesn't block the others.
- The gaps are 3 s, 6 s, 9 s: `min(3000 × delivery-count, 10000)` ms.
- `x-delivery-limit=3` means 3 **retries**: 4 deliveries, then dead-lettered with reason `delivery_limit`.
- `x-death.count` is 1 (dead-lettered once); `x-delivery-count` is 4 (failed deliveries).

### Act 2: the nack trap

```bash
php worker.php nack            # terminal 1
php publish.php 5 3            # terminal 2; after ~10 s the UI shows retry.jobs Total 3 (2 ready + 1 unacked), then Ctrl-C the worker and ../reset.sh
```

```
# jobs 1-2: ok -> ack (omitted)
[+  0.2s] job 3  delivery-count=0 acquired-count=0          FAIL (timeout) -> nack(requeue)
[+  0.8s] job 3  delivery-count=0 acquired-count=1          FAIL (timeout) -> nack(requeue)
...
[+  9.4s] job 3  delivery-count=0 acquired-count=15         FAIL (timeout) -> nack(requeue)
```

- `delivery-count` stays at 0. Only `acquired-count` grows, so there's no backoff and no dead-lettering, ever.
- The nacked job goes straight back out first, so with prefetch 1 **jobs 4 and 5 never run**: `retry.jobs` still holds 3 messages. (The worker sleeps 0.5 s per nack only to keep the output readable.)

### Act 3: dead-letter by reason (app-level)

```bash
php worker.php route                          # terminal 1
php publish.php 5 3:timeout,4:validation      # terminal 2, then wait ~20 s
php dead.php retry.dlq.validation
php dead.php retry.dlq.timeout                # then Ctrl-C the worker
```

```
# jobs 1, 2, 5 (ok -> ack) and job 3's rejects at +0.2/3.3/9.4 s omitted
[+  0.3s] job 4  delivery-count=0 acquired-count=0          FAIL (validation) -> publish to retry.dlq (x-failure-reason=validation), then ack
[+ 18.5s] job 3  delivery-count=3 acquired-count=3          FAIL (timeout) -> publish to retry.dlq (x-failure-reason=timeout), then ack
[retry.dlq.validation] job 4  {"x-failure-reason":"validation"}
[retry.dlq.validation] 1 message(s)
[retry.dlq.timeout] job 3  {"x-delivery-count":3,"x-failure-reason":"timeout"}
[retry.dlq.timeout] 1 message(s)
```

- A validation error will never succeed, so it skips the retries. A timeout gets its retries, and on the last one the worker routes it itself instead of letting the broker send it to `retry.dead`.
- `retry.dlq` is a headers exchange: it routes on message headers, not a routing key. Each DLQ is bound with `x-match: all-with-x`, `x-failure-reason: <reason>`. A reason with no binding goes to the exchange's alternate exchange `retry.dlx`, so it lands in `retry.dead` instead of being dropped.
- `x-delivery-count` here is the header the worker copied on its 4th delivery (3 failed so far), not a count the DLQ keeps.
- The worker republishes with publisher confirms (the broker acks each publish) and acks the original only after the confirm. A crash in between gives a duplicate, never a lost job.
- This is **app-level**. The native way (the consumer annotates its rejection) needs AMQP 1.0, and PHP has no mature AMQP 1.0 client.

---

## Kafka (`cd kafka`)

Wait until both consumers print `waiting on` before publishing.

```bash
./run worker.php               # terminal 1: group retry.workers on retry.jobs
./run retrier.php              # terminal 2: group retry.retrier on retry.jobs.retry
./run publish.php 5 3          # terminal 3, then wait ~12 s
./run dlq.php                  # reads retry.jobs.dlq from the start; then Ctrl-C the worker and the retrier
```

```
# terminal 1 (worker); jobs 1-2: ok -> commit (omitted)
[+  0.7s] job 3  attempt=1 FAIL (timeout) -> retry.jobs.retry, then commit
[+  0.8s] job 4  attempt=1 ok -> commit
[+  0.9s] job 5  attempt=1 ok -> commit
# terminal 2 (retrier)
[+  3.8s] job 3  attempt=2 FAIL (timeout) -> retry.jobs.retry (due in 3s), then commit
[+  6.9s] job 3  attempt=3 FAIL (timeout) -> retry.jobs.retry (due in 3s), then commit
[+ 10.0s] job 3  attempt=4 FAIL (timeout) -> retry.jobs.dlq, then commit
# dlq.php
[retry.jobs.dlq] job 3  {"x-failure-reason":"timeout","x-attempt":"4"}  (offset 0)
[retry.jobs.dlq] 1 message(s)
```

- Jobs 4 and 5 still finish at once, but only because the worker **copied job 3 out and committed it**. Without the copy, committing past job 3 would lose it, and not committing blocks the partition (demo 06).
- The gaps are a fixed 3 s, not a backoff. Every message in one retry topic has the same delay, so the topic stays in due order and the retrier can simply wait on the head. Real backoff takes one retry topic per step (`retry-3s`, `retry-6s`, ...), and a consumer that pauses each one until its head is due.
- The retrier sleeps. Production code calls `$consumer->pausePartitions()` and keeps polling, so a long delay can't exceed `max.poll.interval.ms` (5 min).
- Attempt count, due time and reason are headers you invented. The broker knows nothing about them.

---

## Compare

| | RabbitMQ | Kafka |
|---|---|---|
| You declare | 1 queue with 5 arguments, 1 DLX exchange + queue | 3 topics |
| You run | 1 worker | 2 consumers (worker + retrier) |
| Retry code | `reject(true)` + 5 queue arguments | a second consumer (`retrier.php`, 33 lines) + `send()` with delivery check + attempt/due headers |
| Backoff | 3 s, 6 s, 9 s, computed by the broker | fixed per topic; backoff means more topics |
| Who counts attempts | the broker (`x-delivery-count`) | your header (`x-attempt`) |
| DLQ by reason | headers exchange + bindings, the app republishes | an `if` that picks a topic name (not built here) |
| Crash between copy and ack/commit | retries: n/a, nothing is copied. Act 3's DLQ republish: a duplicate in the DLQ | every retry is a copy, so a crash duplicates it: at-least-once (it may run twice, never zero times). Exactly-once needs `sendOffsetsToTransaction`, which php-rdkafka doesn't expose, so a PHP consumer has to dedupe |

JVM frameworks generate this for you (Spring Kafka `@RetryableTopic`, Kafka Connect and Kafka Streams DLQs), but it's still client-side retry topics and consumers, not a broker feature, and PHP has no equivalent.

Kafka share groups count deliveries (default limit 5) but **archive** the record after the limit (mark it done and never deliver it again). A share-group DLQ (KIP-1191) is coming in Kafka 4.4 (not released as of Oct 2026; check before the talk), and PHP can't use share groups anyway (demo 08).

## Talking points

- "On RabbitMQ the retry policy is 5 queue arguments. On Kafka it's a small distributed system you own." Uber published exactly this design in 2018: retry topics with growing delays plus a DLQ (uber.com/blog/reliable-reprocessing).
- On both sides the failed job doesn't block the others. On Kafka that's only true because you moved it out of the way.
- `reject`, not `nack`: since 4.3 a requeued nack isn't a failed delivery, so it never backs off or hits the limit. A one-word bug that loops forever and starves the queue.

## Gotchas (hit while building this)

- `basic.nack` on 4.3 doesn't increment `delivery-count`: no backoff, no limit, no dead-lettering. The job is redelivered before the ones behind it, so with prefetch 1 they starve (acquired-count reached 15 in 10 s; jobs 4 and 5 never ran).
- `x-delivery-limit=3` allows 4 deliveries. The dead job carries `x-delivery-count: 4`.
- Delayed retry is quorum-queue only, linear, and set per queue. A per-message delay needs AMQP 1.0 (`x-opt-delivery-time`), which PHP can't use.
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
