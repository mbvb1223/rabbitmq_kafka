# Demo 10: what "acknowledged" means (~8 min)

Both brokers keep 3 copies. The difference is **when they say "got it"**. A Kafka producer picks its own promise: `acks=1` means the leader has it, and `acks=all` means every in-sync replica has it, where `min.insync.replicas` says how few that may be. A RabbitMQ quorum queue makes one promise only: a Raft majority has written **and fsynced** it. This demo kills leaders mid-stream and counts acknowledged messages that vanished.

## You'll learn

- `acks=1` loses exactly the window the followers hadn't copied yet, and the producer is never told
- `acks=all` + `min.insync.replicas=2` loses nothing. Writes stop when 2 of 3 brokers are gone, and librdkafka hides the `NOT_ENOUGH_REPLICAS` behind a timeout
- A quorum-queue confirm always means a majority: under the same chaos, 0 lost, and there's no `acks=1` to opt into
- "Not confirmed" doesn't mean "not stored", so resending creates duplicates and consumers must be idempotent
- What a laptop can't show: page cache vs fsync when replicas lose power together

| | RabbitMQ quorum queue (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| "ack" means | a Raft majority wrote and fsynced it | `acks=1`: the leader has it. `acks=all`: every ISR member has it (in the page cache) |
| Knobs | none: a confirm is always a majority | `acks`, `min.insync.replicas` (default **1**), replication factor |
| Lagging replica | can't win an election: Raft picks the most up-to-date log | stays in the ISR for up to `replica.lag.time.max.ms` (30 s) and can become leader |

## Setup

This demo doesn't use the main stack. Its own [compose.yaml](compose.yaml) (project `rabbitmq-kafka-durable`) has two profiles. Run **one at a time**: the Kafka profile uses ~560 MB (1 controller + 3 brokers with 192 MB heaps) and RabbitMQ ~450 MB. The main stack can keep running.

```bash
docker compose --profile kafka up -d          # from demo/10-durability, ~20 s
./reset.sh kafka                              # durable.acks1 (min.insync 1), durable.acksall (min.insync 2), RF 3
# ... Kafka acts ...
docker compose --profile kafka down
docker compose --profile rabbitmq up -d       # ~30 s; UI on http://localhost:15681
./reset.sh rabbitmq                           # durable.orders, leader on rabbit-1
```

- **Helpers:** `./kafka-cli <tool>` and `./rabbit-cli <tool>` run the CLI tools inside this cluster.
- **`./kill-leader <topic|queue> [stall-s]`:** `docker kill`s the leader, i.e. SIGKILL with no clean shutdown. With `stall-s`, it first `docker pause`s both followers for that long, like a GC pause or a network blip: they fall behind but are still ISR members / Raft voters.
- **Producers:** they log every acknowledged id, and `verify.php` reads everything back and prints `LOST` = acknowledged but missing.

---

## Kafka (from this folder)

### Act 1: `acks=1`, kill the leader

```bash
kafka/run produce.php durable.acks1 1 25 20000   # terminal 1: 25 s at 20k msg/s
./kill-leader durable.acks1                      # terminal 2, after ~5 s
docker compose --profile kafka start             # after the producer is done
kafka/run verify.php durable.acks1               # LOST 0
```

- **LOST 0 in 5 of 5 runs.** On one laptop a follower is less than 1 ms behind, so the kill never lands in the gap. Acks pause for ~10 s: the controller waits `broker.session.timeout.ms` (9 s) before it gives the partition a new leader.
- Now widen the gap the way a GC pause would (run `./reset.sh kafka` first):

```bash
kafka/run produce.php durable.acks1 1 25 20000
./kill-leader durable.acks1 3                    # freeze both followers 3 s, then kill the leader
```

```
06:14:23 durable.acks1: leader kafka-3, isr 3,1,2
06:14:23 docker pause kafka-1 kafka-2  for 3 s
06:14:26 docker kill kafka-3
done: sent 380750, acked 380750, failed 0
acked 380750, present 319646 (0 duplicates), LOST 61104
```

- **The producer saw 0 failures, and 61,104 acked messages are gone** (60,968 on the second run). That's 3 s × 20k msg/s: everything the leader acked while the followers were frozen. They were still in the ISR, so one became leader without that tail, and kafka-3 truncated its log to match when it came back.

### Act 2: `acks=all` + `min.insync.replicas=2`, same chaos

```bash
kafka/run produce.php durable.acksall all 25 20000
./kill-leader durable.acksall 3
```

```
[ 5s] sent 100434, acked 100434, failed 0
[ 6s] sent 120195, acked 115067, failed 0        <- followers frozen: acks stop
[13s] sent 215067, acked 115067, failed 0
done: sent 345853, acked 345853, failed 0
acked 345853, present 345853 (0 duplicates), LOST 0
```

- Acks freeze the moment the followers do. The idempotent producer retries to the new leader with no loss and no duplicates.

### Act 3: 2 of 3 brokers down

```bash
./reset.sh kafka
kafka/run produce.php durable.acksall all 50 2000
./kafka-cli kafka-topics --describe --topic durable.acksall      # note the Leader
docker compose stop kafka-2 kafka-3                              # the two that aren't the leader
./kafka-cli kafka-topics --describe --topic durable.acksall      # Isr: 1  Elr: 2
echo order-1 | ./kafka-cli kafka-console-producer --topic durable.acksall \
  --producer-property acks=all --producer-property delivery.timeout.ms=8000 --producer-property request.timeout.ms=3000
```

- PHP: acks stop at 11,885. After `message.timeout.ms` (30 s here, **300 s by default**) every message fails with `Local: Message timed out` (88,097 failed). librdkafka retries the broker's error silently.
- The Java producer shows the real error: `Got error produce response ... Error: NOT_ENOUGH_REPLICAS`.
- With `acks=1` the same outage goes unnoticed: 29,993 sent, 29,993 acked by the leader alone.
- `Elr: 2` is KIP-966 (eligible leader replicas): the controller remembers that broker 2 has every committed record.

---

## RabbitMQ (from this folder, profile `rabbitmq`)

### Act 1: same chaos (freeze followers 3 s, kill the leader)

```bash
RABBITMQ_PORT=5681 php rabbitmq/publish.php 20 2000   # terminal 1: publisher on rabbit-1, the leader
./kill-leader durable.orders 3                        # terminal 2, after ~5 s
docker compose start rabbit-1
RABBITMQ_PORT=5682 php rabbitmq/verify.php
```

```
[ 6s] sent 12003, confirmed 12003, nacked 0, waiting 0
[ 7s] sent 14034, confirmed 13067, nacked 0, waiting 967     <- followers frozen: confirms stop
connection lost: PhpAmqpLib\Exception\AMQPConnectionClosedException
done: sent 19304, confirmed 13067, nacked 0, never confirmed 6237
confirmed 13067, in queue 13400 (0 duplicates), LOST 0
```

- The leader alone isn't a majority, so confirms stop. No setting turns that off.
- **333 messages were stored but never confirmed** (13,400 in the queue vs 13,067 confirmed). The app has to resend the 6,237 it never heard about, and that resend creates duplicates.

### Act 2: kill the leader, publisher on another node

```bash
./reset.sh rabbitmq
RABBITMQ_PORT=5683 php rabbitmq/publish.php 25 2000
docker compose kill rabbit-1                          # after ~8 s; start it again ~10 s later
RABBITMQ_PORT=5682 php rabbitmq/verify.php            # confirmed 50000, in queue 50000, LOST 0
```

- A ~1 s blip (`waiting 105`), then rabbit-2 is leader, compared with Kafka's ~10 s. Erlang distribution sees the dead node's TCP link drop at once, while the Kafka controller waits for missed heartbeats.

### Act 3: 2 of 3 nodes down

```bash
./reset.sh rabbitmq
RABBITMQ_PORT=5683 php rabbitmq/publish.php 30 1000
docker compose kill rabbit-1 rabbit-2                 # after ~5 s; start them ~13 s later
RABBITMQ_PORT=5682 php rabbitmq/verify.php            # confirmed 30000, in queue 30000, LOST 0
```

- Confirms freeze at 5,003 while `waiting` climbs to 15,016. **Nothing fails**: AMQP has no publish timeout, so your `wait_for_pending_acks` timeout is the only clock. When the majority returns, all 30,000 are confirmed.

### Optional: a classic queue lives on one node

```bash
RABBITMQ_PORT=5681 php rabbitmq/classic.php declare
docker compose kill rabbit-1
RABBITMQ_PORT=5683 php rabbitmq/classic.php publish  # confirmed 0, nacked 5; NOT_FOUND ... due to timeout
docker compose start rabbit-1                         # ~12 s later, publish again: confirmed 5, holds 5
```

---

## What this demo can't show: page cache vs fsync

`docker kill` kills a process, not a machine. The Docker VM's kernel keeps the page cache, so a "killed" Kafka broker still has everything it wrote. Talk point 3 is about **correlated power loss**: Kafka's acked data sits in the page cache, while a quorum queue has fsynced before confirming. Showing that needs replicas losing power together (e.g. a VM hard reset), which this laptop can't do. What you can show are the defaults:

```bash
./kafka-cli kafka-configs --describe --all --entity-type brokers --entity-name 1 | grep -E ' (log.flush.interval.messages|min.insync.replicas)='
#  log.flush.interval.messages=9223372036854775807   never fsync per message, the OS decides
#  min.insync.replicas=1                              acks=all can mean one broker
```

**The fair counterpoint** ([Vanlightly, 2023](https://jack-vanlightly.com/blog/2023/4/24/why-apache-kafka-doesnt-need-fsync-to-be-safe)): Kafka's replication protocol plus recovery make skipping fsync safe unless replicas fail together. KIP-966 (the `Elr` column) closes the "last replica standing" gap. Across 3 AZs with `min.insync.replicas=2`, that's a reasonable trade. So the honest slide is: Kafka swaps fsync for replication across failure domains, while RabbitMQ fsyncs **and** replicates. Configured right, neither lost a message here.

## Compare

| | Kafka `acks=1` | Kafka `acks=all`, `min.insync=2` | RabbitMQ quorum queue + confirms |
|---|---|---|---|
| Ack means | the leader has it | every ISR member (≥ 2) has it | a majority wrote + fsynced it |
| Leader killed, followers current | LOST 0 (5 of 5 runs) | LOST 0 | LOST 0 |
| Leader killed, followers 3 s behind | **LOST 61,104**, 0 errors reported | LOST 0, acks pause | LOST 0, confirms pause |
| Failover pause here | ~10 s | ~10 s | ~1 s |
| 2 of 3 down | still acked by the leader alone | refused: `NOT_ENOUGH_REPLICAS` (PHP: timeout after 30 s) | confirms wait, delivered when the majority is back |
| Default that bites | you must choose it (librdkafka defaults to `acks=all`) | `min.insync.replicas` defaults to 1 | classic queues live on one node |

## Talking points

- "Acknowledged" is a promise about **where** the data is. Kafka lets every producer choose; a quorum queue has one answer.
- `acks=1` fails silently: 0 errors reported, 61,104 messages gone.
- On both sides durability is bought with availability: with 2 of 3 down, nothing is written.
- No-loss has a flip side: resending the unconfirmed creates duplicates, so consumers must be idempotent (demo 03 / Q13).
- When Kafka people say "fsync isn't needed", agree for separate AZs, then ask what shares a rack, a PDU, or a cloud zone outage.

## Gotchas (hit while building this)

- **3 brokers at the 1 GB default heap don't fit next to the main stack.** At 192 MB, the log cleaner's 128 MB dedupe buffer OOMs on startup, hence `KAFKA_LOG_CLEANER_DEDUPE_BUFFER_SIZE=16777216`.
- **The controller is a separate node,** so stopping 2 brokers doesn't also kill the metadata quorum. With 3 combined nodes the ISR couldn't shrink, and you'd get timeouts instead of `NOT_ENOUGH_REPLICAS`. That's reasoning; only the separate-controller setup was tested. A single controller is a SPOF, which is fine for a demo.
- **Without the stall, `acks=1` lost nothing in 5 of 5 runs.** Don't promise loss on stage without `./kill-leader ... 3`.
- **librdkafka never surfaces `NOT_ENOUGH_REPLICAS`:** it retries until `message.timeout.ms`. The default is 300 s, so `produce.php` sets 30 s.
- **The Kafka CLI tools print a stack trace for every stopped broker** (its DNS name disappears). `./kafka-cli` filters them out.
- **RabbitMQ clusters only with a shared Erlang cookie.** `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-setcookie ...` (server) plus `RABBITMQ_CTL_ERL_ARGS` (CLI) worked on 4.3.6.
- **`x-queue-leader-locator: client-local`** puts the leader on the node you declare from. That's why `reset.sh` declares via rabbit-1.
- **A classic queue on a dead node** nacks publishes and times out on declare. Its messages come back with the node; the nacked ones were never stored.

## Checklist

- [ ] Kafka Act 1: a plain kill gives LOST 0; with a 3 s stall, LOST ≈ 3 s of traffic while the producer reports 0 failures
- [ ] Kafka Act 2: same chaos with `acks=all` + `min.insync.replicas=2` gives LOST 0, and acks pause instead
- [ ] Kafka Act 3: 2 of 3 down shows `NOT_ENOUGH_REPLICAS` at the broker and "Message timed out" in PHP
- [ ] RabbitMQ Acts 1-3: LOST 0 every time; confirms wait instead, and "never confirmed" ≠ "not stored"
- [ ] I can explain why `docker kill` can't show the fsync difference, and Vanlightly's counterpoint
