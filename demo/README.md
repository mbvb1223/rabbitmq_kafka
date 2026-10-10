# Demos: RabbitMQ vs Kafka, one concept each

Each demo proves **one** claim from [../docs/03-topic-proposal.md](../docs/03-topic-proposal.md) and runs it on **both** brokers, so the comparison is built into every lesson. [99-final](99-final/README.md) collects the results and picks what goes on stage.

## Learning checklist

| # | Demo | Claim it proves | Talk point | Done |
|---|---|---|---|---|
| 01 | [queue-vs-stream](01-queue-vs-stream/README.md) | a message is a work item (queue) or a position (log) | 2 | [ ] |
| 02 | [scale-consumers](02-scale-consumers/README.md) | Kafka parallelism stops at the partition count; RabbitMQ scales to N workers | 5 | [ ] |
| 03 | [retry-dlq](03-retry-dlq/README.md) | RabbitMQ retries one job with backoff and dead-letters it; Kafka needs retry topics you build | 2 | [ ] |
| 04 | [routing](04-routing/README.md) | the consumer decides what it gets (bindings) vs the producer decides (topics) | 4 | [ ] |
| 05 | [ttl-priority](05-ttl-priority/README.md) | per-message TTL and priority exist in RabbitMQ only; Kafka retention isn't TTL | 2 | [ ] |
| 06 | [head-of-line](06-head-of-line/README.md) | one slow message blocks its partition; ordering and head-of-line blocking are the same coin | 2 | [ ] |
| 07 | [replay](07-replay/README.md) | both can replay; who stores the offset differs | Q&A | [ ] |
| 08 | [share-groups](08-share-groups/README.md) | Kafka's real queue (4.2+): what it fixes, what it doesn't, and why PHP can't use it | 2 | [ ] |
| 09 | [log-compaction](09-log-compaction/README.md) | where Kafka genuinely wins: latest value per key | 13 | [ ] |
| 10 | [durability](10-durability/README.md) | what "acknowledged" means: `acks`/`min.insync.replicas` vs quorum fsync | 3 | [ ] |
| 99 | [final](99-final/README.md) | the comparison matrix + which demos go on stage | all | [ ] |

Every demo README ends with its own `## Checklist`.

## One-time setup

```bash
docker compose up -d                  # from the repo root: RabbitMQ 4.3.6, Kafka 4.3.1, kafka-ui
cd demo
composer install                      # php-amqplib for every RabbitMQ demo (runs on the host)
docker build -t demo-kafka-php lib    # PHP 8.4 alpine + php-rdkafka; kafka/run builds it if missing
```

- UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.
- Port 5672 already taken by another RabbitMQ? Start with `RABBITMQ_PORT=5673 RABBITMQ_UI_PORT=15673 docker compose up -d` and `export RABBITMQ_PORT=5673` in every terminal.
- Demo 10 needs 3-node clusters and brings its own compose file.

## Layout

```
demo/
  composer.json, vendor/   php-amqplib, shared by every rabbitmq/ folder
  lib/rabbitmq.php         rabbit(): a channel to localhost:$RABBITMQ_PORT
  lib/kafka.php            kafka_conf(), running(), kafka_message(), partitions()
  lib/Dockerfile           the demo-kafka-php image
  bin/kafka-php            runs a PHP script in that image on the compose network (called by each kafka/run)
  bin/kafka <tool> ...     /opt/kafka/bin/<tool>.sh --bootstrap-server kafka:19092 ...
  bin/rabbit <tool> ...    rabbitmqctl, rabbitmq-queues, rabbitmq-diagnostics inside the container
  bin/reset-topic t n      delete + recreate topic t with n partitions
  NN-name/
    README.md              concept, acts, what you should see, compare, gotchas, checklist
    reset.sh               deletes and recreates everything the demo uses
    rabbitmq/*.php         php publish.php ...      (run on the host)
    kafka/run, kafka/*.php ./run publish.php ...    (runs in the container)
```

Each demo prefixes its queues, exchanges, topics and groups with its own name (`scale.`, `retry.`, ...), so demos don't collide and `reset.sh` only touches its own demo.

## Troubleshooting

- **A Kafka consumer is idle for no reason:** a killed member keeps its partitions for the 45 s session timeout. Wait, or check `bin/kafka kafka-consumer-groups --describe --group <g> --members`.
- **Records go to a consumer you can't see:** a `bin/kafka` console consumer killed without a terminal keeps running inside the kafka container. Find it with `docker compose exec kafka ps aux | grep Console` and kill it (demo 08's `reset.sh` does this).
- **A demo behaves differently on the second run:** run its `./reset.sh`. Empty KIP-848 groups survive topic deletion, so the reset scripts delete their own groups too.
- **Counts look stale:** `rabbitmqctl list_queues`, the RabbitMQ UI and `kafka-consumer-groups --describe` lag a few seconds behind. Re-run before concluding.
- **`PRECONDITION_FAILED` on `queue_declare`:** the queue exists with other arguments. `./reset.sh` deletes it.

