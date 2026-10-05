# Demo: queue vs stream (~4 min)

Same broker, same PHP client (php-amqplib), one difference: `x-queue-type` is `quorum` or `stream`.
Queue = a message is a **work item** (ack deletes it). Stream = a message is a **position** in a log (ack deletes nothing).

## Setup

```bash
docker compose up -d rabbitmq          # from repo root
cd demo/queue-vs-stream && composer install
```

Reset between runs:

```bash
docker compose exec rabbitmq rabbitmqctl delete_queue demo.queue
docker compose exec rabbitmq rabbitmqctl delete_queue demo.stream
```

Keep the management UI open on the Queues tab: <http://localhost:15672> (`app` / `app`).

## Act 1: queue (consumers compete)

```bash
php consume.php queue A        # terminal 1
php consume.php queue B        # terminal 2
php publish.php queue 6        # terminal 3
```

Expected:
- A and B **split** the 6 events (e.g. A gets 1, 4, 5 and B gets 2, 3, 6). Each event is processed once.
- The UI shows `demo.queue` at **0** messages, so the acked events are gone and their disk space is freed.
- `php consume.php queue C` started late gets **nothing**. A queue can't replay.

## Act 2: stream (every consumer reads everything)

```bash
php consume.php stream A       # terminal 1
php consume.php stream B       # terminal 2
php publish.php stream 6       # terminal 3
```

Expected:
- A **and** B each get all 6 events, offsets 0-5.
- The UI shows `demo.stream` still at **6** messages. The ack only gave the consumer credit for the next message. Retention decides when data is deleted.
- Late consumers choose where to start:

| Command | Reads |
|---|---|
| `php consume.php stream C first` | everything (replay) |
| `php consume.php stream C 3` | from offset 3 |
| `php consume.php stream C 5m` | the last 5 minutes (chunk-granular, so it may start a little earlier) |
| `php consume.php stream C` | only new events (`next` is the default) |

## Talking points

- **Queue:** the broker tracks every message, so you get per-message retry, DLQ, TTL and priority, and you can add as many workers as you want.
- **Stream:** the consumer tracks its own position, so you get replay and fan-out, but no competing consumers. This is also how a Kafka topic works.
- **Kafka mapping:** a Kafka topic behaves like Act 2. Act 1 needs Kafka share groups, which have no PHP client yet.

## Gotchas (all hit while building this)

- RabbitMQ 4.3 refuses php-amqplib's default `queue_declare` (`durable=false`). Both declares in `bootstrap.php` are durable.
- A stream consumer over AMQP 0-9-1 must set `basic_qos` and use manual ack, otherwise the broker refuses the consumer.
- `x-stream-offset`: a PHP **int** is read as an offset and a **string** as `first` / `last` / `next` / an interval. Passing `"3"` instead of `3` fails.
- `rabbitmqctl list_queues` message counts lag by about 5 s, so use the UI on stage.
