# Demo 04: who decides what a consumer gets (~6 min)

In RabbitMQ the **consumer** decides, along the dimensions the producer put in the routing key: it declares its own queue and binds it with a pattern, at runtime, and the broker filters. In Kafka the **producer's topic layout** decides: the broker never routes on message content, so a consumer either reads a whole topic and filters in PHP, or someone adds topics.

Publisher -> **exchange** (a router, stores nothing) -> **bindings** (queue + pattern) -> queues. Each message carries a **routing key**, a dot-separated string like `orders.paid.vn`; each part is a *word*. A **topic exchange** copies the message into every queue whose binding pattern matches the key. Not to be confused with a Kafka **topic**, which is a stored log.

## You'll learn

- Topic exchange wildcards: `*` is exactly one word, `#` is zero or more
- A binding is declared by the consumer at runtime; the publisher and the other teams don't change
- `mandatory` + publisher confirms: the broker tells you when a routing key reaches no queue
- Filtering a shared Kafka topic costs every consumer the full read; topic-per-slice moves the decision to the producer
- KIP-848 regex subscription works from PHP and picks up a new topic in ~6-13 s

| | RabbitMQ (`rabbitmq/`) | Kafka (`kafka/`) |
|---|---|---|
| Who filters | the broker, by binding (`orders.*.vn`) | the consumer in PHP, or the producer by choosing the topic |
| A new slice | the consumer adds a binding | a client-side filter, or a new topic and a producer change |
| Same 8 orders | [orders.php](orders.php): keys `orders.<event>.<country>` | event + country in headers (Act 1) or in the topic name (Act 2); the Kafka message key is just `order-N` |

## Setup

One-time setup is in [../README.md](../README.md). Then, before each run:

```bash
./reset.sh      # deletes routing.* queues and groups, recreates routing.orders + 6 slice topics (1 partition each)
```

UIs: RabbitMQ <http://localhost:15672> (`app` / `app`), Kafka <http://localhost:8081>.

---

## RabbitMQ (`cd rabbitmq`)

### Act 1: each team binds its own slice

```bash
php consume.php vn-team 'orders.*.vn'      # terminal 1
php consume.php finance 'orders.paid.*'    # terminal 2
php consume.php audit 'orders.#'           # terminal 3
php publish.php                            # terminal 4: the 8 orders
```

- `vn-team` gets orders 1, 3, 6, 7, 8. `finance` gets 3, 4, 7. `audit` gets all 8.
- Order 3 (`orders.paid.vn`) lands in **three** queues (vn-team, finance, audit): the publisher sent it once and the broker copied it. 8 publishes, 16 deliveries.
- The UI's `routing.orders` exchange page (or `../../bin/rabbit rabbitmqctl list_bindings source_name destination_name routing_key`) shows the three bindings. They're broker state: Ctrl-C `audit`, publish again, and `routing.audit` holds 8 messages until it comes back. Start it again (`php consume.php audit 'orders.#'`): it prints the 8 it missed.

### Act 2: a new team appears, another changes its mind

```bash
php consume.php refunds 'orders.refunded.#'   # terminal 5
php publish.php
php unbind.php finance 'orders.paid.*'
php publish.php
```

- `refunds` gets only 5 and 8 from **this** publish. A new queue sees messages published after it's bound; there's no history.
- After the unbind, `finance` is still running but gets nothing; the other teams are unaffected.
- The publisher and the other teams weren't touched or restarted.

### Act 3: the broker tells you when nothing matches

```bash
php publish.php orders.cancelled.th    # an event type nobody planned for
php publish.php order.cancelled.th     # a typo
```

- `orders.cancelled.th` goes to `audit` thanks to `#`: a new event type reaches the catch-all with no change anywhere.
- The typo prints `returned: order.cancelled.th  (312 NO_ROUTE)` and then `confirmed: order.cancelled.th`. A publisher confirm is the broker's ack to the publisher, and it says ack either way, so `mandatory` is the only thing that tells you.

---

## Kafka (`cd kafka`)

### Act 1: one topic, filter in PHP

```bash
./run filter.php vn-team 'orders.*.vn'     # terminal 1
./run filter.php finance 'orders.paid.*'   # terminal 2
./run publish.php                          # terminal 3: 8 orders -> routing.orders, event + country in headers
```

- Each team is its own group (`routing.filter.<team>`), so each reads all 8: `vn-team` prints `read 8, kept 5`, `finance` prints `read 8, kept 3`, with a `skip` line for every order fetched for nothing. At 8 orders that's free. At 50 K msg/s and 20 teams, the broker serves 20x the traffic so each team can throw most of it away.
- Start `./run filter.php refunds 'orders.refunded.#'` **after** the publish: `read 8, kept 2`. A new group with `auto.offset.reset=earliest` reads the history because every slice shares one topic. A RabbitMQ queue or stream bound now would start empty.

### Act 2: topic per slice + regex subscription

A subscription starting with `^` is a regex. With `group.protocol=consumer` (KIP-848) the broker resolves it, not the client.

```bash
./run regex.php vn-team '^routing\.orders\..*\.vn$'   # terminal 1
./run publish.php split                               # terminal 2: order 1 -> routing.orders.created.vn, ...
../../bin/kafka kafka-topics --create --topic routing.orders.cancelled.vn --partitions 1 --replication-factor 1   # prints a WARNING about '.' and '_': harmless, see Gotchas
./run publish.php orders.cancelled.vn
./run publish.php order.cancelled.th                  # a typo
```

```
[vn-team] +1s topics: none
[vn-team] +5s topics: routing.orders.created.vn, routing.orders.paid.vn, routing.orders.refunded.vn
[vn-team] order 8  routing.orders.refunded.vn
[vn-team] order 3  routing.orders.paid.vn
[vn-team] order 7  routing.orders.paid.vn
[vn-team] order 1  routing.orders.created.vn
[vn-team] order 6  routing.orders.created.vn
[vn-team] +20s topics: routing.orders.cancelled.vn, routing.orders.created.vn, ...
[vn-team] order 9  routing.orders.cancelled.vn
```

- Only the 3 `.vn` topics are assigned and nothing is filtered in PHP, but the producer had to know the slices in advance: 6 topics for 3 events x 2 countries, each with its own partitions.
- Orders come grouped by topic in whatever order the fetches return (8, 3, 7, 1, 6 above; another run gave 3, 7, 1, 6, 8). 3 always comes before 7 and 1 before 6 because they share a partition; there is no order across topics.
- The new topic was picked up ~6-13 s after creation. Order 9 still arrives because the group starts new partitions from `earliest`.
- The typo prints `failed: Broker: Unknown topic or partition` after 3 s. Kafka can tell you a topic doesn't exist, but never that nobody reads it: an existing topic nobody reads just keeps the data until retention. RabbitMQ's NO_ROUTE means no queue wants this key.

---

## Compare

| | RabbitMQ | Kafka |
|---|---|---|
| Who decides | the consumer (binding), within the producer's key format | the producer (topic layout) |
| Add a team with a new slice | one `queue_bind`, nothing else changes | filter client-side, or a new topic + producer change, or a Kafka Streams/Flink job |
| Cost of filtering | broker matches the key, sends only matches | every group reads the whole topic |
| One message, many teams | copied into each matching queue | one copy; each group keeps a cursor |
| Late subscriber | a new binding gets only what's routed after it exists (queue or stream); for history, read a stream bound earlier (demo 01) | can read history (retention permitting) |
| Nothing matches | `basic.return` 312 NO_ROUTE with `mandatory`, or an alternate exchange catches it on the broker | missing topic: `Unknown topic or partition` (after 30 s by default); unread topic: stored, no signal |

## Talking points

- **Smart broker:** the routing key is metadata the broker reads. Kafka never routes or filters on a message's key, headers or payload, so headers can't route.
- **Kafka's three answers** for the next team: a client-side filter (cost = teams x volume), topic sprawl (the producer knows every slice, and topics x partitions add up), or a Kafka Streams / Flink job writing a derived topic (another service to run). Only the first two are built here.
- **Regex subscription doesn't work with the share consumer** (KIP-932), so Kafka's queue can't use Act 2's pattern.
- **One broker, many protocols:** with `rabbitmq-plugins enable rabbitmq_mqtt`, an MQTT device publishing `orders/paid/vn` arrives on `amq.topic` (RabbitMQ's built-in topic exchange) as `orders.paid.vn`, and a queue bound there with `orders.*.vn` gets it (slide only, not built here). The same in Kafka means an MQTT broker plus a connector.

## Gotchas (hit while building this)

- `orders.#` also matches `orders.cancelled.th`, so the first "unroutable" test wasn't. A catch-all absorbs new event types; only a typo in the prefix gets returned.
- `*` is exactly one word: a queue bound with `orders.*` got 0 of the 8 orders.
- Adding a word to the key (`orders.paid.vn.web`) silently breaks `orders.*.vn`; bind `orders.*.vn.#` if the key may grow. Filtering on a field the producer never published needs a producer change on both brokers.
- librdkafka waits `topic.metadata.propagation.max.ms` (default 30 s) before failing a produce to a missing topic with `Unknown topic or partition`. If `message.timeout.ms` is shorter, you get `Message timed out` instead and lose the real cause. `publish.php` sets 3 s.
- New topics matching a regex aren't picked up instantly: creating a topic makes the broker re-resolve the regex on the group's next heartbeat (5 s), at most once per 10 s, and the new assignment arrives a heartbeat later (6-13 s here).
- With `group.protocol=consumer` the broker matches the regex against the **whole** topic name: `^routing\.orders` gets only the topic `routing.orders`, not the slices. Use `^routing\.orders\..*`.
- `kafka-topics --create` warns that topic names with `.` can collide with `_` in metric names. Harmless here; pick one separator per cluster.
- `rabbitmqctl list_bindings` also prints every queue's default-exchange binding (empty source). The UI exchange page is easier on stage.

## Checklist

- [ ] RabbitMQ Act 1: each team got exactly its slice, and order 3 reached three queues
- [ ] RabbitMQ Act 2: added `refunds` and unbound `finance` without touching the publisher; `refunds` got no history
- [ ] RabbitMQ Act 3: a typo came back as 312 NO_ROUTE; a new event type reached `audit`
- [ ] Kafka Act 1: `read 8, kept 5` / `kept 3`; a late group read the history
- [ ] Kafka Act 2: the regex consumer picked up a new topic within ~15 s; the typo failed with Unknown topic or partition
- [ ] I can explain who decides: the consumer (binding) vs the producer (topic layout)
