
# Source: rabbitmq.com/docs/compare/kafka — structured summary

> Source: <https://www.rabbitmq.com/docs/compare/kafka>
> Read on 2026-09-01. **Written by the RabbitMQ team — self-declared bias.** See `02-related-research.md` for cross-checks.

## 0. The thesis of the page

The old rule — *"Kafka for streaming, RabbitMQ for queueing"* — is dead:

| Year | Event | Effect |
|------|-------|--------|
| 2021 | RabbitMQ **3.9** ships **Streams** | RabbitMQ gets a replicated append-only log with offsets + replay |
| 2026 | Kafka **4.2** ships **Share Groups** (KIP-932) GA | Kafka gets per-message ack / queue semantics |

Both systems now do both jobs. The page argues the *remaining* difference is:
**how much the broker knows about each individual message.**

Page's closing recommendation: *start with RabbitMQ, add Kafka when you hit something only Kafka does.*

---

## 1. Data model

### RabbitMQ queue types

| Type | Storage | Reads | Best for |
|------|---------|-------|----------|
| **Quorum queue** | Raft-replicated log, **fsync before confirm** | Destructive | Work distribution needing data safety / HA |
| Classic queue | Local per-message persistence | Destructive | Transient, high-churn, single node |
| **Stream** | Replicated append-only log | **Non-destructive** | Fan-out, replay, big backlogs, high throughput |
| JMS queue | Raft log, fsync before confirm | Either | JMS apps w/ selectors, queue browsers |
| MQTT QoS 0 queue | None | Destructive | Fire-and-forget IoT fanout |

Publishers → **exchange** → **bindings** (routing rules) → queues/streams.
Routing is decided **broker-side**, *before* storage.

### Kafka

Topic → partitions (ordered, immutable log) → offsets. Key hashing decides partition **client-side**. Broker never inspects the payload. Consumer group parallelism ≤ partition count (share groups lift that).

### Mapping table

| Kafka | RabbitMQ | Nuance |
|-------|----------|--------|
| Topic | Super stream | RabbitMQ's grouping is of independent, individually-named streams |
| Partition | Stream | Stream is a first-class named object; a partition is not |
| Offset | Offset | Both track offsets broker-side |
| Record batch | Chunk | Same amortisation trick |
| Consumer group | Single active consumer per stream | Same partition-parallel ordered consumption |

---

## 2. Where they are the SAME (this is the surprising part)

Both use the same streaming playbook:

1. **Batching is the unit of work** — Kafka message sets ≈ RabbitMQ chunks; both support sub-entry batch compression.
2. **One binary format end-to-end** — no re-encoding producer→broker→consumer. (RabbitMQ streams use AMQP 1.0 format.)
3. **Zero-copy reads** — both use `sendfile()`; both lose it under TLS.
4. **Page cache, not heap** — neither holds the log in process memory.
5. **Publisher-side dedup** — Kafka: producer ID + sequence number. RabbitMQ: named producers + publishing IDs.
6. **Raft for metadata** — Kafka dropped ZooKeeper for **KRaft**; RabbitMQ dropped Mnesia for **Khepri**.

> Takeaway for the talk: *throughput and log design are not the differentiator. They converged.*

---

## 3. Where they genuinely DIVERGE — the queueing model

> **"In Kafka, a message is a position in a shared log. In RabbitMQ, a message is an independent item of work."**

RabbitMQ parses the message **envelope** (headers, properties, annotations) → enables broker-side routing, filtering, TTL, priority, scheduled delivery.

Kafka: a message's fate is coupled to its neighbours; its position is fixed.

### Quorum queues = Raft-replicated *delivery state*

The replicated state machine covers not just the message, but the operations:

- enqueue
- grant consumer credit / assign message
- ack + delete
- reject + dead-letter
- failed processing → increment delivery count, requeue
- hold message until time T

Each is replicated to a majority **and flushed to disk** before taking effect.
→ *Delivery state is as durable as the message itself.*

On leader failure, the new leader knows exactly which messages were held by which consumer, attempt counts, parked/scheduled messages.

**Kafka contrast:** delivery state and counter are durable, but *acquisition* (who holds it right now) is leader **memory** state. Leader change → in-flight messages flip back to `Available` → **burst of redeliveries**. Fine for at-least-once, painful for expensive work.

### The RabbitMQ per-message toolkit (no Kafka equivalent)

| Feature | What it does |
|---------|--------------|
| Message delays | Publisher-set: confirmed immediately, hidden from consumers until time T |
| Delayed retry | Backoff derived from delivery count; consumer can override per-message (e.g. honour an HTTP `Retry-After`) |
| Message deferral | Consumer parks a message under a token, asks for *that exact message* back later |
| Per-message TTL | Every message can expire on its own schedule |
| Priorities | 32 strict levels — urgent work overtakes the backlog |
| Dead-letter routing | Rejected messages re-enter *exchanges* → failure paths have the same routing power as success paths |
| Modified outcome | Consumer annotates the message on return (why it failed, when, which consumer); next consumer sees it; headers exchange can route on it |
| Poison message handling | Repeated failures get dead-lettered instead of looping forever |
| Consumer timeouts | Stuck consumer's messages go back to healthy ones |
| Single active consumer / consumer priorities | Hot-standby patterns |
| Message interceptors | Broker-side custom logic per message |
| **Backlog convergence** | Acked = deleted = **disk reclaimed**. Queue trends to empty. |

Kafka: acking does **not** delete. Disk is sized by the **retention window**, not by the backlog.

### Kafka share groups — what they do and don't fix

**Do provide:** many consumers per partition, per-message ack, durable delivery counting w/ limits, 30s acquisition lock auto-released on consumer death, explicit `release` / `reject` / `renew`.

**Do NOT provide:**

- Ack ≠ delete (retention still governs disk)
- No per-message TTL / priorities / delays / deferral / annotated returns
- No dead-letter **routing** — rejecting archives per share group; reprocessing is DIY
- **Head-of-line blocking reduced, not eliminated** — in-flight window = share-partition start offset → last fetched offset, capped at **2000 offsets** by default. The start offset only advances when *every* earlier message is terminal. One slow/failing message pins it; once the span hits the cap, **that partition stops being fetched**.
- Always reads from the **partition leader** (no rack awareness / KIP-392 follower fetch). RabbitMQ quorum queues serve from whichever replica the consumer is connected to — locality for free across AZs/DCs.
- No server-side filtering — consumers fetch everything in their partitions and throw away what they don't want.
- Records stay opaque to the broker — no interceptors, selectors, content routing.

---

## 4. Durability — the sharpest technical point on the page

> Kleppmann, quoted: perfect durability doesn't exist; there are only risk-reduction techniques.

### Kafka default: replication only

Kafka's own docs: durability *"does not require syncing data to disk"*; the recommended default **disables application fsync entirely**.

So with `acks=all`, the message is in the **page cache** of every in-sync replica — not necessarily on physical disk. OS writeback can lag by seconds.

**Risk:** correlated failure (whole DC / shared power / same rack) → acknowledged messages are lost. Racks & AZs mitigate this in cloud; a single on-prem facility does not.

### RabbitMQ quorum queues: replication **and** fsync

Publisher confirm only after a Raft **majority has written and flushed** to disk. Pull the power on the whole DC → confirmed messages survive.

### DR comparison

| | Replicated | fsync before confirm | Survives site-wide power loss | Cross-site standby |
|---|---|---|---|---|
| Kafka topic (stream or queue mode) | Yes | No (discouraged) | **No** | MirrorMaker 2 |
| RabbitMQ **stream** | Yes | No (same trade as Kafka) | **No** | Warm Standby Replication |
| RabbitMQ **quorum queue** | Yes | **Yes** | **Yes** | Warm Standby Replication |

### Why fsync costs Kafka more (the architectural reason)

- **Kafka's log is per partition.** The flush check sits inside each partition's append lock; each fsync syncs the segment *plus* offset/time/transaction indexes. Nothing coordinates flushes across partitions. Per-message fsync = a separate flush per partition. Kafka's own docs warn `flush.messages` causes inefficient disk patterns and latency.
- **RabbitMQ's Ra is multi-Raft.** One node runs thousands of independent Raft clusters (one per quorum queue) that **share a single write-ahead log**. The WAL batches ops from many queues → one write → **one fsync** → notify every queue. fsync rate tracks batching cadence, not queue count.

> Claim: RabbitMQ gets *many queues + data safety + throughput* simultaneously. Kafka's design forces you to pick two — *"and the one you give up is usually data safety."*

---

## 5. Broker-side routing & filtering

**RabbitMQ:** exchanges (direct, topic, fanout, headers, local random, consistent-hash — plus custom, pluggable), bindings, and exchange-to-exchange bindings for composition. Consumers declare interest at runtime.

**Streams filtering:** Bloom filters (skip whole chunks on disk without decoding) + AMQP filter expressions (SQL-like on message properties).

**Kafka:** routing is producer-side; consumers subscribe to whole topics.

### The worked example: "give me everything about orders" (`orders.#`)

**RabbitMQ:** one binding on a topic exchange, declared at runtime. Publishers don't know or care. No existing consumer is affected.

**Kafka — three workarounds, all with a bill:**

| # | Workaround | Cost |
|---|-----------|------|
| 1 | Many topics + regex subscribe `Pattern.compile("orders\\..*")` | Producer fixes the taxonomy at write time; every combination = a topic w/ partitions, files, metadata, replication. **Regex subscribe doesn't work with `KafkaShareConsumer`** — queue mode takes explicit topic lists only. |
| 2 | One topic, filter in the consumer | Every consumer fetches + deserialises everything, discards most. Full network + CPU cost for unwanted data. |
| 3 | Stream-processing job writing a derived topic | Another app to deploy/monitor, a second copy on disk, extra latency. |

> **Underlying difference: who decides.** Kafka → the producer, at write time. RabbitMQ → the consumer, at any time; the broker does the matching.

---

## 6. Protocols

| Protocol | RabbitMQ | Kafka |
|----------|----------|-------|
| AMQP 1.0 (ISO/IEC + OASIS standard) | ✅ | ❌ |
| AMQP 0-9-1 | ✅ | ❌ |
| MQTT 3.1 / 3.1.1 / 5.0 | ✅ | ❌ |
| STOMP | ✅ | ❌ |
| RabbitMQ Stream protocol | ✅ | ❌ |
| Kafka protocol | ❌ | ✅ |
| WebSocket | AMQP 1.0 / MQTT / STOMP over WS | community proxies |
| JMS | VMware Tanzu RabbitMQ over AMQP 1.0 | ❌ |
| HTTP | Management HTTP API | Admin API; REST proxies are vendor products |

Example the page gives: publish over **MQTT** from a device → consume over **AMQP 1.0** in a backend service → re-read from the **stream protocol** in an analytics job. Broker converts transparently.

Kafka equivalent = deploy an MQTT broker + a WebSocket proxy + a JMS bridge, each to secure and monitor.

---

## 7. Security

| | RabbitMQ | Kafka |
|---|---|---|
| Transport | TLS on every protocol + inter-node | TLS on client + inter-broker listeners |
| AuthN | SASL (user/pass, mTLS, x.509, LDAP, OAuth2/OIDC), pluggable | SASL (Kerberos, PLAIN, SCRAM, OAUTHBEARER), mTLS |
| AuthZ | Per-vhost + per-user perms w/ regex, **topic authorisation**, ACLs | ACLs per resource, prefixed ACLs |
| Tenant isolation | **Virtual hosts** — real namespaces | Topic naming convention + prefixed ACLs + quotas |
| Resource protection | Per-vhost/per-user connection & queue limits | Client/broker quotas, request throttling |
| Credentials | Mgmt UI, HTTP API, CLI, definitions export/import | CLI, Admin API |

vhosts are a **boundary**; Kafka's prefixed topic naming is a **convention**.
Tanzu adds FIPS 140-2, audit logging (who deleted a queue), continuous CVE scanning.

---

## 8. Performance — the myth-buster section

> *"RabbitMQ is slow" is the most common misconception in the marketplace.*

Numbers quoted by the page:

| Setup | Throughput |
|---|---|
| Single **quorum queue** (replicating **and fsyncing every message**) | ~80,000 msg/s |
| Single **classic queue** | ~100,000 msg/s |
| Single **stream** | several million msg/s |
| Single stream + sub-entry batching | > 4,000,000 msg/s |
| Five streams | > 17,000,000 msg/s |

Throughput in **both** systems is governed by chunk fullness. One-message-at-a-time is slow in RabbitMQ for the same reason it's slow in Kafka.

> Warning worth repeating on stage: *a benchmark that tunes one side hard and leaves the other on defaults is comparing tuning effort, not the systems.*

---

## 9. Operations

- **Management UI in the box** — web UI + HTTP API + `rabbitmqadmin`; inspect queues, trace messages, change policies, export/import topology. Kafka ships CLI tools; web UIs are third-party or commercial.
- **Monitoring** — RabbitMQ serves `/metrics` for Prometheus natively, with maintained Grafana dashboards. Kafka reports via **JMX** → you run a JMX exporter agent *inside each broker JVM* (or enable+secure remote JMX) **and** maintain a regex mapping file. Kafka 4.2 renamed metrics to a `kafka.COMPONENT` convention → existing rules/dashboards/alerts needed revisiting.
- **Windows** — Kafka docs: *not currently a well supported platform*. RabbitMQ treats Windows as first-class. Matters for finance/manufacturing/healthcare/public-sector on-prem estates.
- **Kubernetes** — RabbitMQ ships official Operators maintained by the team; Kafka's Strimzi is an excellent CNCF project but is not part of Apache Kafka.

### The runtime argument (Erlang/BEAM vs JVM)

Kafka = JVM: great JIT, decades of optimisation, world-class profiling, biggest talent pool.
RabbitMQ = Erlang/BEAM: built at Ericsson for switches that aren't allowed to stop.

Properties RabbitMQ *inherits* rather than builds:

- **Fault isolation** — each connection / session / queue is its own process with its own heap, no shared memory, message passing only. Crash → supervisor restarts in microseconds; everything else continues. A malformed frame from a bad client is a non-event.
- **Scalability** — processes are cheap; a node hosts millions. One process per connection/queue is the natural unit, not a compromise.
- **Parallelism without locks** — parsing, routing, dispatching, writing all run in parallel with no app-level locking.
- **Per-process GC** — pre-emptive scheduler; a major GC pauses **one process**, not the broker. Tail latency doesn't fall off a cliff as memory grows; GC tuning isn't a prerequisite for production.

Kafka's own docs admit object overhead often doubles data size and *"Java garbage collection becomes increasingly fiddly and slow as the in-heap data increases"* — which is exactly why Kafka keeps data off-heap in page cache. Sound engineering, but a workaround for a problem BEAM doesn't have.

---

## 10. Scorecard

### Only RabbitMQ

- Queue semantics designed as queue semantics (DLX routing, TTL, 32 priorities, delayed retry w/ per-message override, deferral by token, publisher delays, consumer-annotated returns, consumer priorities, SAC) — and a backlog that converges to empty, returning disk
- Replication **and** fsync by default on quorum queues
- Thousands of independent queues per cluster with amortised fsyncs
- Per-message delivery state replicated through Raft
- Broker-side routing (exchanges, bindings, e2e composition) changeable at runtime without touching producers
- Broker-side filtering (Bloom filters + SQL expressions on streams; JMS selectors on queues)
- Five protocols in one broker + WebSocket transports
- Connectable through a plain load balancer (no need to address every node)
- Local delivery from **any** replica (leader or follower, zero config)
- Virtual hosts as real tenant boundaries; topic authorisation
- Native Prometheus + built-in management UI, no agents
- First-class Windows
- Runtime built for always-on systems
- MQTT at scale (millions of device connections)
- First-class JMS (Tanzu) — migration path off legacy brokers

### Only Kafka (the page calls these "real and substantial")

| Feature | Detail |
|---|---|
| **Tiered storage** (KIP-405) | Offload old segments to object storage → retention decoupled from broker disk. *Nuance: Apache Kafka gives the framework but no out-of-the-box `RemoteStorageManager`; the backend comes from a vendor.* RabbitMQ streams are bounded by local disk. |
| **Log compaction** | Keep the last value per key forever → topic as durable changelog; backbone of CDC. RabbitMQ streams retain by size/age only. |
| **Stream-processing ecosystem** | Flink, Spark, Iceberg, ksqlDB, and the long tail of connectors default to Kafka. Kafka Connect is part of Apache Kafka (though production connectors + Schema Registry are vendor products). |
| **Kafka Streams** | Java library — joins, windows, aggregations, state stores, exactly-once — inside your app, no extra cluster. |

---

## 11. Use-case table (verbatim structure from the page)

| Use case | Pick | Why |
|---|---|---|
| Task/job queues, background workers | **RabbitMQ** | Exclusive assignment, retry w/ backoff, DLQ, priorities. Kafka has no native "retry this job in 90 seconds". |
| Priorities, SLAs, expiry, scheduled delivery | **RabbitMQ** | 32 priorities, per-message TTL, delayed delivery, deferral. Kafka: none. |
| Request/reply (RPC) | **RabbitMQ** | Direct reply-to, exclusive queues, per-message correlation. |
| Microservice event notifications | **RabbitMQ** | New subscriber = a new binding. Kafka: new topics or client-side filtering. |
| Orders, payments, cannot-lose work | **RabbitMQ** | Quorum queues fsync before confirming. |
| IoT / device telemetry | **RabbitMQ** | Native MQTT on the same cluster as the backend queues. |
| Per-tenant / per-device / per-workflow isolation | **RabbitMQ** | Thousands of cheap queues vs a partition per boundary. |
| Migrating off legacy JMS | **RabbitMQ** | JMS + broker-side selectors + queue browsers (Tanzu). |
| Windows Server deployment | **RabbitMQ** | First-class platform. |
| Streaming tens of thousands of logical subjects | **RabbitMQ** | Bloom filters let one stream serve many narrow subscriptions. |
| Event streaming w/ replay, multiple readers | **Either** | Same job, same techniques. |
| Exactly-once read-process-write | **Either** | Kafka: transactional producer. RabbitMQ: source offset as publishing ID. Neither covers external side effects. |
| Activity tracking, metrics firehose, log aggregation | **Either** | Both do millions/sec — pick on ecosystem/ops. |
| Stateful stream processing (joins, windows) | **Either** | Kafka's ecosystem is broader. |
| Months/years of history, cheaply | **Kafka** | Tiered storage. |
| Event sourcing w/ keyed changelog | **Kafka** | Log compaction. |

> *"If your list of ticks is entirely in the RabbitMQ or Either column, you do not need Kafka, and running it anyway is a second distributed system to staff, secure and upgrade for no return."*

---

## 12. Myths the page retires

| Myth | Verdict |
|---|---|
| "Kafka for streaming, RabbitMQ for queueing" | True in 2019. Dead since streams (3.9) and share groups (4.2). |
| "RabbitMQ can't do high throughput" | A single stream does several million msg/s. Folklore predates streams. |
| "RabbitMQ deletes on consume, so no replay" | True for queues, false for streams. |
| "RabbitMQ needs lots of RAM" | Classic and quorum queues don't hold bodies in memory; streams use page cache like Kafka. |
| "Queues for Kafka means you don't need RabbitMQ" | Share groups are genuinely useful, but add none of: TTL, priorities, delays, deferral, annotated returns, routable failure paths, broker routing/filtering, interceptors, disk reclamation — and HOL blocking is reduced, not eliminated. |
| "Only Kafka does exactly-once" | Both do it; RabbitMQ calls it **"effectively-once"** — the more honest name. |
| "Erlang is a liability" | You interact via client libs (Java/.NET/Python/Go), UI, CLI, Prometheus — never the broker's source language. |
| "RabbitMQ replication is bolted on" | That was **mirrored queues**, removed in 4.0. Quorum queues (GA 2019) are Raft-native, continuously Jepsen-tested. |
