# RabbitMQ vs Kafka: real-world case studies, migrations and war stories

As of 2026-10-04. There are 20 cases. Each source was fetched where possible. Flags used below:
- **(dated)**: published before 2023, included because it is still widely cited.
- **(partial)**: the original page blocked fetching (403 or Medium), so the facts come from a secondary source.
- **[PHP]**: the company runs PHP or Hack in the path being described.

## TL;DR: patterns across companies

- **Task queues that moved from RabbitMQ to Kafka all had to rebuild per-message acks.** DoorDash added local in-worker queues, Uber built uForwarder (out-of-order commit plus a DLQ), and Sentry built taskbroker (in-flight tasks kept in SQLite "to avoid head-of-line blocking"). Kafka 4.2 share groups (GA Feb 2026) now provide this out of the box, but no company had published a share-groups production story by 2026-10.
- **Teams pick the broker their people already know.** DoorDash chose Kafka partly because it had "no in-house Celery or RabbitMQ experts" and did have Kafka expertise. Detectify gave up on being "accidental RabbitMQ experts" and moved to managed RabbitMQ (Amazon MQ) rather than to Kafka.
- **Most outages came from how clients were used or configured, not from broker bugs.** PagerDuty created a new producer on every `send()`. At CircleCI, an ack timeout arrived in a patch upgrade. Inngest had time-only retention and filled a disk. Klarna's "soft" Kafka dependency turned out to be hard.
- **Mature shops run both: Kafka keeps the backlog, RabbitMQ routes and delivers.** Zalando's communication platform reads from Nakadi (its Kafka layer) and feeds RabbitMQ, deliberately so RabbitMQ never becomes the storage layer. Slack put Kafka in front of its Redis job queue as a durable buffer.
- **Kafka to RabbitMQ is the rarer direction.** The teams that took it cited ops cost and simpler integration at modest scale: Simplebet, and 84codes, which shut down its hosted Kafka service (CloudKarafka) in favour of RabbitMQ Streams and LavinMQ.
- **At very large scale, the pain is partitions.** Shopify had to add partitions before Black Friday. Uber found that partition count caps consumer parallelism. LinkedIn is replacing Kafka with Northguard.
- **[PHP] Big PHP/Hack shops do not let PHP hold a Kafka connection.** Slack's PHP app goes through Kafkagate (HTTP to Kafka). At Wikimedia, MediaWiki sends events to EventGate over HTTP, and ChangeProp POSTs each job back to PHP. With RabbitMQ, PHP's pain is long-running consumers: heartbeats and memory (Mollie, Facile.it).

## Master table

| # | Company | Date | Direction | Scale | Main reason | Link |
|---|---|---|---|---|---|---|
| 1 | Sentry | 2025-09-17 | RabbitMQ+Celery → Kafka (custom taskbroker) | All async tasks of Sentry and self-hosted | Fewer moving parts; consolidate on the Kafka they already run | [release](https://github.com/getsentry/self-hosted/releases/tag/25.9.0) |
| 2 | PagerDuty | 2025-09-04 | Kafka incident | 4.2M extra producers/h; ~95% of API events rejected at peak | Producer created on every `send()` → broker heap exhausted | [postmortem](https://www.pagerduty.com/eng/august-28-kafka-outages-what-happened-and-how-were-improving/) |
| 3 | Detectify | 2025-10-23 | Self-hosted RabbitMQ → Amazon MQ (stayed on RabbitMQ) | 50+ services; payments, notifications | Low bus factor; "accidental RabbitMQ experts" | [blog](https://blog.detectify.com/best-practices/migrating-critical-messaging-from-self-hosted-rabbitmq-to-amazon-mq/) |
| 4 | Uber | 2021-08-31 / 2026-02-05 | Kafka used as a queue → built consumer proxy (uForwarder) | 1,000+ consumer services; trillions of msgs/day | Partition-bound parallelism, head-of-line blocking, poison pills | [2021](https://www.uber.com/en-IN/blog/kafka-async-queuing-with-consumer-proxy/), [2026](https://www.uber.com/us/en/blog/introducing-ufowarder/) |
| 5 | Zalando | 2024-04-23 | Both (Nakadi/Kafka + RabbitMQ) | 1,000+ event types trigger customer comms | Kafka holds the backlog; RabbitMQ routes; priority-aware load shedding | [blog](https://engineering.zalando.com/posts/2024/04/enhancing-distributed-system-load-shedding-with-tcp-congestion-control-algorithm.html) |
| 6 | Zendesk | 2024-10 (RabbitMQ Summit) | Chose RabbitMQ | High-volume, irregular traffic | Overflow queues absorb spikes | [video](https://www.youtube.com/watch?v=k0_ESUdc7NU) |
| 7 | Shopify | 2025-11-20 | Kafka (scaling) | 146M req/min in BFCM tests | ETL pipelines needed more partitions to keep data fresh | [blog](https://shopify.engineering/bfcm-readiness-2025) |
| 8 | LinkedIn | 2025-06-25 | Kafka → Northguard + Xinfra (in-house) | Tens of PB/day; thousands of topics migrated | Kafka got too hard to scale and operate | [coverage](https://siliconangle.com/2025/06/25/linkedin-introduces-northguard-xinfra-replace-kafka-scalable-log-storage/) |
| 9 | 84codes (CloudKarafka) | 2024-02 → 2025-01-27 EOL | Vendor dropped hosted Kafka for RabbitMQ Streams/LavinMQ | One of the first hosted-Kafka services (2013) | "CloudAMQP is the better option for many use cases" | [EOL](https://www.cloudkarafka.com/blog/end-of-life-announcement.html) |
| 10 | Inngest | 2025-10-24 | Kafka incident | Trace-ingest cluster | Full disk + producer retries delayed function execution | [report](https://www.inngest.com/blog/2025-10-24-october-incident-report) |
| 11 | Etsy [PHP] | 2023-01-31 | Kafka (re-architecture) | Kafka on GKE across 3 zones | Single-AZ cluster was a risk (stale search results) | [blog](https://www.etsy.com/codeascraft/adding-zonal-resiliency-to-etsys-kafka-cluster-part-1) |
| 12 | Exoticca [PHP] | 2025-09-04 (v0.2.0) | Chose Kafka with Symfony Messenger | n/a | Open-sourced a production Kafka transport for Messenger | [package](https://packagist.org/packages/alvarorosado/event-driven-kafka-messenger-transport) |
| 13 | DoorDash (dated) | 2020-09-03 | RabbitMQ+Celery → Kafka | 900+ async task types | RabbitMQ outages; in-house Kafka expertise | [blog](https://careersatdoordash.com/blog/eliminating-task-processing-outages-with-kafka/) |
| 14 | Slack [PHP] (dated) | 2017-12-06 | Added Kafka in front of the Redis job queue | 1.4B jobs/day, 33k/s peak | Redis OOM lost jobs; Kafka as a durable buffer | [blog](https://slack.engineering/scaling-slacks-job-queue/) |
| 15 | Wikimedia [PHP] (dated) | 2018-03 | Redis → Kafka job queue | refreshLinks ~100 jobs/s, 400/s spikes | One platform (EventBus), retries, multi-DC | [wikitech](https://wikitech.wikimedia.org/wiki/Kafka_Job_Queue) |
| 16 | CircleCI (dated) | 2021-05-21 | RabbitMQ incident | ~12 h of blocked or delayed jobs | Patch upgrade added a 15-min consumer ack timeout | [postmortem](https://status.circleci.com/incidents/qxkj1832wf9l) |
| 17 | Klarna (dated) | 2021 | Kafka incident | Whole Kred (Erlang) cluster down for hours | Loss of 1 Kafka node triggered OOM in every app node | [blog](https://engineering.klarna.com/the-hunt-for-the-cluster-killer-erlang-bug-81dd0640aa81) |
| 18 | Simplebet (dated) | 2021 (RabbitMQ Summit) | Kafka → RabbitMQ | B2B market-update feed | Kafka costly to run and hard for customers to integrate | [talk repo](https://github.com/davydog187/migrating_from_kafka) |
| 19 | Zapier | 2022-01-21 (+2017 incident) | Chose RabbitMQ (Celery) | One message per Zap step | Autoscaling on queue depth, not CPU | [CNCF](https://www.cncf.io/blog/2022/01/21/keda-at-zapier/) |
| 20 | Mollie + Facile.it [PHP] (dated) | 2018-11 / 2019-05-29 | Beanstalk → RabbitMQ; PHP consumer design | Payments exports; insurance comparison | Long-running PHP consumers vs heartbeats and memory | [Mollie](https://medium.com/mollie-payments/keeping-rabbitmq-connections-alive-in-php-b11cb657d5fb), [Facile](https://engineering.facile.it/blog/eng/common-problems-faced-by-php-developers-in-consuming-an-ampq-message/) |

## Cases

### 1. Sentry: RabbitMQ + Celery → Kafka-based taskbroker (2025)
- **What changed:** self-hosted 25.9.0 (2025-09-17): "Taskbroker & taskworker are now enabled to replace Celery – eliminating the outdated RabbitMQ and reducing Redis memory consumption."
- **Design:** Sentry produces task activations to Kafka. Taskbroker (Rust) consumes them and keeps in-flight tasks in SQLite "to avoid head-of-line blocking, enable out-of-order execution and per-task acknowledgements". Workers pull tasks over gRPC.
- **Constraints:** partition count must be divisible by the number of broker replicas, and Sentry advises staying at or below 24 workers per broker.
- **Lesson:** Kafka gave Sentry one fewer system to run, but they still had to build RabbitMQ-style per-message acks on top of it.
- **Caveat:** Sentry has published no "why" blog post. The motive above comes from the release notes, plus long-standing forum statements that "eventually that all will become Kafka".

### 2. PagerDuty: 4.2M Kafka producers per hour (Aug 2025)
- **Root cause:** a new API-usage tracking feature (Scala, `pekko-connectors-kafka`) passed producer *settings* instead of a producer instance, so "every time send() is called, a new producer is created".
- **Impact:** 4.2M extra producers per hour (84x normal) drove broker JVM heap into GC thrash and then exhaustion, and it cascaded from one broker to the whole cluster. About 95% of API events got 502s for 38 min, and ~23% of notifications were delayed by 5+ min. Two incidents spanned 03:53 to 20:24 UTC.
- **Fix:** doubled the heap, rolling restarts, then rolled back the feature.
- **Lesson:** reuse the producer. [PHP] This is the default trap for PHP-FPM, where everything is per request.

### 3. Detectify: self-hosted RabbitMQ → Amazon MQ (Oct 2025)
- **Context:** 50+ services, including payments, notifications and data sync. Only a handful of engineers could debug the broker, and a forced Kubernetes upgrade put it at risk.
- **Migration:** downstream-first (consumers before producers), with Shovel as a failsafe. Zero downtime and no message loss.
- **Quorum queues:** most queues moved to quorum, except one queue doing hundreds of msg/s, where consensus overhead hurt performance.
- **Lesson:** they stayed on RabbitMQ and dropped the ops burden. "The hardest part wasn't the technical complexity—it was building confidence."

### 4. Uber: building a queue on top of Kafka (2021, 2026)
- **Scale:** 300+ services used Kafka for queueing in 2021. By 2026, 1,000+ consumer services, trillions of messages per day.
- **Problems:** partition count caps parallelism (1 s RPCs at 1,000 msg/s would need ~1,000 partitions), head-of-line blocking, and poison pills. Autocommit was off because billing cannot lose messages.
- **Fix:** a consumer proxy pushes each message to services over gRPC, commits offsets out of order, sends failures to a DLQ, and can delay processing per partition. Open-sourced as uForwarder (2026 post).
- **Lesson:** this is the same per-message-ack gap that share groups (KIP-932) now close.

### 5. Zalando: Kafka (Nakadi) feeding RabbitMQ (Apr 2024)
- **Architecture:** Nakadi (a REST event bus on Kafka) carries 1,000+ event types that trigger customer messages. A Stream Consumer pushes them into RabbitMQ, which Zalando calls the "backbone of our platform".
- **Problem:** during peaks RabbitMQ was overloaded and critical messages (order confirmations) risked missing SLOs.
- **Fix:** AIMD (TCP congestion control) throttling per event type, with higher priority recovering faster. Queues went from millions of messages to ~300k.
- **Lesson:** "Use Nakadi to persist the backlog, hence reducing risk to overload RabbitMQ." In other words, the log stores and the broker delivers.

### 6. Zendesk: RabbitMQ under spiky load (RabbitMQ Summit, Oct 2024) (partial)
- Talk by Paweł Bereza. Producers are spread by a load balancer. Once a threshold is crossed, consumers spill excess messages into extra queues and drain them when load drops.
- **Lesson:** RabbitMQ can absorb spikes with topology rather than raw throughput.
- Facts come from the [evoila recap](https://evoila.com/us/blog/rabbitmq-summit-2024-recap/); the video was not watched.

### 7. Shopify: Kafka partitions as the BFCM bottleneck (Nov 2025)
- **Scale tests:** 146M req/min and 80k+ checkouts/min, with a p99 scenario at 200M req/min. Black Friday 2024 peaked at 12 TB/min.
- **Finding:** "ETL pipelines needed Kafka partition increases to maintain data freshness during spikes" (on an analytics platform rebuilt in 2024).
- **Lesson:** partitions set your parallelism ceiling, so load-test them before peak, not during it. Shopify has used Kafka since 2014 and runs Ruby, not PHP.

### 8. LinkedIn: Kafka's creator replaces Kafka (Jun 2025) (partial)
- **What:** Northguard (segment-level replication, sharded Raft metadata) plus Xinfra, a virtual pub/sub layer spanning both Kafka and Northguard. Dual writes allow topics to migrate without downtime.
- **Scale and status:** tens of PB/day. Xinfra clients run in 90% of apps, and thousands of topics have migrated. Kafka is still running alongside.
- **Why:** "increasingly difficult to scale up and operate Kafka". The causes were load balancing and partition-based replication.
- **Note:** facts are from press coverage of the LinkedIn Engineering post. This scale is irrelevant to most teams; the takeaway is that the partition is the pain point.

### 9. 84codes / CloudKarafka: a hosting vendor quits Kafka (2024–2025)
- **Timeline:** announced Feb 2024, end of sales May 2024, end of life 2025-01-27, data deleted 2025-02-28.
- **Quote:** "With the addition of RabbitMQ Streams and our high performance AMQP broker LavinMQ we believe that CloudAMQP is the better option for many use cases that might have led potential customers to consider Apache Kafka."
- **Caveat:** this is the RabbitMQ hosting vendor talking. Treat it as a market signal, not evidence.

### 10. Inngest: Kafka disk full (Oct 2025)
- **What happened:** the Kafka cluster for trace spans (feeding ClickHouse) filled its disk, which exhausted file descriptors in the Event API. A week later, `OutOfOrderSequenceError` retries slowed producers and delayed function execution.
- **Fix:** added `retention.bytes` on top of time-based retention, plus alerting and producer batching changes.
- **Lesson:** with time-only retention, a traffic spike fills the disk.

### 11. Etsy [PHP]: making Kafka zone-resilient (Jan 2023, work done in fall 2021) (partial)
- **Before:** the Kafka cluster ran mostly in one GCP zone to save money. As Kafka grew more important, a zone outage would have meant stale search results.
- **After:** brokers on GKE across 3 zones, migrated with zero downtime. Part 2 (2023-02-09) covers broker upgrades without downtime.
- **Lesson:** a cluster set up cheaply in one zone becomes a liability once more teams depend on it.
- **Note:** fetch was blocked (403); details are from search snippets.

### 12. Exoticca [PHP]: Kafka with Symfony Messenger (2025)
- **What:** a travel company's in-house Messenger↔Kafka transport, later open-sourced. It "incorporat[es] lessons learned from production use".
- **Design choices:**
  - "Retries policies are not supported in Kafka". Retries go through Messenger's failure transport instead.
  - Payload is JSON, with Messenger metadata in Kafka headers, so one topic can carry several event types that consumers filter.
- **Lesson:** Symfony Messenger has no built-in Kafka transport, so expect to build or adopt one.

### 13. DoorDash: RabbitMQ + Celery → Kafka (Sep 2020) (dated) (partial)
- **Context:** 900+ async task types, including checkout, order transmission to merchants and Dasher location. When RabbitMQ failed, DoorDash effectively went down.
- **Problems:** failovers "took more than 20 minutes … often get stuck"; heavy load from Celery countdown/ETA tasks; poor observability.
- **Why Kafka:** "no in-house Celery or RabbitMQ experts", while DoorDash did have Kafka expertise.
- **New problems with Kafka:**
  - Head-of-line blocking, fixed with a local queue inside each worker.
  - A rebalance on every deploy, with several deploys a day.
  - Worker fleet ran at 2x capacity while feature flags switched traffic between brokers.
- **Counterpoint:** HN commenters argued that many of these failures were Celery misusing RabbitMQ.
- **Note:** the original page returns 403; facts come from the [HN thread](https://news.ycombinator.com/item?id=24699534) and search excerpts.

### 14. Slack [PHP/Hack]: Kafka in front of Redis (Dec 2017) (dated)
- **Trigger:** a database slowdown filled Redis memory, so Slack could not enqueue (and lost jobs) or even dequeue.
- **Scale:** 1.4B jobs/day, 33k/s peak. 16 brokers, 32 partitions per topic, RF 3, 2-day retention.
- **Design:**
  - Kafka was added in front of Redis as the "minimum viable change", rather than replacing Redis.
  - PHP posts to Kafkagate (Go, HTTP) and gets a leader-only ack.
  - JQRelay moves jobs from Kafka to Redis.
- **Gotcha:** Go escapes `< > &` in JSON and PHP escapes `/`, so the same job had different bytes in each runtime.
- **Rollout:** double-writes with a shadow relay, plus canaries on all 1,600 partitions.

### 15. Wikimedia [PHP]: MediaWiki job queue on Kafka (2018) (dated, still current)
- **Flow:** MediaWiki (PHP) sends jobs through EventBus/EventGate (HTTP) into Kafka. ChangeProp consumes them and POSTs each job to `/rpc/RunSingleJob.php`, which checks a signature on every job.
- **Features:** retries go to a retry topic with exponential backoff; dedup is stored in Redis; concurrency is set per rule; topics are mirrored across data centres.
- **Rollout:** staged from 2018-02-22 to a full switch on 2018-03-07. Redis reads were disabled on 2018-03-29. refreshLinks runs ~100 jobs/s with spikes to 400/s.
- **Lesson:** PHP never holds a Kafka consumer. Jobs are pushed to it over plain HTTP.

### 16. CircleCI: RabbitMQ patch upgrade kills consumers (May 2021) (dated)
- **Cause:** upgrading 3.8.9 → 3.8.16 brought a 15-min delivery-ack timeout. The changelog said it applied to quorum queues only, but it affected all queue types.
- **Impact:** long-running consumers were disconnected one by one until the VM-cleanup queue had zero consumers. Docker and machine jobs were blocked or delayed for ~11h48m.
- **Lesson:** alert on queues with zero consumers and read RabbitMQ release notes closely. [PHP] Long-running PHP jobs hit the same limit: `consumer_timeout` now defaults to 30 min.

### 17. Klarna: one Kafka node takes down an Erlang cluster (2021) (dated) (partial)
- **What happened:** routine maintenance ended with a broker killed by `kill -9`, and partition leader election was slow. Memory then grew on every Kred node until the OOM killer stepped in, and the cluster was down for hours.
- **Root cause:** a monitoring process pretty-printed data (1 GB binary ≈ 57 GB as a string) and locked a scheduler.
- **Lesson:** the author wrote that "Kafka being a soft dependency is not an assumption, it's a design goal." Test that degraded mode on purpose.
- **Note:** the original page returns 403; facts come from the [HN thread](https://news.ycombinator.com/item?id=31746090).

### 18. Simplebet: Kafka → RabbitMQ (RabbitMQ Summit 2021) (dated)
- **Context:** B2B sports-betting market updates, first delivered over Kafka.
- **Why they left:** Kafka was "difficult and expensive to maintain, and non-trivial for our customers to integrate with". RabbitMQ is "low-cost, low-latency" and "standards-based" (AMQP), so customers bring their own clients.
- **Lesson:** when consumers sit outside your company, protocol familiarity beats throughput.

### 19. Zapier: RabbitMQ at the heart of Zaps (2022; incident 2017)
- **Setup:** one RabbitMQ message per Zap step, consumed by Celery workers.
- **Problem:** Kubernetes autoscaling on CPU never fired, because I/O-bound workers idled at low CPU while queues grew.
- **Fix:** KEDA scales on ready messages. Zapier contributed multi-host RabbitMQ support to KEDA 2.4.0.
- **2017 incident:** one RabbitMQ node failure caused a 15-min hard outage, ~11k tasks stuck and ~139k webhooks lost ([status](https://status.zapier.com/incidents/q8qqnrj8062x)).
- **Lesson:** autoscale on queue depth, not CPU.

### 20. Mollie and Facile.it [PHP]: long-running PHP consumers (2018–2019) (dated)
- **Mollie (payments), ~Nov 2018 (partial, Medium blocked):**
  - Replaced Beanstalk with RabbitMQ for HA and features.
  - The payment-export daemon then hit random "invalid frame type" errors. Heartbeats were missed while synchronous PHP did long exports.
  - A later php-amqplib release reportedly broke their fix.
- **Facile.it, 2019-05-29:** PHP consumers suffered memory leaks, single-threading, awkward deploys and heartbeat issues (dead TCP connections take ~11 min to surface on Linux). They built a Rust consumer that runs a PHP command per message. The trade-off is framework bootstrap cost on every message.
- **Lesson:** the problem is PHP's process model, not the broker. The same shape returns with Kafka consumers.

## Stories worth telling on stage

1. **PagerDuty, Aug 2025:** one line of client code creates a Kafka producer per `send()`. That is 4.2M producers an hour, the broker heap dies, and ~95% of incoming events bounce for 38 minutes. Then ask the room: "In PHP-FPM, where does your producer live?" Close with Slack (Kafkagate) and Wikimedia (EventGate), which keep PHP off raw Kafka for this reason.
2. **"Everyone who left RabbitMQ for Kafka rebuilt RabbitMQ":** DoorDash (local queues, 2020), Uber (uForwarder, out-of-order commits and DLQ), Sentry (taskbroker plus SQLite "to avoid head-of-line blocking … per-task acknowledgements", 2025). Then in Feb 2026 Kafka 4.2 shipped share groups. The arc shows why the difference now is how much the broker knows about each message.
3. **CircleCI, May 2021:** a routine RabbitMQ patch upgrade added a 15-min ack timeout that the changelog described as quorum-only. Consumers dropped one by one until the VM-cleanup queue had none, and about 12 hours of degradation followed. Tie it to PHP workers running long jobs against `consumer_timeout` (now 30 min by default).
   - Runner-up: DoorDash's "no in-house RabbitMQ experts" next to Detectify's "accidental RabbitMQ experts". Both show that staffing picks the broker.

## Gaps (searched, not found)
- No company had published a production case study of Kafka share groups (KIP-932) by 2026-10.
- No big-name Laravel engineering-blog case study for either broker. Symfony evidence is limited to small shops and library authors (Exoticca; the Symfony blog post on the streaming AMQP transport, 2025-04-25).
- No 2023–2026 company post on moving from Kafka to RabbitMQ beyond the 84codes vendor decision. Most "we switched back" posts are anonymous Medium/DEV pieces and were excluded.

## Sources
- Sentry: https://github.com/getsentry/self-hosted/releases/tag/25.9.0 · https://github.com/getsentry/taskbroker · https://develop.sentry.dev/self-hosted/tasks/
- PagerDuty: https://www.pagerduty.com/eng/august-28-kafka-outages-what-happened-and-how-were-improving/
- Detectify: https://blog.detectify.com/best-practices/migrating-critical-messaging-from-self-hosted-rabbitmq-to-amazon-mq/
- Uber: https://www.uber.com/en-IN/blog/kafka-async-queuing-with-consumer-proxy/ · https://www.uber.com/us/en/blog/introducing-ufowarder/
- Zalando: https://engineering.zalando.com/posts/2024/04/enhancing-distributed-system-load-shedding-with-tcp-congestion-control-algorithm.html
- Zendesk: https://www.youtube.com/watch?v=k0_ESUdc7NU · https://evoila.com/us/blog/rabbitmq-summit-2024-recap/
- Shopify: https://shopify.engineering/bfcm-readiness-2025
- LinkedIn: https://siliconangle.com/2025/06/25/linkedin-introduces-northguard-xinfra-replace-kafka-scalable-log-storage/
- 84codes: https://www.cloudkarafka.com/blog/end-of-life-announcement.html
- Inngest: https://www.inngest.com/blog/2025-10-24-october-incident-report
- Etsy: https://www.etsy.com/codeascraft/adding-zonal-resiliency-to-etsys-kafka-cluster-part-1 · https://www.etsy.com/codeascraft/leveraging-zonal-resiliency-to-improve-updates-for-etsys-kafka-cluster-part-2
- Exoticca: https://packagist.org/packages/alvarorosado/event-driven-kafka-messenger-transport
- DoorDash: https://careersatdoordash.com/blog/eliminating-task-processing-outages-with-kafka/ · https://news.ycombinator.com/item?id=24699534
- Slack: https://slack.engineering/scaling-slacks-job-queue/
- Wikimedia: https://wikitech.wikimedia.org/wiki/Kafka_Job_Queue · https://phabricator.wikimedia.org/T185052
- CircleCI: https://status.circleci.com/incidents/qxkj1832wf9l
- Klarna: https://engineering.klarna.com/the-hunt-for-the-cluster-killer-erlang-bug-81dd0640aa81 · https://news.ycombinator.com/item?id=31746090
- Simplebet: https://github.com/davydog187/migrating_from_kafka
- Zapier: https://www.cncf.io/blog/2022/01/21/keda-at-zapier/ · https://status.zapier.com/incidents/q8qqnrj8062x
- Mollie: https://medium.com/mollie-payments/keeping-rabbitmq-connections-alive-in-php-b11cb657d5fb · https://www.rabbitmq.com/blog/2018/12/04/this-month-in-rabbitmq-dec-4-2018
- Facile.it: https://engineering.facile.it/blog/eng/common-problems-faced-by-php-developers-in-consuming-an-ampq-message/
- Context: https://www.rabbitmq.com/docs/consumers (`consumer_timeout` default 30 min) · https://symfony.com/blog/introducing-a-streaming-amqp-transport-for-symfony-messenger
