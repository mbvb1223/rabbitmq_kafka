# RabbitMQ news & releases, Apr 2025 → Oct 2026

As of 2026-10-04. Demo pin: `rabbitmq:4.3.6-management` (Erlang 27.3.4.18). Numbers like [13] point to **Sources**.

## TL;DR for the demo

- **4.3 (2026-04-23) is the big release for us**: Khepri is the only metadata store (Mnesia removed), and quorum queues gain 32 strict priorities, delayed retry, consumer timeouts and separate `delivery-count` / `acquired-count` [13][14].
- **PHP trap on 4.3, tested locally**: php-amqplib's default `queue_declare('name')` is durable=false + exclusive=false, which is a "transient non-exclusive" queue. 4.3 denies these by default and **closes the whole connection** [14][35][V].
- **`nack` ≠ `reject` on 4.3 quorum queues, tested locally**: `basic.nack` never counts toward `x-delivery-limit` (the message loops forever), while `basic.reject` does [13][36][V].
- **Delayed messages**: the community `x-delay` plugin was archived 2026-09-24 and won't run on 4.3. On 4.3, publish-time delay is Tanzu-only. **4.4 (not released yet) adds open-source quorum-queue delays via an `x-opt-delivery-time` header that AMQP 0-9-1 can set**, so PHP can reach it [13][17][19].
- **Community support only covers the newest series**: 4.3 community support ends 2026-11-30. Older-series patches (4.2.10+, 4.1.9+, 3.13.x) ship to Broadcom customers only [20][21][24][25].
- **Security wave**: 83 GitHub advisories were published between 2026-05-06 and 2026-09-18 (2 critical, 20 high). About half were found internally by Team RabbitMQ / Broadcom. **4.3.6 is the minimum version that covers every published advisory** [26][27][28].
- **The newest features are AMQP 1.0-first**: SQL stream filters (4.2), direct reply-to over AMQP 1.0 (4.2), modified outcome / per-message delay override (4.3) and deferral tokens (4.4). None of them work from php-amqplib (0-9-1) [9][11][13][18].
- **Tooling churn hit PHP shops**: Bitnami's free `bitnami/rabbitmq` images were frozen to `bitnamilegacy` (catalog deleted 2025-09-29), and the apt repos moved to `deb*.rabbitmq.com` (old ones off 2025-11-01) [6][30].

## Timeline

| Date | Version / event | What changed | Link |
|---|---|---|---|
| 2025-04-08 | Blog: 4.1 performance | Quorum-queue (QQ) disk reads moved to channels: ~100 → ~6000 msg/s publish under consumer load (20 KB msgs) | [3] |
| 2025-04-15 | **4.1.0** | QQ throughput, AMQP 1.0 property filters on streams, rabbitmqadmin v2, `frame_max` pre-auth 4096→8192, MQTT max packet 256→16 MiB | [1][2] |
| 2025-04-16 | AMQP 1.0 over WebSocket | New `rabbitmq_web_amqp` plugin, **Tanzu-only (closed source)** | [4] |
| 2025-04-24 | CVE-2025-32433 (Erlang SSH) | RabbitMQ not affected (doesn't use SSH) | [5] |
| 2025-06-18 | CVE-2025-50200 | Node could log the Basic Auth header | [26] |
| 2025-07-16 | apt repos moving | `ppa*.rabbitmq.com` → `deb*.rabbitmq.com`, old repos off 2025-11-01 | [6] |
| 2025-07-16 | Bitnami catalog change | Free versioned images moved to `bitnamilegacy`, `docker.io/bitnami` deleted 2025-09-29 | [30] |
| 2025-07-29 | Blog: mirrored → quorum | Blue/green migration from 3.13 using rabbitmqadmin v2 | [7] |
| 2025-09-01 | Blog: Khepri roadmap | 4.2 default for new clusters, then required | [8] |
| 2025-09-08 | `rabbitmq-stream-s3` repo (AWS) | Tiered (S3) storage for streams, Apache-2.0, needs custom server branches | [33] |
| 2025-09-23 | Blog: SQL filters | Broker-side SQL filtering on streams, AMQP 1.0 only | [9] |
| 2025-09-26 | Blog: stream read-ahead | Up to ~10× delivery for small-chunk streams (4.2) | [10] |
| 2025-10-27 | **4.2.0** | Khepri default for new clusters, SQL stream filters, AMQP 1.0 direct reply-to, message interceptors, local shovels | [11] |
| 2025-11-06 | MQ Summit Berlin | RabbitMQ talks incl. streams beyond local disk (AWS) | [32] |
| 2025-11-20 | Amazon MQ supports 4.2 | Managed 4.2 (AMQP 1.0, Khepri, QQ priorities, local shovels) | [12] |
| 2026-01-31 | 4.1 community EOL | 4.1.8 (2026-01-22) is the last public 4.1 build | [20] |
| 2026-03-16 | Tutorials modernised | Getting-started tutorials (incl. PHP) now declare durable **quorum** queues | [34] |
| 2026-04-23 | **4.3.0** | Mnesia removed, QQ priorities, delayed retry, consumer timeout, CQv1 gone, deprecated features denied by default. Tanzu adds Message Scheduler, JMS queue, Stream Browser | [13][14][23] |
| 2026-05-06 → 09-18 | Advisory wave | 83 GHSAs (70 with CVE IDs) | [26] |
| 2026-07-09 | 2 critical CVEs | OAuth2 JWKS `verify_none` fallback, trust-store issuer+serial bypass | [27][28] |
| 2026-07-20 | 4.3.3 / 4.2.9 | Minimum Erlang 27.0 (OTP 26 EOL). 4.2.9 is the last public 4.2 build | [15] |
| 2026-07-31 | 4.2 community EOL | 4.2.10/4.2.11 listed by Broadcom only | [20][25] |
| 2026-08-18 | CVE-2026-67419 | Consecutive `#` in topic bindings → combinatorial routing DoS (fixed 4.3.5) | [29] |
| 2026-09-04 | PR #16583 "QQ: v9" merged | QQ delayed messages + AMQP 1.0 deferral tokens (targets 4.4) | [18] |
| 2026-09-14 | **4.3.6** | Current patch, fixes the last 3 published advisories | [16][26] |
| 2026-09-24 | Delayed-message plugin archived | Repo read-only, README points to 4.4 QQ delays | [19] |
| 2026-09-29 | 4.4.0 release notes drafted | `v4.4.x` branch exists, `main` builds now say `5.0.0-alpha` | [17] |
| 2026-10-21/22 | MQ Summit 2026 (Haarlem) | Ansari on JMS/AMQP 1.0, Davis on "infinite streaming" | [31] |

## Context: 4.0 (2024-09, before the window)

- AMQP 1.0 became a **core, always-on protocol**. The plugin is now a no-op [37].
- Classic queue mirroring was **removed**. Quorum queues and streams are the only replicated types [7][37].
- Quorum queues got a default delivery limit of 20 [37].
- Default max message size dropped to 16 MiB [37].

## 4.1 — 2025-04-15

- QQ log reads moved off the queue process into channels/sessions, so consumer throughput goes up and publishers interfere less [2][3].
- AMQP 1.0 `properties` / `application-properties` filters let many consumers each read an ordered subset of a stream [2].
- Breaking: pre-auth `frame_max` 4096 → 8192. Node.js `amqplib` < 0.10.7 can't connect. Don't override `frame_max` [2].
- MQTT default max packet size 256 MiB → 16 MiB [2].
- Kubernetes peer discovery rewritten to seed from pod `-0` instead of calling the K8s API [2].
- `rabbitmqctl force_reset` deprecated because it is incompatible with Khepri [2].
- Policy: 4.0.x patches went customer-only once 4.1 shipped [1].

## 4.2 — 2025-10-27

- **Khepri is the default for new clusters**. Upgraded clusters keep their store until `khepri_db` is enabled [11].
- **SQL filter expressions on streams** (AMQP 1.0 only): `=`, `LIKE`, `IN`, `AND/OR/NOT`, `UTC()`. Combined with Bloom filters: ~4.87 M msg/s vs ~405 k msg/s with SQL only. The blog notes Kafka still lacks broker-side filtering (KAFKA-6020) [9][11].
- Direct reply-to over AMQP 1.0, including cross-protocol with 0-9-1 [11].
- Message interceptors for AMQP 1.0, AMQP 0-9-1 and MQTT v3/v5, with built-ins for outgoing timestamps and MQTT client ID [11].
- Local shovels: intra-cluster, AMQP 1.0-based, no TCP connection [11].
- Breaking: an AMQP 1.0 message without a header now defaults to `durable=false` (per spec). `rabbitmq_raft_*` Prometheus metrics renamed [11].
- Stream read-ahead (4 KiB, on by default): ~16 k → ~134 k msg/s for 12-byte single-message chunks [10].
- No release blog post for 4.2, only GitHub notes [11].

## 4.3 — 2026-04-23 (our demo version)

- **Khepri only, Mnesia removed**. A cluster needs a majority of nodes online. `cluster_partition_handling` (`autoheal`, `pause_minority`) keys are ignored. Recovery follows Raft for metadata, QQs and streams alike [14].
- **Upgrade path**: from 4.2.x only. Required flags include `khepri_db` [14].
- **QQ state machine v8** (Ra 3.x) [13][14]:
  - 32 strict priorities (0–31), always on, no opt-in argument. Per-priority counts in the UI [36].
  - Delayed retry via `x-delayed-retry-type` / `-min` / `-max` (or policy). Backoff = `min(min × delivery-count, max)` [13].
  - `delivery-count` (failed attempts) is now separate from `acquired-count` (every return). **0-9-1 `basic.nack` increments only acquired-count, `basic.reject` increments both** [13][36].
  - Consumer timeout per consumer, queue, policy or global. The blog says AMQP 0-9-1 gets `basic.cancel` first. The consumers doc still says the channel is closed, so test before relying on either [13].
  - ~50% less per-message memory for messages ≤ 32 KiB [13].
- **Consumer timeouts are no longer evaluated for classic queues and streams** [14].
- **Deprecated features now `denied_by_default`**: `transient_nonexcl_queues`, `global_qos`, `queue_master_locator`, `amqp_address_v1`, `amqp_filter_set_bug`. `ram_node_type` removed [14][V].
- **CQv1 removed**: `x-queue-version=1` is rejected. The notes also say `x-queue-mode` is rejected, but 4.3.6 accepted `lazy` in our test [14][V].
- `x-modulus-hash` exchange moved into core with stable distribution [14].
- AMQP 1.0: rejected outcome now carries the queue name and reason. SAC state changes are signalled to consumers [13].
- **Tanzu-only additions in 4.3**: Message Scheduler / delayed queues (replacing the 4.2 Tanzu delayed-queue plugin), JMS queue type with selectors / browser / delivery delay, Stream Browser, Linter, Spark connector [13][23].
- 4.3.x patches: 4.3.1 (05-20), 4.3.2 (06-15), 4.3.3 (07-20, **minimum Erlang 27.0**), 4.3.4 (07-23), 4.3.5 (08-17), 4.3.6 (09-14). 4.3.6 adds a `channel_tx_message_max` limit and fixes exchange-deletion binding cleanup [15][16].

## 4.4 (in preparation) and beyond

- No GA yet. The `v4.4.x` branch and 4.4.0 alpha builds exist, and the release notes are "far from" complete (2026-09-29) [17].
- **Open-source QQ delayed messages**: `x-opt-delivery-time` (epoch ms) as an AMQP 1.0 annotation or an **AMQP 0-9-1 `long` header** → reachable from php-amqplib. Priorities are ignored for delayed messages [17][18].
- AMQP 1.0-only: deferral tokens (park a message, pull it back on demand) and `modified` + `x-opt-delivery-time` to delay a redelivery [17][18].
- Also: `x-stream-initial-offset`, `x-name-prefix` for server-named queues (0-9-1), PBKDF2 password hashing, Unix-socket listeners, classic-queue message store v2 (no downgrade to 4.3) [17].
- Breaking: stricter OIDC discovery validation. `GET /api/auth/hash_password/{pw}` → `POST` [17].
- `main` builds are now labelled `5.0.0-alpha` (first seen 2026-09-30). No 5.0 announcement found (unverified intent) [17].
- Inference, not announced: 4.3 community EOL on 2026-11-30 suggests 4.4 GA before then [20].

## Delayed-message plugin archive

- `rabbitmq/rabbitmq-delayed-message-exchange` was archived on **2026-09-24**. Last release v4.2.0 (2025-11-04) [19].
- Reason: it is built on Mnesia, which was removed in 4.3. A distributed redesign "took a few person-years worth of R&D" [19].
- Listed alternatives: QQ delays in 4.4, DLX+TTL "delay queues", or build your own on Khepri + RocksDB [19].
- On 4.3 the only supported publish-time delay is Tanzu's Message Scheduler [13][23].
- Impact: any `x-delayed-message` exchange or `x-delay` header (Symfony Messenger / Laravel delay setups) blocks the upgrade to 4.3.

## Broadcom / Tanzu licensing

- The core broker is still MPL 2.0. Tanzu RabbitMQ = OSS core + commercial plugins + longer support [22][23].
- **Community support**: newest series only. Older-series patches are "not made available to non-paying users", except possibly for very high-severity CVEs [21].
- Support lifecycle [20]:

  | Series | Community EOL | Commercial EOL |
  |---|---|---|
  | 4.3 | 2026-11-30 | 2028-04-30 |
  | 4.2 | 2026-07-31 | 2030-06-30 |
  | 4.1 | 2026-01-31 | 2027-04-30 |
  | 3.13 | 2024-09-30 | 2029-12-31 |
- In practice: GitHub's last public builds are 4.1.8 and 4.2.9. Broadcom lists 4.2.10 (08-18) and 4.2.11 (09-15), and 3.13.20 (09-15) is marked "binary artifacts … only available to customers with a valid commercial support license" [24][25].
- Security fixes for old series appear in advisories as 4.1.11, 4.0.20 and 3.13.15, none of which are public builds [27].
- Commercial-only features [4][13][22][23]:
  - Warm standby DR
  - Intra-cluster compression (up to 96%)
  - Distributed shovels
  - AMQP 1.0 over WebSocket
  - Audit logging
  - FIPS 140-2
  - Stream Browser
  - JMS queue + selectors
  - Message Scheduler
  - Spark connector
  - Linter
- Packaging: Tanzu RabbitMQ is sold inside "Tanzu Data Intelligence" subscriptions (unverified: pricing model).

## Security (Apr 2025 → Oct 2026)

- 2025 was quiet: CVE-2025-30219 (mgmt UI XSS, 2025-03-25), CVE-2025-50200 (Basic Auth header logged, 2025-06-18). CVE-2025-32433 (Erlang SSH) did not affect RabbitMQ [5][26].
- **2026 wave: 83 advisories from 2026-05-06 to 2026-09-18**, by severity:
  - 2 critical
  - 20 high
  - 46 medium
  - 15 low
- 41 of the 83 say "identified by Team RabbitMQ and/or other teams at Broadcom" [26].
- Critical (2026-07-09, fixed in 4.2.6 / 4.3.0) [27][28]:
  - CVE-2026-67404: the OAuth2 plugin silently falls back to `verify_none` for the JWKS fetch when no CA bundle exists, e.g. a minimal container image.
  - CVE-2026-67231: trust-store whitelist matched issuer + serial only, so a forged self-signed cert passed.
- Themes [26]:
  - Management UI XSS
  - OAuth2 / JWT handling
  - Pre-auth memory exhaustion (AMQP 1.0, Web-MQTT/STOMP)
  - Authz bypasses (topic auth, passive declare, federation cross-vhost)
- Demo-relevant: **CVE-2026-67419**. A binding key like `#.#.#` caused combinatorial routing work, fixed in 4.3.5. Our `orders.#` example is safe [29].
- Coverage: per-advisory patched versions top out at 4.3.6, so the demo pin covers all published advisories as of 2026-10-04 [26].

## Ecosystem / ops

- **Bitnami**: free versioned images stopped on 2025-08-28 and the `docker.io/bitnami` catalog was deleted on 2025-09-29. `bitnamilegacy/*` gets no updates. Use the official `rabbitmq:*-management` image (as our compose does) [30].
- **apt**: move to `deb1/deb2.rabbitmq.com` with Team RabbitMQ's signing key. The old Cloudsmith-backed `ppa*` repos were frozen and switched off on 2025-11-01 [6].
- **Amazon MQ**: RabbitMQ 4.2 available since 2025-11-20 [12].
- **AWS `rabbitmq-stream-s3`**: S3 tiered storage for streams, v1.0.0-beta.2 (2026-09-29). It needs the `streams-tiered-storage` server branch, so it is not in mainline [33].
- **Tutorials**: since 2026-03-16 the PHP getting-started tutorials declare `durable=true` + `x-queue-type: quorum` [34].

## Blog posts & talks

- rabbitmq.com blog in the window (all fetched):
  - 4.1 perf (04-08)
  - 4.1.0 (04-15)
  - AMQP over WebSocket (04-16)
  - CVE-2025-32433 (04-24)
  - Go stream client fix (07-04)
  - apt move (07-16)
  - mirrored → quorum migration (07-29)
  - Khepri roadmap (09-01)
  - SQL filters (09-23)
  - stream delivery (09-26)
  - 4.3 Highlights (2026-04-23)
- **No blog posts since 2026-04-23** [archive, 1–13].
- Khepri roadmap claims: 1000 vhosts, `start_app` 419 s → 16 s. Migration took under 13 s in their tests [8].
- **MQ Summit Berlin, 2025-11-06** [32]:
  - Brett Cameron, "Replacing legacy message queueing solutions with RabbitMQ"
  - Simon Unge (AWS), "Breaking Storage Barriers: How RabbitMQ Streams Scale Beyond Local Disk"
- **MQ Summit 2026, Haarlem, 2026-10-21/22** (RabbitMQ Summit's successor, now multi-broker incl. Kafka) [31]:
  - David Ansari (Broadcom), "Bringing JMS to RabbitMQ: AMQP 1.0 and Cross-Broker Interoperability"
  - Michael Davis (Amazon MQ), "Bringing infinite streaming to RabbitMQ"
  - Lovisa Johansson (84codes), "The Return of the Message: Why the AI Boom Needs Message Queues to Survive"
- Spring I/O 2026: Gregory Green, "Spring for Rabbit Super Stream and SQL filters" (unverified: seen only via the daily.dev aggregator).

## Verified locally [V]

Tested on 2026-10-04 with `rabbitmq:4.3.6-management` and php-amqplib v3.7.5 (throwaway container, removed afterwards).

| Test | Result |
|---|---|
| `$ch->queue_declare('q')` (defaults: durable=false, exclusive=false, auto_delete=true) | `AMQPConnectionClosedException: INTERNAL_ERROR - Feature transient_nonexcl_queues is deprecated` |
| `queue_declare('')` (server-named, defaults) | same error |
| `queue_declare('', false, false, true, true)` (exclusive) | OK |
| durable classic + `x-queue-version=1` | `PRECONDITION_FAILED … unsupported queue version '1'` |
| durable classic + `x-queue-mode=lazy` | OK (accepted despite the 4.3.0 notes) |
| `basic_qos(0, 10, true)` (global) | OK. The source rewrites it to per-consumer when `global_qos` is denied |
| QQ `x-delivery-limit=2`, `nack(requeue)` ×6 | never dropped. `x-acquired-count` 1…5, no `x-delivery-count` |
| QQ `x-delivery-limit=2`, `reject(requeue)` | `x-delivery-count` 1, 2, then dropped after the 3rd delivery |
| QQ with `x-delayed-retry-*` args | declared OK |
| `khepri_db` flag | enabled |

## Demo talking points

- **"RabbitMQ 4.3 is a Raft broker now."** Khepri, quorum queues and streams all use Raft, much as KRaft does for Kafka. Majority-or-unavailable is the trade-off, and partition-handling knobs are gone [14].
- **Show the trap first**: run the old-style `queue_declare('jobs')` and watch the connection die, then fix it with `durable=true` + `x-queue-type=quorum`. One slide of value for every PHP dev upgrading [V].
- **Demo A wording**: "use `reject`, not `nack`". Show the `x-acquired-count` vs `x-delivery-count` headers live [V].
- **Delays**: "4.3 open source has delayed *retry* but not delayed *publish*. That's Tanzu, or 4.4." Say that 4.4 is unreleased and re-check before the talk [13][17][19].
- **Feature gravity is moving to AMQP 1.0** (SQL filters, deferral, modified outcome). That is the honest PHP gap: php-amqplib is 0-9-1 only [9][11][18].
- **Licensing slide (30 s)**: free = latest series only, upgrade every ~6 months, or pay Broadcom. Kafka has no equivalent split [20][21].
- **Security hygiene**: 83 advisories in 5 months. Pin a current patch (4.3.6) and don't expose the management UI [26].
- **Corrections for docs/02–03**:
  - Delayed publish is Tanzu-only **on 4.3**; 4.4 brings it to open-source QQs, including over 0-9-1.
  - "Deferral by token" is a 4.4 feature, not 4.3.
  - SQL stream filtering is AMQP 1.0-only; Bloom `x-stream-filter` remains the 0-9-1 path.

## Sources

1. RabbitMQ 4.1.0 is released — rabbitmq.com blog (M. Klishin) — 2025-04-15 — https://www.rabbitmq.com/blog/2025/04/15/rabbitmq-4.1.0-is-released
2. RabbitMQ 4.1.0 release notes — GitHub rabbitmq-server — 2025-04-15 — https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.1.0
3. RabbitMQ 4.1 Performance Improvements — rabbitmq.com blog (M. Kuratczyk) — 2025-04-08 — https://www.rabbitmq.com/blog/2025/04/08/4.1-performance-improvements
4. AMQP 1.0 over WebSocket — rabbitmq.com blog (D. Ansari) — 2025-04-16 — https://www.rabbitmq.com/blog/2025/04/16/amqp-websocket
5. RabbitMQ is not affected by CVE-2025-32433 — rabbitmq.com blog (M. Klishin) — 2025-04-24 — https://www.rabbitmq.com/blog/2025/04/24/rabbitmq-is-not-affected-by-cve-2025-32433
6. Team RabbitMQ's Debian (apt) Repositories are Moving — rabbitmq.com blog (M. Klishin) — 2025-07-16 — https://www.rabbitmq.com/blog/2025/07/16/debian-apt-repositories-are-moving
7. Migrating from Classic Mirrored Queues to Quorum Queues in 2025 — rabbitmq.com blog (A. Perez, M. Klishin) — 2025-07-29 — https://www.rabbitmq.com/blog/2025/07/29/latest-benefits-of-rmq-and-migrating-to-qq-along-the-way
8. The Roadmap for Making Khepri the Default Metadata Store — rabbitmq.com blog (M. Gary) — 2025-09-01 — https://www.rabbitmq.com/blog/2025/09/01/6-khepri-default
9. Broker-Side SQL Filtering with RabbitMQ Streams — rabbitmq.com blog (D. Ansari) — 2025-09-23 — https://www.rabbitmq.com/blog/2025/09/23/sql-filter-expressions
10. Delivery Optimization for RabbitMQ Streams — rabbitmq.com blog (A. Cogoluègnes) — 2025-09-26 — https://www.rabbitmq.com/blog/2025/09/26/stream-delivery-optimization
11. RabbitMQ 4.2.0 release notes — GitHub rabbitmq-server — 2025-10-27 — https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.2.0
12. Amazon MQ now supports RabbitMQ version 4.2 — AWS What's New — 2025-11-20 — https://aws.amazon.com/about-aws/whats-new/2025/11/amazon-mq-rabbitmq-42/
13. RabbitMQ 4.3 Highlights — rabbitmq.com blog (D. Ansari) — 2026-04-23 — https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release
14. RabbitMQ 4.3.0 release notes — GitHub rabbitmq-server — 2026-04-23 — https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.3.0
15. RabbitMQ 4.3.3 release notes — GitHub rabbitmq-server — 2026-07-20 — https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.3.3
16. RabbitMQ 4.3.6 release notes — GitHub rabbitmq-server — 2026-09-14 — https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.3.6
17. RabbitMQ 4.4.0 release notes (draft, `main`) + PR #17658 — GitHub rabbitmq-server — 2026-09-29 — https://github.com/rabbitmq/rabbitmq-server/blob/main/release-notes/4.4.0.md
18. PR #16583 "QQ: v9" (delays, deferral tokens) — GitHub rabbitmq-server — merged 2026-09-04 — https://github.com/rabbitmq/rabbitmq-server/pull/16583
19. rabbitmq-delayed-message-exchange (archived) README — GitHub — archived 2026-09-24 — https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
20. Release Information (support table) — rabbitmq.com — undated, accessed 2026-10-04 — https://www.rabbitmq.com/release-information
21. COMMUNITY_SUPPORT.md — GitHub rabbitmq-server — last changed 2024-12-03 — https://github.com/rabbitmq/rabbitmq-server/blob/main/COMMUNITY_SUPPORT.md
22. Commercial Features — rabbitmq.com — undated, accessed 2026-10-04 — https://www.rabbitmq.com/commercial-features
23. VMware Tanzu RabbitMQ RPM 4.3 release notes — Broadcom TechDocs — 4.3.0 2026-04-23 … 4.3.6 2026-09-15 — https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-rpm/4-3/tanzu-rabbitmq-rpm-offering/site-release-notes.html
24. Open Source RabbitMQ 3.13 release notes (customer-only binaries) — Broadcom TechDocs — 3.13.20 2026-09-15 — https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/open-source-rabbitmq/3-13/opn-src-rabbitmq/site-release-notes.html
25. Open Source RabbitMQ 4.2 release notes — Broadcom TechDocs — 4.2.11 2026-09-15 — https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/open-source-rabbitmq/4-2/opn-src-rabbitmq/site-release-notes.html
26. rabbitmq-server security advisories (90 total) — GitHub — 2021-06 … 2026-09-18 — https://github.com/rabbitmq/rabbitmq-server/security/advisories
27. GHSA-37wx-r6q9-6fhj / CVE-2026-67404 (OAuth2 JWKS verify_none) — GitHub — 2026-07-09 — https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-37wx-r6q9-6fhj
28. GHSA-cw8c-4m83-9c6w / CVE-2026-67231 (trust-store bypass) — GitHub — 2026-07-09 — https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-cw8c-4m83-9c6w
29. GHSA-h964-v5mf-22cq / CVE-2026-67419 (consecutive topic wildcards) — GitHub — 2026-08-18 — https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-h964-v5mf-22cq
30. Upcoming changes to the Bitnami catalog (issue #35164) — GitHub bitnami/charts — 2025-07-16 — https://github.com/bitnami/charts/issues/35164
31. MQ Summit 2026 — mqsummit.com — event 2026-10-21/22 (lineup post 2026-08-11) — https://mqsummit.com/
32. MQ Summit Berlin 2025 archive — mqsummit.com — 2025-11-06 — https://mqsummit.com/archives/berlin_2025
33. amazon-mq/rabbitmq-stream-s3 — GitHub — created 2025-09-08, v1.0.0-beta.2 2026-09-29 — https://github.com/amazon-mq/rabbitmq-stream-s3
34. RabbitMQ tutorial one (PHP) + website commit "Change Getting Started tutorials to use quorum queues" — rabbitmq.com / GitHub — 2026-03-16 — https://www.rabbitmq.com/tutorials/tutorial-one-php
35. php-amqplib `AMQPChannel::queue_declare` defaults — GitHub php-amqplib (v3.7.5 released 2026-09-28) — https://github.com/php-amqplib/php-amqplib/blob/master/PhpAmqpLib/Channel/AMQPChannel.php
36. Quorum Queues guide (priorities, delivery-count table, delayed retry) — rabbitmq.com docs — undated, accessed 2026-10-04 — https://www.rabbitmq.com/docs/quorum-queues
37. RabbitMQ 4.0.1 release notes (context: AMQP 1.0 core, mirroring removed, QQ delivery-limit 20) — GitHub rabbitmq-server — 2024-09-19 — https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.0.1

[V] = verified locally on 2026-10-04, see "Verified locally".
