# Apache Kafka — news & releases, Apr 2025 → Oct 2026

As of 2026-10-04. Demo pin: `apache/kafka:4.3.1`. `[n]` = entry in Sources.

## TL;DR for the demo

- **4.4.0 is not out yet.** RC3 vote closes **2026-10-05** [13]. 4.4 ships **dead-letter queues for share groups (KIP-1191)** [14][20]. If the talk lands after the release, "share groups have no DLQ routing" (docs/01, docs/03) becomes stale. Reword it to "DLQ exists but is opt-in, and by default it stores headers only".
- **Our 4.3.1 pin is fine.** It is the latest 4.3 release; 4.3.2 RC0 started voting on 2026-09-29 [15]. On a fresh 4.3.1 cluster, share groups are on by default (`share.version=1` is the latest production level [10]). The single-node RF=1 overrides for share state in `compose.yaml` are required [8].
- **Share groups are GA since 4.2.0 (2026-02-17)** [4]. Path: early access in 4.0, preview in 4.1, GA in 4.2 [1][3][8]. 4.3 added per-group knobs (KIP-1240) [5]. Java is the only GA client. librdkafka's share consumer is still Preview [39], and Confluent targets non-Java support for H2 2026 [33].
- **The classic consumer protocol is on its way out.** 4.3 logs a hint, 5.0 makes `group.protocol=consumer` the default, and 6.0 removes classic from the Java consumer (KIP-1274) [22]. librdkafka has had KIP-848 GA since 2.12.0 (2025-10-08) [39]. KIP-848 does **not** lift the partitions = max-workers ceiling. Only share groups do.
- **ZooKeeper is gone since 4.0 (2025-03-18).** 4.0 also raised Java minimums: clients need Java 11 and brokers need Java 17 [1][8]. 3.9 was the last ZooKeeper "bridge" release [2].
- **Diskless topics (KIP-1150) were accepted 2026-03-02 but are not in any release** [23][24]. Present them as a direction, not a feature.
- **IBM owns Confluent.** The deal was announced 2025-12-08 and closed 2026-03-17: $31/share, about $11B [29][30]. Apache Kafka itself stays under ASF governance.
- **CVEs: 4.3.1 is clear of all 2025–2026 Kafka CVEs that have code fixes** [27]. CVE-2026-41115 has no code fix; the action is to audit group ACLs [28].

## Timeline

| Date | Version / event | What changed | Link |
|---|---|---|---|
| 2025-03-18 | **Kafka 4.0.0** | KRaft only (ZooKeeper removed). KIP-848 GA. KIP-932 early access. Clients need Java 11, brokers Java 17. Log4j2 | [1] |
| 2025-05-20 | 3.9.1 | Bugfix, plus fixes for the 2025 CVEs | [7][27] |
| 2025-06-09 | CVE-2025-27817/27818/27819 | SASL/OAuth SSRF + file read, and RCE via the JAAS LdapLoginModule and JndiLoginModule; fixed in 3.9.1 / 4.0.0 | [27] |
| 2025-09-04 | **Kafka 4.1.0** | Share groups in Preview. Streams rebalance protocol (KIP-1071) in early access. OAuth jwt-bearer (KIP-1139) | [3] |
| 2025-10-08 | librdkafka 2.12.0 | KIP-848 consumer protocol GA for non-Java clients (php-rdkafka is built on librdkafka) | [39] |
| 2025-10-13 | 4.0.1 | Bugfix | [7] |
| 2025-10-29 | Current New Orleans / Confluent Cloud Q4 | Confluent Intelligence, Streaming Agents (open preview). Queues for Kafka in early access on Dedicated clusters | [31] |
| 2025-11-12 | 4.1.1 | Bugfix | [7] |
| 2025-12-08 | IBM to acquire Confluent | $31/share cash, ~$11B EV | [29] |
| 2026-02-17 | **Kafka 4.2.0** | **Share groups GA**. RENEW ack (KIP-1222). Streams rebalance protocol GA (core features). Metrics renamed to `kafka.COMPONENT` (KIP-1100). Java 25 support | [4] |
| 2026-02-21 | 3.9.2 | 3.x bugfix; includes KIP-1252 (ZooKeeper/KRaft behaviour alignment) and CVE fixes | [7][27] |
| 2026-02-26 | Confluent Cloud Q1 2026 | Queues for Kafka GA on Enterprise and Dedicated clusters | [32] |
| 2026-03-02 | KIP-1150 accepted | Diskless topics: 9 binding +1 votes. Umbrella KIP only; no code yet | [24] |
| 2026-03-16/17 | 4.0.2 / 4.1.2 | Bugfix plus CVE fixes | [7][27] |
| 2026-03-17 | IBM closes Confluent deal | Confluent becomes an IBM subsidiary and is delisted | [30] |
| 2026-04-07 | CVE-2026-35554 | Java producer race silently sends records to the **wrong topic**. Fixed in 3.9.2 / 4.0.2 / 4.1.2 / 4.2.0 | [27] |
| 2026-04-16 | Confluent Platform 8.2 | Built on Kafka 4.2. Queues for Kafka GA on self-managed CP | [34] |
| 2026-04-17 | CVE-2026-33557 / 33558 | Missing OAUTHBEARER JWT validation (4.1.0–4.1.1). DEBUG logs leak secrets | [27] |
| 2026-05 | Current London | AI-focused launches: managed MCP server, Agent Skills, PII redaction | [38] |
| 2026-05-22 | **Kafka 4.3.0** | 25 KIPs. Share-group configs (KIP-1240). Classic-protocol hint (KIP-1274 phase 1). Tiered-storage fixes. streams-scala deprecated | [5] |
| 2026-05-30 | 4.2.1 | Bugfix (incl. a classic→streams group migration bug) | [7][8] |
| 2026-06-02 | CVE-2026-41115 | `CONSUMER_GROUP_DESCRIBE` ACL docs/KIP mismatch (4.0.0–4.3.0). Docs fixed; no code change | [28] |
| 2026-06-25 | **4.3.1** (our pin) | Bugfix. Most notable fix: Kafka Streams RocksDB native memory leak (KAFKA-20616) | [6][18] |
| 2026-06-30 | librdkafka 2.15.0 | Share consumer in **Preview** (not for production) | [39] |
| 2026-07-16 | confluent-kafka-python 2.15.0 | Share consumer Preview; needs Kafka ≥ 4.2.0 | [35] |
| 2026-08-21 | 4.4.0 RC0 | 26 KIPs. Blockers found → RC1, RC3 | [12][13] |
| 2026-09-29 | 4.2.2 released; 4.3.2 RC0 vote; 4.5.0 plan | 4.5.0 release no earlier than 2027-03-17 | [7][15][16] |
| 2026-09-30 | **4.4.0 RC3** | Vote closes 2026-10-05 09:00 PT | [13] |
| 2026-11-04/05 | Current San Francisco (upcoming) | Confluent flagship; first US Current under IBM | [36][37] |

## Kafka 4.0 (2025-03-18)

- **KRaft only.** ZooKeeper mode is removed. To upgrade from ZooKeeper: go to 3.9, migrate, then go to 4.0 [1][2].
- **Java:** clients and Streams need Java 11 or newer. Brokers, Connect and tools need Java 17 (KIP-750, KIP-1013) [1][8].
- **KIP-848 GA.** Server-side consumer rebalance with no stop-the-world. Opt in with `group.protocol=consumer` [1][21].
- **KIP-932 early access:** test only, not upgradeable [1].
- **Removed:** message formats v0/v1 and old protocol API versions (brokers must be ≥ 2.1) [1]. Log4j → Log4j2 [1].
- **Why it matters:** the "Kafka needs ZooKeeper" objection is dead. The demo's single `apache/kafka` container runs as broker and controller together.

## Kafka 4.1 (2025-09-04)

- Share groups move to **Preview**; enable with `kafka-features.sh … share.version=1` [3][8].
- KIP-1071 Streams rebalance protocol (built on KIP-848) in early access [3].
- KIP-1139: OAuth jwt-bearer grant. KIP-1092: `Consumer.close(CloseOptions)` [3].
- KIP-1109: topic metric names are no longer mangled; the old names go away in 5.0 [3].

## Kafka 4.2 (2026-02-17) — share groups GA

- **KIP-932 is production-ready** [4][8]. Adds the RENEW ack (KIP-1222) for long-running jobs, adaptive batching, and lag metrics [4].
- KIP-1071 Streams rebalance protocol GA for its core features [4][8].
- KIP-1100 renames metrics to `kafka.COMPONENT`, so JMX exporter rules need a review [4].
- Kafka Streams DLQ in exception handlers (KIP-1034). This is Streams only, **not** share groups [4].
- Java 25 support added [4].
- On clusters with fewer than 3 brokers you must set `share.coordinator.state.topic.replication.factor=1` and `…min.isr=1`. Otherwise `__share_group_state` never becomes available [8][26]. `compose.yaml` already does this.

## Kafka 4.3 (2026-05-22) + 4.3.1 (2026-06-25)

- 25 KIPs and 600+ commits [5].
- **KIP-1240:** per-group share configs `share.delivery.count.limit`, `share.partition.max.record.locks` and `share.renew.acknowledge.enable` [5][8].
- **KIP-1274 phase 1:** the consumer logs a hint when it runs the `classic` protocol [5][22].
- KIP-1237 deprecates `group.coordinator.rebalance.protocols`. In 5.0, protocols are enabled only through feature versions [5][8].
- Tiered storage: `follower.fetch.last.tiered.offset.enable` (KIP-1023), metadata-topic `min.isr` (KIP-1235) [5][8]. KIP-1066 adds broker cordoning [5].
- Deprecated for removal in 5.0: `streams-scala` (KIP-1244) and legacy MirrorMaker metrics (KIP-1280) [5].
- Broker defaults in 4.3 docs: `group.share.record.lock.duration.ms`=30000, `group.share.delivery.count.limit`=5 (range 2–10), `group.share.partition.max.record.locks`=2000, `group.share.max.size`=200 members [9].
- 4.3.1 fixes the RocksDB close-path leak (KAFKA-20616). Kafka Streams only; it does not affect the demo [6][18].

## Kafka 4.4 (in RC) / 4.5 / 5.0

- **4.4.0:** release manager Omnia Ibrahim. Code freeze was 2026-08-12 [11]. RC0 on 2026-08-21 hit blockers: KAFKA-20970 consumer busy loop, KAFKA-20982 docker-compose SASL, KAFKA-21025 Streams warm-up tasks, and the KIP-939 API revert [12][18]. RC3 posted 2026-09-30 [13].
- **4.4 contents (RC3 release notes):** share-group DLQ (KAFKA-19469 / KIP-1191). Share offsets can be initialized from an offset or a file (KAFKA-20524). KIP-1170 unifies metadata bootstrapping. `broker.id` deprecated. Docker images on **Java 25** (KAFKA-20857) [14].
- On 4.4 the latest production `share.version` is **2**, which is what turns on DLQ. A fresh 4.4 cluster gets it automatically; an upgraded cluster must bump it [10][20].
- **4.5.0:** release manager Andrew Schofield. KIP freeze 2027-01-13; release no earlier than 2027-03-17 [16][17].
- **5.0:** not scheduled (unverified). Known 5.0 items: `group.protocol` default flips to `consumer` (KIP-1274), streams-scala removed (KIP-1244), protocol enablement only via feature versions (KIP-1237) [8][22].
- No KIP found that moves **clients** to Java 17 (unverified).

## Key KIPs

| KIP | Status (2026-10-04) | One-liner | Demo relevance |
|---|---|---|---|
| **848** next-gen consumer rebalance | GA in 4.0; librdkafka GA in 2.12.0 | Broker-side assignment; incremental, no global sync barrier | PHP can opt in via librdkafka `group.protocol=consumer` (not tested with php-rdkafka). Still capped by partition count [21][39] |
| **932** Queues for Kafka | GA in 4.2 | Share groups: per-record ack (ACCEPT / RELEASE / REJECT / RENEW), acquisition locks, delivery count, more consumers than partitions | The "Kafka can't do work queues" line is outdated. No ordering, no follower fetch, no static membership [19] |
| **1191** DLQ for share groups | Accepted; shipping in 4.4 (RC) | Records that are REJECTed or hit the delivery limit are copied to a DLQ topic with `__dlq.errors.*` headers | See the DLQ details below [20] |
| **1274** retire classic consumer protocol | Accepted; phase 1 in 4.3 | 4.3 hint → 5.0 default `consumer` → 6.0 removes classic from the Java consumer. The broker keeps classic (Connect uses it) | Broker-side classic stays, so librdkafka/PHP clients on classic keep working [22] |
| **1150** diskless topics | Accepted 2026-03-02; sub-KIPs 1163/1164 in progress | Write straight to object storage and skip cross-AZ replication cost | Not shippable. Aiven runs its "Inkless" fork meanwhile [23][24] |
| **405** tiered storage | Production-ready since 3.9 (2024-11) | Old segments go to object storage; retention is no longer tied to broker disk | Still incremental fixes in 4.3 (KIP-1023, KIP-1235, KIP-1208) [2][5] |
| **1071** Streams rebalance protocol | GA (core) in 4.2; more in 4.4 | KIP-848-style protocol for Kafka Streams | Java only; not relevant to PHP [4][11] |
| **1279** cluster mirroring | Wiki says Accepted (vote thread not verified); not in 4.4 | Replication built into the broker, offset-preserving, replaces MirrorMaker 2 | Mention only if DR comes up [25] |

### KIP-1191 DLQ details (useful for the RabbitMQ DLX comparison)

- **Triggers:** an explicit REJECT, or reaching the delivery-count limit [20].
- **Opt-in on both sides.** The group config `errors.deadletterqueue.topic.name` defaults to blank, which means off. The DLQ topic itself needs `errors.deadletterqueue.group.enable=true` [20].
- DLQ topic names must start with `dlq.` by default (`errors.deadletterqueue.topic.name.prefix`) [20].
- No auto-create unless `errors.deadletterqueue.auto.create.topics.enable=true` [20].
- **The payload is not copied by default** (`errors.deadletterqueue.copy.record.enable=false`). You get a pointer record with topic, partition, offset, group, delivery count and reason [20].
- The DLQ write is asynchronous and not atomic with the state change, so duplicate DLQ records are possible [20].
- **vs RabbitMQ DLX:** Kafka gets one DLQ topic per group. There is still no exchange-style routing, TTL, or delayed retry. RabbitMQ dead-letters the full message by default.

## Industry news

- **IBM–Confluent.** Announced 2025-12-08: $31/share cash, ~$11B EV, expected close mid-2026 [29]. Closed **2026-03-17** [30]. Confluent is being integrated with watsonx.data, IBM Z, IBM MQ and webMethods [30]. IBM now owns both IBM MQ and the main Kafka vendor.
- **Confluent product cadence:**
  - Queues for Kafka: early access on Confluent Cloud in Oct 2025 [31], GA on Confluent Cloud 2026-02-26 [32], GA on Confluent Platform 8.2 on 2026-04-16 [34].
  - Confluent's own roadmap after GA: DLQ in 4.4, then EOS acks in transactions, exponential redelivery back-off, and key-based ordering [33].
- **Current (formerly Kafka Summit):**
  - New Orleans, Oct 2025: Confluent Intelligence, Real-Time Context Engine via MCP, Streaming Agents [31].
  - London, May 2026: managed MCP server, Agent Skills, PII redaction [38].
  - San Francisco is the 2026 flagship, Nov 4–5 [36][37]. Expect AI/agent announcements, not Kafka-core ones.
- **Diskless race:** three competing proposals (KIP-1150, KIP-1176, KIP-1183) converged on KIP-1150. The KIP-1183 authors dropped theirs [24].
- **Vendor framing to quote carefully:** Kai Waehner (Confluent) says don't use Queues for Kafka when you need strict ordering, exactly-once, request/reply, or AMQP/JMS/MQTT [40].

## CVEs (Apache Kafka, 2025–2026)

| CVE | Announced | Affects | Fixed | Note |
|---|---|---|---|---|
| CVE-2025-27817 | 2025-06-09 | 3.1.0–3.9.0 | 3.9.1, 4.0.0 | Arbitrary file read / SSRF via OAuth endpoint URL configs. 4.0 adds an allow-list that is empty by default [27] |
| CVE-2025-27818 | 2025-06-09 | 2.3.0–3.9.0 | 3.9.1, 4.0.0 | RCE via SASL JAAS `LdapLoginModule` (Connect) [27] |
| CVE-2025-27819 | 2025-06-09 | 2.0.0–3.3.2 | 3.9.1, 4.0.0 | RCE/DoS via `JndiLoginModule` [27] |
| CVE-2026-35554 | 2026-04-07 | Java kafka-clients 2.8.0–4.1.1 | 3.9.2, 4.0.2, 4.1.2, 4.2.0 | Producer buffer-pool race sends records to the **wrong topic** [27]. Java client only; librdkafka/PHP presumably unaffected (inference) |
| CVE-2026-33557 | 2026-04-17 | 4.1.0–4.1.1 | 4.1.2, 4.2.0 | OAUTHBEARER accepts any JWT [27] |
| CVE-2026-33558 | 2026-04-17 | kafka-clients 0.11.0–3.9.1, 4.0.0 | 3.9.2, 4.0.1, 4.1.0 | DEBUG logs leak credentials [27] |
| CVE-2026-41115 | 2026-06-02 | 4.0.0–4.3.0 | docs only | `CONSUMER_GROUP_DESCRIBE` checks DESCRIBE, not READ as documented. Code is correct; audit ACLs [28] |

- No Apache Kafka CVE found for Jul–Sep 2026 (searched 2026-10-04).

## Demo talking points

1. **"Kafka has queues now — since February."** 4.2 share groups GA. Show the path EA (4.0) → Preview (4.1) → GA (4.2), and admit it's real [4][8].
2. **"…and DLQs in about two weeks."** 4.4 RC3 is in vote. The DLQ is opt-in, needs a `dlq.`-prefixed topic, and by default stores **metadata, not the message** [13][20]. Compare: RabbitMQ DLX routes the full message through an exchange.
3. **"But not from PHP yet."** Share consumer: Java GA, librdkafka Preview (2.15.0), no php-rdkafka binding [33][39]. Confluent targets non-Java GA for H2 2026, so re-check the week of the talk.
4. **"The new rebalance protocol doesn't save your worker count."** KIP-848 removes stop-the-world rebalances, and PHP can use it through librdkafka ≥ 2.12. Workers beyond the partition count still sit idle (Demo B) [21][39].
5. **"No ZooKeeper, one container."** Kafka 4.x is KRaft only. The compose file runs broker and controller in one JVM [1]. The fair comparison is RabbitMQ 4.x's Khepri (covered in docs/01).
6. **"Diskless is coming — eventually."** KIP-1150 is accepted, but implementation KIPs are still open and nothing has shipped [23][24]. Don't let anyone claim Kafka is S3-native today.
7. **"Kafka's main vendor is now IBM."** Neutral fact (closed 2026-03-17). Apache Kafka governance is unchanged [30].
8. **Upgrade hygiene slide:** CVE-2026-35554 (silent wrong-topic delivery) shows why to keep Java clients patched. 4.3.1 is clean [27].

## Sources

1. Apache Kafka 4.0.0 Release Announcement — Apache Kafka — 2025-03-18 — https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/
2. Apache Kafka 3.9.0 Release Announcement — Apache Kafka — 2024-11-06 — https://kafka.apache.org/blog/2024/11/06/apache-kafka-3.9.0-release-announcement/
3. Apache Kafka 4.1.0 Release Announcement — Apache Kafka — 2025-09-04 — https://kafka.apache.org/blog/2025/09/04/apache-kafka-4.1.0-release-announcement/
4. Apache Kafka 4.2.0 Release Announcement — Apache Kafka — 2026-02-17 — https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/
5. Apache Kafka 4.3.0 Release Announcement — Apache Kafka — 2026-05-22 — https://kafka.apache.org/blog/2026/05/22/apache-kafka-4.3.0-release-announcement/
6. Apache Kafka 4.3.1 Release Announcement — Apache Kafka — 2026-06-25 — https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/
7. Apache Kafka blog index (patch releases 3.9.1–4.2.2) — Apache Kafka — accessed 2026-10-04 — https://kafka.apache.org/blog
8. Kafka 4.3 Upgrade Guide (notable changes 4.0–4.3) — Apache Kafka — accessed 2026-10-04 — https://kafka.apache.org/43/getting-started/upgrade/
9. Kafka 4.3 Broker Configs — Apache Kafka — accessed 2026-10-04 — https://kafka.apache.org/43/configuration/broker-configs/
10. `ShareVersion.java` at tags 4.3.1 / 4.4.0-rc3 — apache/kafka GitHub — 2026-06 / 2026-09-30 — https://github.com/apache/kafka/blob/4.3.1/server-common/src/main/java/org/apache/kafka/server/common/ShareVersion.java
11. Release Plan 4.4.0 — Apache Kafka wiki — accessed 2026-10-04 — https://cwiki.apache.org/confluence/spaces/KAFKA/pages/429064575/Release+Plan+4.4.0
12. Re: [VOTE] 4.4.0 RC0 (Justine Olshan) — dev@kafka.apache.org — 2026-09-03 — http://www.mail-archive.com/dev@kafka.apache.org/msg158387.html
13. [VOTE] 4.4.0 RC3 (Omnia Ibrahim) — dev@kafka.apache.org — 2026-09-30 — https://www.mail-archive.com/dev@kafka.apache.org/msg158838.html
14. Kafka 4.4.0-rc3 RELEASE_NOTES — Apache dist (staging) — 2026-09-30 — https://dist.apache.org/repos/dist/dev/kafka/4.4.0-rc3/RELEASE_NOTES.html
15. [VOTE] 4.3.2 RC0 — dev@kafka.apache.org — 2026-09-29 — https://www.mail-archive.com/dev@kafka.apache.org/msg158811.html
16. Re: [DISCUSS] Apache Kafka 4.5.0 release (Andrew Schofield) — dev@kafka.apache.org — 2026-09-29 — https://www.mail-archive.com/dev@kafka.apache.org/msg158807.html
17. Re: [DISCUSS] Apache Kafka 4.5.0 release (Lucas Brutschy) — dev@kafka.apache.org — 2026-09-24 — https://www.mail-archive.com/dev@kafka.apache.org/msg158701.html
18. ASF JIRA KAFKA-20970 / KAFKA-20982 / KAFKA-21025 / KAFKA-20616 — Apache JIRA — 2026-05-26 → 2026-09-05 — https://issues.apache.org/jira/browse/KAFKA-20970 (swap key for others)
19. KIP-932: Queues for Kafka — Apache Kafka wiki — accessed 2026-10-04 — https://cwiki.apache.org/confluence/display/KAFKA/KIP-932:+Queues+for+Kafka
20. KIP-1191: Dead-Letter Queues for Share Groups — Apache Kafka wiki — accessed 2026-10-04 — https://cwiki.apache.org/confluence/display/KAFKA/KIP-1191:+Dead-Letter+Queues+for+Share+Groups
21. KIP-848: The Next Generation of the Consumer Rebalance Protocol — Apache Kafka wiki — accessed 2026-10-04 — https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
22. KIP-1274: Deprecate and remove support for Classic rebalance protocol in KafkaConsumer — Apache Kafka wiki — accessed 2026-10-04 — https://cwiki.apache.org/confluence/display/KAFKA/KIP-1274%3A+Deprecate+and+remove+support+for+Classic+rebalance+protocol+in+KafkaConsumer
23. KIP-1150: Diskless Topics — Apache Kafka wiki — accessed 2026-10-04 — https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics
24. KIP-1150 Accepted, and the Road Ahead (Josep Prat) — Aiven — 2026-03-12 — https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead
25. KIP-1279: Cluster Mirroring — Apache Kafka wiki — accessed 2026-10-04 — https://cwiki.apache.org/confluence/display/KAFKA/KIP-1279:+Cluster+Mirroring
26. Kafka 4.2 Upgrade Guide — Apache Kafka — accessed 2026-10-04 — https://kafka.apache.org/42/getting-started/upgrade/
27. Apache Kafka CVE List — Apache Kafka — accessed 2026-10-04 — https://kafka.apache.org/community/cve-list/
28. CVE-2026-41115: Improper Authorization in CONSUMER_GROUP_DESCRIBE API (Luke Chen) — oss-security — 2026-06-02 — https://www.openwall.com/lists/oss-security/2026/06/02/5
29. IBM to Acquire Confluent — Confluent press release — 2025-12-08 — https://www.confluent.io/press-release/ibm-to-acquire-confluent-to-create-smart-data-platform-for-enterprise/
30. IBM Completes Acquisition of Confluent — IBM Newsroom / Confluent — 2026-03-17 — https://newsroom.ibm.com/2026-03-17-ibm-completes-acquisition-of-confluent,-making-real-time-data-the-engine-of-enterprise-ai-and-agents (mirror: https://www.confluent.io/press-release/ibm-completes-acquisition-of-confluent/)
31. New in Confluent Cloud: Data in Motion for AI in Action (Q4 2025 launch) — Confluent — 2025-10-29 — https://www.confluent.io/blog/2025-q4-confluent-cloud-launch/
32. Confluent Cloud Q1 2026: Queues for Kafka, KCP for migration — Confluent — 2026-02-26 — https://www.confluent.io/blog/2026-q1-confluent-cloud-launch/
33. Queues for Apache Kafka Is Here (Share Consumer GA) — Confluent — 2026-03-03 — https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/
34. Introducing Confluent Platform 8.2 — Confluent — 2026-04-16 — https://www.confluent.io/blog/introducing-confluent-platform-8-2/
35. Confluent Developer Newsletter: Kafka 4.3 / Queues for Kafka comes to Python clients — Confluent — 2026-07-16 — https://developer.confluent.io/newsletter/current-san-francisco-updates-queues-for-kafka-comes-to-python-clients/
36. The Future of Current (Tim Berglund) — Confluent — 2026-01-13 — https://www.confluent.io/blog/future-of-current-data-streaming-community/
37. Current San Francisco 2026 event page — Confluent — accessed 2026-10-04 — https://current.confluent.io/san-francisco
38. Confluent Current London 2026 — AI requires a data re-think (Mark Chillingworth) — diginomica — 2026-05-20 — https://diginomica.com/confluent-current-london-2026-ai-requires-data-rethink
39. librdkafka CHANGELOG + GitHub releases (2.12.0 2025-10-08, 2.15.0 2026-06-30) — Confluent / GitHub — accessed 2026-10-04 — https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md
40. When (Not) to Use Queues for Kafka (Kai Waehner) — kai-waehner.de — 2026-01-28 — https://www.kai-waehner.de/blog/2026/01/28/when-to-use-queues-for-kafka/
