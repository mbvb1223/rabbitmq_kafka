# Ecosystem, managed offerings, cost, licensing & alternatives — 2025 → Oct 2026

As of 2026-10-04. Demo pins: `rabbitmq:4.3.6-management`, `apache/kafka:4.3.1`, `kafbat/kafka-ui:v1.5.0`. `[n]` = entry in Sources.
Prices: USD list prices, us-east-1 / East US unless noted, 730 h/month, **as checked 2026-10, approximate**. Vendor claims are marked as such.

## TL;DR

- **Both "default" vendors now belong to large enterprise owners.** IBM closed the Confluent deal on 2026-03-17 (~$11B EV) [1], and WarpStream came with it [4]. Jay Kreps stepped back as CEO on 2026-08-13 [3]. Broadcom owns RabbitMQ through Tanzu. Apache Kafka (ASF, Apache 2.0) and RabbitMQ (MPL 2.0) keep their licences. The changes are in the commercial layer.
- **Free RabbitMQ means upgrading all the time.** Only the newest minor series gets community patches. 4.3's community support ends **2026-11-30**, and 4.2's ended 2026-07-31 [18]. Patched builds of older series, such as 3.13.20 released 2026-09-15, go to paying customers only [22]. Some new features ship only in Tanzu: delayed queues / message scheduler, Stream Browser, Linter, JMS queue type and warm standby [17][20].
- **PHP gotcha: `x-delayed-message` is dead on 4.3.** The plugin repo is archived and the plugin depended on Mnesia, which 4.3 removed. The open-source replacement is native delivery delay in quorum queues, arriving in **4.4**, which is not released yet [21][26]. Until then, use TTL+DLX or quorum-queue delayed retries (4.3), or buy Tanzu.
- **Diskless Kafka is 2025-26's big story, but it doesn't affect us yet.** KIP-1150 was accepted on 2026-03-02 and is in no Apache release [28]. Vendors already sell diskless products: Confluent Freight, WarpStream, AutoMQ, Aiven, Bufstream (now at CoreWeave) and StreamNative Ursa [29]. They trade roughly 0.5–2 s of latency for cheaper cross-AZ traffic. That only pays off at tens of MB/s.
- **Smallest HA production floor:** RabbitMQ costs **~$150–300/mo** (CloudAMQP, Amazon MQ). Kafka costs **~$145–640/mo** (Redpanda Serverless, Aiven, MSK, Confluent Standard), or ~$900 on MSK Express. Cloud-native queues cost **~$30/mo** at 100 msg/s. See the worked example.
- **Consolidation and churn:**
  - Upstash Kafka shut down on 2025-03-11 [48].
  - The free Bitnami images went away in Aug–Sep 2025 [27].
  - Redpanda bought Oxla in Oct 2025 [35].
  - CoreWeave bought Bufstream on 2026-05-08 [36].
  - Synadia tried to take NATS out of the CNCF in Apr 2025 and backed down within a week [40].

## 1. Managed services

### 1a. RabbitMQ / AMQP side

| Service | Small HA prod (approx, list) | Cheapest non-HA / free | 2025-26 news | Src |
|---|---|---|---|---|
| **Amazon MQ for RabbitMQ** | 3-node `mq.m7g.medium` cluster: $0.41/h ≈ **$299/mo**. `m7g.large` cluster: $0.8201/h ≈ $599/mo. Storage $0.10/GB-mo | Single `m7g.medium` ≈ $100/mo. `t3.micro` $0.027/h ≈ $20/mo (3.13 only; 4.x needs m7g) | 4.2 on 2025-11-20 (first 4.x; local shovels, QQ priorities). In-place 3.13→4.2 upgrade on 2026-05-05. Prometheus metrics in 2026-04. **4.3 on 2026-09-10** | [14][15] |
| **CloudAMQP** (84codes) | RabbitMQ dedicated 3-node "Big Bunny" **$297/mo** (~1k msg/s). LavinMQ 3-node "Passionate Puffin" **$147/mo** | Free "Little Lemur" (1M msgs/mo). Shared "Tough Tiger" $19/mo | Sold through the AWS, Azure and GCP marketplaces, so it bills to your cloud account. Per-second billing | [16] |
| **Tanzu RabbitMQ** (Broadcom) | Quote only. Core-based licence. A reseller reports a **72-core minimum order since 2025-04-10** (unverified; another reseller page says 50) | n/a | Tanzu 4.3.6 on 2026-09-15. "TrueSource Data Services" (trusted RabbitMQ/Postgres builds) announced 2026-08-31 | [22][23][24] |
| **Azure** | No first-party RabbitMQ. Options: CloudAMQP through the Marketplace, or self-host. The native equivalent is **Service Bus**: Standard $10/mo base + ops ($0.80/M from 13M→100M, then $0.50/M). Premium $0.9275/MU-h ≈ $677/mo | Service Bus Basic $0.05/M ops | — | [16][47] |
| **GCP** | No first-party RabbitMQ. CloudAMQP is on the GCP Marketplace. The native alternative is Pub/Sub (see §3) | — | — | [16][45] |
| Instaclustr (NetApp) | No managed RabbitMQ (Kafka only) | — | — | [49] |

### 1b. Kafka side

| Service | Model | Small HA prod (approx, list) | 2025-26 news | Src |
|---|---|---|---|---|
| **MSK Provisioned (Standard)** | Broker-hours + provisioned EBS | 3× `kafka.m7g.large` $0.204/h ≈ **$447/mo** + $0.10/GB-mo. Dev: 3× `t3.small` ≈ $100/mo | Kafka 4.1 on 2025-10-15 (queues *preview*). Tiered storage $0.06/GB-mo | [12][13] |
| **MSK Express** | Broker-hours + $0.01/GB in + $0.10/GB-mo used | 3× `express.m7g.large` $0.408/h ≈ **$894/mo** | KRaft/3.9 in 2025-12. **Kafka 4.2 on 2026-07-15** (Express only). Replicator from external Kafka in 2026-04. Native S3 delivery + Iceberg tables on 2026-07-30 | [12][13] |
| **MSK Serverless** | Cluster-h + partition-h + GB | $0.75/h ≈ **$548/mo** base + $0.0015/partition-h + $0.10/GB in, $0.05/GB out | — | [12] |
| **Confluent Cloud** (IBM) | eCKU-h + GB + storage | **Basic**: first eCKU free, then $0.14/h, $0.05/GB in/out, 99.5% SLA. **Standard**: $0.75/eCKU-h, listed "from ~$385/mo" (1 eCKU running 24×7 would be ~$548; check the calculator), 99.9%/99.99% SLA. **Enterprise**: $1.75–2.25/eCKU-h, from ~$895. **Freight**: $2.25/eCKU-h, minimum 2 eCKU, ≈ $2,300/mo, storage $0.03/GB-mo. $400 credit for the first 30 days | Freight GA on 2025-02-03 (latency "up to a second or two"). Tableflow GA (Iceberg) in 2025-03; Delta Lake/Unity on 2025-10-29; $0.10/topic-h + $0.04/GB per a Confluent blog (unverified on the pricing page). Private Cloud (on-prem) on 2025-10-29. IBM close 2026-03-17. **No public price change since the IBM close** | [5][6][7][8][9] |
| **WarpStream** (IBM/Confluent, BYOC) | Cluster tier + $0.01/GiB *uncompressed* write + $0.01/GiB storage; **you also pay for the agents' VMs and S3** | Dev $100/mo (no SLA). **Fundamentals $500/mo (99.9%)**. Pro $1,500 (99.99%) | Kreps said in 2024 that WarpStream would develop "in its own way". Its post-IBM roadmap is unannounced | [4][32] |
| **Aiven for Apache Kafka** | **New usage pricing from 2026-08-26**: compute from $235/mo + $0.10/GB-mo + network (classic $0.02 in / $0.04 out per GB; diskless $0.01 / $0.03) | Aiven's own example: 1 MB/s in, 3 MB/s out, 3-day retention ≈ **$620/mo**. Free plan and dev tier unchanged | Inkless (diskless fork) on Aiven Cloud from 2026-02-26. Diskless topics are now a standard feature (classic and diskless can be mixed on plans from 10 MB/s) | [30][31] |
| **Redpanda Cloud** | Serverless: $0.10/h (~$73/mo) + $0.045/GB in + $0.04/GB out + $0.0015/partition-h + $0.09/GB-mo (rates from 2025-03-25) | Serverless **GA on AWS 2026-02-03**, 99.9% SLA, 30-day trial with $100 credit. BYOC/Dedicated need an annual commitment (quote) | Shadowing, a Confluent→Redpanda migration tool, in 2026-08. Oxla SQL engine acquired 2025-10 | [33][35] |
| **Google Managed Service for Apache Kafka** | vCPU/RAM-h + storage + inter-zone | From $0.09/vCPU-h. Local SSD $0.17/GiB-mo, GCS $0.10/GiB-mo, inter-zone $0.01/GiB. Google's example: 10 MiB/s ≈ $1.1K/mo. CUD discounts 20% (1 yr) / 40% (3 yr) | Figures come from Google's pricing page via a search excerpt; the page didn't render for fetch | [46] |
| **Azure Event Hubs** (Kafka endpoint) | Throughput units | Standard $0.03/TU-h (≈ $22/mo per TU = 1 MB/s in, 2 MB/s out, 1k events/s) + $0.028/M events. Premium $1.027/PU-h ≈ $750/mo. The retail API also lists a "Standard Kafka Endpoint" meter at $0.09/h; whether it is billed is **unverified** | — | [47] |
| Instaclustr (NetApp) | Consumption-based, no public list price | — | Kafka 4.3.1 GA. Tiered storage on GCP in preview (2026-03). Multi-region BYOC (2026-09-22) | [49] |
| ~~Upstash Kafka~~ | **Discontinued 2025-03-11.** Upstash now offers QStash/Workflow (HTTP) instead | — | — | [48] |

### 1c. Worked example: "typical PHP app" monthly cost

Assumptions: 100 msg/s, 1 KB messages. That gives ≈ 263 GB/mo in and 2 consumer groups (≈ 526 GB/mo out), with 7-day retention (≈ 60 GB) and ~30 partitions where they're billed. Network to clients and support plans are excluded. Rough figures, approximate.

| Option | ≈ $/mo | Note |
|---|---|---|
| SQS standard (batched ×10) | ~$31 | ~79M requests × $0.40/M. Unbatched ≈ $315. Fan-out to 2 consumers needs SNS → 2 queues (extra cost) [44] |
| Google Pub/Sub | ~$28 | 0.71 TiB × $40 (10 GiB free) [45] |
| Azure Event Hubs Std, 1 TU | ~$29 | +$66 if the Kafka-endpoint meter applies (unverified) [47] |
| Confluent Basic | ~$44 | Single eCKU free; 99.5% SLA, so it's not an HA-grade SLA [5] |
| Redpanda Serverless | ~$144 | $73 base + $33 partitions + $33 traffic + $5 storage [33] |
| CloudAMQP LavinMQ 3-node | $147 | Flat [16] |
| Aiven Kafka (new pricing) | ~$267 | $235 compute + $26 network + $6 storage [30] |
| CloudAMQP RabbitMQ 3-node | $297 | Flat, ~1k msg/s plan [16] |
| Amazon MQ RabbitMQ, 3× m7g.medium | ~$300 | [14] |
| MSK Standard, 3× m7g.large | ~$477 | Includes 3×100 GB provisioned EBS [12] |
| WarpStream Fundamentals | ~$503 + your VMs/S3 | Agent compute not included [32] |
| Confluent Standard (1 eCKU) | ~$385–590 | Listed floor vs. a 24×7 eCKU calculation [5] |
| MSK Serverless | ~$639 | [12] |
| MSK Express, 3× large | ~$902 | [12] |

At this scale, **Kafka's fixed floor dominates the bill**. Diskless and Freight discounts start to matter only at tens of MB/s.

## 2. Licensing & governance

| Project | Licence / steward | Commercial-only or restricted bits | 2025-26 changes | Reaction / risk | Src |
|---|---|---|---|---|---|
| **Apache Kafka** | Apache 2.0, ASF | None in the project | KIP-1150 accepted. Kafka 4.x is KRaft-only | Factor House (sells Kpow): the risk sits in Confluent's commercial layer, not in Kafka. It reports up to ~800 Confluent layoffs after the close; IBM hasn't confirmed | [2][28] |
| **Confluent Platform** | Schema Registry, ksqlDB, REST Proxy, Admin REST and CLI are under the **Confluent Community License**. That licence is source-available, not OSI-approved, and bans running these as a competing SaaS. Security plugins are under the Enterprise licence | Enterprise licence features. Many connectors | **From CP 8.0, the free Community edition gets ~1 year of patches; Enterprise gets up to 3 years.** CP 8.3 (Kafka 4.3) shipped 2026-06-17. No licence change since IBM | Commentators expect "IBM-ification" of pricing and contracts. This is speculation, with no announcement so far | [2][10][11] |
| **RabbitMQ** | MPL 2.0. Broadcom (Tanzu) is the maintainer | Warm Standby Replication, intra-cluster compression, FIPS TLS, OAuth forward proxy, distributed shovels, AMQP 1.0 over WebSockets, audit logging, Stream Browser, Linter, JMS queue type, Spark connector, **Delayed Queues / Message Scheduler**, Schema Definition Sync | Since 2024-06, community support covers only the latest minor (~7 months per series). Older patched binaries and LTS are paid-only. Delayed-message plugin archived (last push 2026-09-24). TrueSource launched 2026-08 | CVE-2026-57219 (CVSS 8.7, 2026-07): fixes for 3.13/4.0/4.1 shipped as paid-only builds, so free users had to upgrade. HN users voiced worries at the time of the Broadcom deal | [17][18][19][20][21][22][25] |
| **Redpanda** | Core under **BSL 1.1** (no competing streaming SaaS; converts to Apache 2.0 after 4 years). Enterprise features under the Redpanda Community License | Tiered storage, audit logging, continuous balancing, Console RBAC/SSO, enterprise connectors | Since 24.3, clusters get a 30-day trial, after which enterprise features are restricted and upgrades are blocked while they're in use. No 2025-26 licence change found | Connect (formerly Benthos) moved from MIT to an Apache 2.0 / RCL mix, and users complained it was unclear | [34] |
| **NATS** | Apache 2.0, CNCF | Synadia commercial add-ons | Apr 2025: Synadia tried to reclaim the trademark and move to **BSL**. 2025-05-01 settlement: the trademarks went to the Linux Foundation and NATS stays Apache 2.0 in the CNCF | Raised the question of whether a project can "exit" a foundation | [40] |
| **Apache Pulsar** | Apache 2.0, ASF | StreamNative cloud | The main vendor, StreamNative, now also sells a Kafka service (Ursa for Kafka, 2026-04-07). It promised to open-source Ursa; that hadn't happened as of Apr 2026 | — | [38][39] |
| **AutoMQ** | Apache 2.0 (vendor says no BSL or SSPL) | Managed service | Independent | Vendor-only performance claims | [37] |
| **LavinMQ** | Apache 2.0, 84codes | CloudAMQP hosting | 2.9.x releases through 2026-09 | — | [43] |
| **Bitnami images** (Broadcom) | Source stays Apache 2.0 | Versioned images now need Bitnami Secure Images (paid) | 2025-08-28 → 09-29: `docker.io/bitnami` was emptied. Old tags moved to `bitnamilegacy` (no updates) | Mass `ImagePullBackOff`. **Our compose uses the official images, so it isn't affected** | [27] |

## 3. Kafka-compatible, diskless & other alternatives

| Option | What it is | Status (2025-26) | Pick it instead when… | Src |
|---|---|---|---|---|
| **KIP-1150 diskless (upstream)** | Topics written directly to object storage; broker disks act as cache | Accepted 2026-03-02 (9 binding votes). The core KIPs, 1163 and 1164, are still being discussed. **Not in 4.3** | Not yet. Use tiered storage (KIP-405) today | [28][29] |
| **Redpanda** | C++ single binary, Kafka API, no JVM | Serverless GA. Shadowing migration tool. Pivoting to AI with an "Agentic Data Plane" | You want lighter self-hosting or a cheap serverless Kafka API and can accept BSL | [33][34][35] |
| **WarpStream** | Stateless agents over S3, BYOC | IBM-owned | High-volume logs/telemetry kept in your own VPC, where ~0.5–1 s latency is OK | [4][32] |
| **Confluent Freight** | Kora engine with direct writes to S3 | GA on AWS 2025-02 | You're already on Confluent and have GB/s-scale logging | [6] |
| **AutoMQ** | Kafka fork with an S3 storage layer + WAL | ~10.9k GitHub stars; 10K reached 2026-06 | You want open-source (Apache 2.0) diskless Kafka today | [37] |
| **Aiven Inkless / diskless topics** | Apache 2.0 fork that tracks KIP-1150 | On Aiven Cloud and BYOC. Inkless 0.49 (2026-09-24) builds on Kafka 4.1/4.2 | You want diskless on a path that converges with upstream | [30][31] |
| **Bufstream** | Kafka-compatible, Protobuf-aware broker | **Sold to CoreWeave on 2026-05-08** for internal use. Buf's post says nothing about existing customers | Avoid for new projects | [36] |
| **StreamNative Ursa / Pulsar** | Ursa: each topic stored as an Iceberg/Delta table. Pulsar: native multi-tenancy and geo-replication | UFK in limited public preview (2026-04). Pulsar 4.0 LTS active support ends 2026-10-21 (security support to 2027-10-21). 5.0 milestones (M2 on 2026-09-12) | Your stream should *be* a lakehouse table, or you need Pulsar-style tenancy. No official PHP client (unverified) | [38][39] |
| **NATS JetStream** | Lightweight Go server: pub/sub, request-reply, streams, KV | 2.12 (2025-09): atomic batches, counters, delayed scheduling. 2.14 (2026): cron schedules. **Jepsen 2025-12-08 (2.12.1): lost acknowledged writes under file corruption. Default fsync runs every 2 min** | Edge/IoT, request-reply, low ops. Tune `sync` if durability matters | [41][42] |
| **LavinMQ** | AMQP 0-9-1 wire-compatible (php-amqplib works), Crystal, streams, MQTT, etcd-based HA | 2.9.3 (2026-09-09). No AMQP 1.0 | You want RabbitMQ semantics with a smaller footprint and no Broadcom dependency. Half the CloudAMQP price | [16][43] |
| **SQS / SNS** | Serverless queues and fan-out | Fair queues 2025-07-21 (+$0.10/M requests). 1 MiB messages 2025-08-04 | You're on AWS and run simple job queues. Laravel and Symfony ship SQS transports. No replay | [44][57] |
| **Google Pub/Sub** | Serverless pub/sub | $40/TiB. Pub/Sub Lite reportedly turned down (unverified) | You're on GCP and want fan-out or HTTP push | [45] |
| **Azure Service Bus / Event Hubs** | Service Bus is RabbitMQ-like (AMQP 1.0, sessions). Event Hubs has a Kafka endpoint | Prices unchanged since 2014–2021 (retail API effective dates) | You're Azure-only and want zero ops | [47] |

## 4. Tooling

| Area | Kafka | RabbitMQ | Src |
|---|---|---|---|
| **Web UI** | No UI built in. **kafbat kafka-ui**: Apache 2.0, fork of provectus. **v1.5.0 (2026-04-20) is the latest stable** (our pin). MCP support since 1.3.0. Factor House (a competitor) counts 20+ high/critical advisories in bundled libraries since then. **AKHQ** 0.28.0 (2026-08-06), Apache 2.0, opt-in audit log. **Conduktor** Console Community is free (pricing page: 50 users / 3 clusters), Team costs $1,200/seat/yr, Desktop has been sunset, and Feb 2026 (v1.43) changes narrowed the free tier per a competitor. **Redpanda Console**: RBAC/SSO need an enterprise licence | **Management plugin is built in** (port 15672). Management-UI stats are deprecated, but a maintainer says they will stay through 4.x (2025-12). 4.3 shows per-priority quorum-queue counts. **Tanzu-only:** Stream Browser and Linter | [20][50][51][52][53] |
| **Metrics** | JMX → Prometheus JMX exporter. Consumer lag via kafka_exporter. Kafka 4.2 renamed metrics (KIP-1100, see 02-kafka-news) | `rabbitmq_prometheus` on **15692**. Official Grafana dashboards (Overview **ID 10991**, Raft, Erlang distribution). **4.2 renamed the Raft metrics**, so update dashboards. Amazon MQ enables Prometheus by default on 4.2+ | [15][53][56] |
| **Schema registry** | Confluent SR (Community License). **Karapace** (Aiven, Apache 2.0, drop-in replacement). **Apicurio Registry 3.x** (Apache 2.0, CNCF Sandbox). Buf Schema Registry (Protobuf; Buf kept it after the Bufstream sale). Azure Event Hubs SR | **None built in.** Message contracts live in app code. Tanzu "Schema Definition Sync" syncs *topology*, not payload schemas | [11][36][54] |
| **Integration** | **Kafka Connect**: the framework is Apache 2.0, but each connector has its own licence (many Confluent connectors are Community or commercial). MSK Express now writes to S3/Iceberg natively (2026-07), so you don't need a sink connector | **Shovel**: one-way, unconditional move. 4.2 added *local* shovels, which run inside the cluster with no TCP connection. **Federation**: moves messages only when the upstream has no local consumers; can run in both directions. **Distributed shovels are Tanzu-only** | [13][17][55] |

## Demo talking points

- **"Pick a protocol, not a vendor."** The Kafka protocol has many independent implementations: Apache Kafka, Confluent Kora, Redpanda, WarpStream, AutoMQ, Aiven Inkless, Event Hubs, Ursa… AMQP 0-9-1 has RabbitMQ and LavinMQ. Both "reference" vendors now belong to large enterprise owners (IBM, Broadcom).
- **Free RabbitMQ has a clock.** Our 4.3.6 pin loses community patches on **2026-11-30**. Budget for a minor upgrade every ~6 months, or for Tanzu support.
- **Check your delayed-message usage today.** `x-delayed-message` won't load on 4.3. Look at what your PHP library does. Symfony Messenger's AMQP transport uses TTL+DLX delay queues (verify against your version). The proper open-source fix is quorum-queue delay in 4.4.
- **Cost slide:** at 100 msg/s, SQS, Pub/Sub or Event Hubs cost ~$30. Managed RabbitMQ runs $150–300. Managed Kafka runs $145–900. Diskless pricing only wins at tens of MB/s.
- **Diskless is the Kafka headline of 2026, and it still isn't in Apache Kafka.** It trades latency for cost. For a request-path PHP app, it's irrelevant.
- **Lock-in is in the extras.** Schema Registry (Community License), Confluent connectors, and Tanzu-only RabbitMQ features carry the lock-in. Apache-licensed swaps exist: Karapace/Apicurio, kafbat/AKHQ, and Prometheus + Grafana.
- **"RabbitMQ ships a UI; Kafka needs a third-party one."** Show the 15672 and 8081 (kafbat) tabs side by side.
- **Bitnami break (Sep 2025):** if your team's compose files use `bitnami/kafka` or `bitnami/rabbitmq`, switch to the official images, as our `compose.yaml` does.

## Sources

1. IBM Newsroom — "IBM Completes Acquisition of Confluent", 2026-03-17 — https://newsroom.ibm.com/2026-03-17-ibm-completes-acquisition-of-confluent,-making-real-time-data-the-engine-of-enterprise-ai-and-agents
2. Factor House (vendor: Kpow) — "What the IBM-Confluent deal means for Kafka users", 2026-03-26, upd. 2026-09-23 — https://factorhouse.io/articles/what-the-ibm-confluent-acquisition-means-for-kafka-users/
3. Jay Kreps on X, 2026-08-13 — https://x.com/jaykreps/status/2087954153582780901 · SDxCentral, 2026-08 — https://www.sdxcentral.com/news/ibm-in-new-software-blow-as-confluent-ceo-exits/
4. Confluent — "Confluent acquires WarpStream", 2024-09-09 — https://www.confluent.io/blog/confluent-acquires-warpstream/
5. Confluent Cloud pricing page, checked 2026-10-04 — https://www.confluent.io/confluent-cloud/pricing/
6. Confluent — "Freight clusters are generally available", 2025-02-03 — https://www.confluent.io/blog/freight-clusters-are-generally-available/
7. Confluent Tableflow billing docs, checked 2026-10 — https://docs.confluent.io/cloud/current/topics/tableflow/concepts/tableflow-billing.html · Tableflow GA press release, 2025-03-18 — https://www.confluent.io/press-release/confluent-announces-tableflow-general-availability/ · Delta/Unity GA, 2025-10-29 — https://www.confluent.io/press-release/tableflow-powers-real-time-ai-across-clouds/
8. Confluent — "Confluent Launches Confluent Private Cloud", 2025-10-29 — https://investors.confluent.io/news-releases/news-release-details/confluent-launches-confluent-private-cloud-bringing-cloud
9. Confluent Cloud billing FAQ ($400 / 30 days), checked 2026-10 — https://docs.confluent.io/cloud/current/billing/billing-faq.html
10. Confluent Platform versions & interoperability (CP 8.0 support policy, CP 8.3 2026-06-17), checked 2026-10 — https://docs.confluent.io/platform/current/installation/versions-interoperability.html
11. Confluent Platform licenses — https://docs.confluent.io/platform/current/installation/license.html · Confluent Community License FAQ — https://www.confluent.io/confluent-community-license-faq/
12. AWS MSK pricing — https://aws.amazon.com/msk/pricing/ · AWS Price List API, AmazonMSK us-east-1 (publication 2026-09-11) — https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonMSK/current/us-east-1/index.json
13. AWS What's New — MSK Kafka 4.1, 2025-10-15 — https://aws.amazon.com/about-aws/whats-new/2025/10/amazon-msk-apache-kafka-version-4-1/ · Express KRaft/3.9, 2025-12 — https://aws.amazon.com/about-aws/whats-new/2025/12/aws-msk-express-brokers-support-kraft · Express Kafka 4.2, 2026-07-15 — https://aws.amazon.com/about-aws/whats-new/2026/07/aws-msk-express-version-42/ · Express → S3, 2026-07-30 — https://aws.amazon.com/about-aws/whats-new/2026/07/aws-msk-express-brokers-delivers-to-amazon-s3/ · Iceberg tables, 2026-07 — https://aws.amazon.com/about-aws/whats-new/2026/07/aws-msk-streaming-tables-for-apache-iceberg/ · Replicator from external Kafka, 2026-04 — https://aws.amazon.com/about-aws/whats-new/2026/04/amazon-msk-replicator-external-kafka-cluster-support/
14. Amazon MQ pricing — https://aws.amazon.com/amazon-mq/pricing/ · AWS Price List API, AmazonMQ us-east-1 (publication 2026-09-11) — https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonMQ/current/us-east-1/index.json
15. AWS What's New — Amazon MQ RabbitMQ 4.2, 2025-11-20 — https://aws.amazon.com/about-aws/whats-new/2025/11/amazon-mq-rabbitmq-42 · In-place upgrades to 4, 2026-05-05 — https://aws.amazon.com/about-aws/whats-new/2026/05/amazon-mq-inplace-upgrades-rabbitmq4/ · RabbitMQ 4.3, 2026-09-10 — https://aws.amazon.com/about-aws/whats-new/2026/09/amazon-mq-rabbitmq-43/ · Prometheus metrics, 2026-04 — https://aws.amazon.com/about-aws/whats-new/2026/04/amazon-mq-rabbitmq-prometheus-metrics/
16. CloudAMQP plans, checked 2026-10-04 — https://www.cloudamqp.com/plans.html · Azure Marketplace listing — https://marketplace.microsoft.com/en-us/product/84codes.cloudamqp-v4?tab=overview · GCP Marketplace docs — https://www.cloudamqp.com/docs/google-GCP-marketplace-rabbitmq.html
17. RabbitMQ — Commercial features — https://www.rabbitmq.com/commercial-features
18. RabbitMQ — Release information (community/commercial EOL dates), checked 2026-10 — https://www.rabbitmq.com/release-information
19. RabbitMQ blog — "Changes to the Open Source RabbitMQ Release and Community Support Policy", 2024-05-31 — https://www.rabbitmq.com/blog/2024/05/31/new-community-support-policy
20. RabbitMQ blog — "RabbitMQ 4.3 Highlights", 2026-04-23 — https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release
21. rabbitmq-delayed-message-exchange README (archived; last push 2026-09-24) — https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
22. Broadcom TechDocs — Exclusive Tanzu RabbitMQ features — https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-ova/4-0/tanzu-rabbitmq-ova-virtual-machine/tanzu-rabbitmq-features.html · Tanzu RabbitMQ RPM 4.3 release notes (4.3.6, 2026-09-15) — https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-rpm/4-3/tanzu-rabbitmq-rpm-offering/site-release-notes.html · OSS RabbitMQ 3.13 release notes (3.13.20, 2026-09-15, paid-only binaries) — https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/open-source-rabbitmq/3-13/opn-src-rabbitmq/site-release-notes.html
23. AceMQ (reseller) — "Broadcom's 72-Core Minimum for RabbitMQ", upd. 2026-08-09 (unverified against Broadcom terms) — https://acemq.com/blogs/broadcom-72-core-minimum-rabbitmq-licensing/
24. The Register — "Broadcom pledges to lock down open source Python, Java libraries", 2026-08-31 — https://www.theregister.com/virtualization/2026/08/31/broadcom-pledges-to-lock-down-open-source-python-java-libraries/5293454 · Broadcom TrueSource press release — https://investors.broadcom.com/news-releases/news-release-details/broadcom-strengthens-spring-security-and-adds-coverage-java
25. CSO Online — "RabbitMQ flaws expose OAuth secrets…", 2026-07-13 — https://www.csoonline.com/article/4196093/rabbitmq-flaws-expose-oauth-secrets-risk-complete-takeover-of-the-broker.html
26. rabbitmq-server GitHub releases (latest 4.3.6, 2026-09-14; no 4.4 yet), checked 2026-10-04 — https://github.com/rabbitmq/rabbitmq-server/releases
27. Bitnami — "Upcoming changes to the Bitnami catalog (effective Aug 28, 2025)", opened 2025-07-16 — https://github.com/bitnami/charts/issues/35164
28. Aiven — "KIP-1150 Accepted, and the Road Ahead", 2026-03-12 — https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead
29. Kai Waehner (vendor-adjacent analyst) — "Data Streaming Trends Q3 2026", 2026-09-21 — https://www.kai-waehner.de/blog/2026/09/21/data-streaming-trends-q3-2026-what-changes-through-2027/ · "Data Streaming Landscape Q3 2026", 2026-09-07 — https://www.kai-waehner.de/blog/2026/09/07/data-streaming-landscape-q3-2026-who-controls-your-streams/
30. Aiven — "Pay For What You Stream: New Aiven for Apache Kafka Pricing", 2026-08-26 — https://aiven.io/blog/pay-for-what-you-stream-kafka-pricing
31. Aiven — "Announcing Inkless clusters", 2026-02-26 — https://aiven.io/blog/announcing-inkless-clusters-cloud-kafka-done-right · Inkless docs — https://aiven.io/docs/products/kafka/inkless-overview · Inkless 0.49 release — https://github.com/aiven/inkless/releases/tag/inkless-release-0.49
32. WarpStream pricing, checked 2026-10-04 — https://www.warpstream.com/pricing
33. Redpanda — "The NEW Redpanda Serverless" (rates), 2025-03-25 — https://www.redpanda.com/blog/new-redpanda-serverless · "Redpanda Serverless GA", 2026-02-03 — https://www.redpanda.com/blog/redpanda-serverless-ga-aws-privatelink · What's new in Redpanda Cloud — https://docs.redpanda.com/current/deploy/deployment-option/cloud/whats-new-cloud/
34. Redpanda — Licenses and enterprise features — https://docs.redpanda.com/current/get-started/licensing/overview/ · BSL text — https://github.com/redpanda-data/redpanda/blob/dev/licenses/bsl.md · Connect licensing issue — https://github.com/redpanda-data/connect/issues/2621
35. Redpanda press (Shadowing 2026-08-25, Serverless GA, etc.) — https://www.redpanda.com/press · TechTarget — Redpanda buys Oxla, 2025-10 — https://www.techtarget.com/searchdatamanagement/news/366633563/Streaming-vendor-Redpanda-buys-SQL-engine-unveils-AI-suite
36. Buf — "CoreWeave Acquires Bufstream", 2026-05-08 — https://buf.build/blog/coreweave-acquires-bufstream
37. AutoMQ GitHub (Apache 2.0) — https://github.com/AutoMQ/automq · AutoMQ (vendor) — "10K stars", 2026-06 — https://www.automq.com/blog/automq-10k-stars-diskless-kafka-global-adoption
38. StreamNative — Lakestream / Ursa for Kafka press release, 2026-04-07 — https://www.businesswire.com/news/home/20260407143701/en/StreamNative-Introduces-Lakestream-Architecture-and-Launches-Native-Kafka-Service-Unifying-Streaming-and-the-Lakehouse
39. Apache Pulsar — 4.1 announcement, 2025-09-09 — https://pulsar.apache.org/blog/2025/09/09/announcing-apache-pulsar-4-1/ · Release policy — https://pulsar.apache.org/contribute/release-policy/ · endoflife.date — https://endoflife.date/apache-pulsar
40. CNCF — "Protecting NATS and the integrity of open source", 2025-05-01 — https://www.cncf.io/blog/2025/05/01/protecting-nats-and-the-integrity-of-open-source-cncfs-commitment-to-the-community/ · The Register — "CNCF and Synadia settle NATS dispute", 2025-05-02 — https://www.theregister.com/2025/05/02/cncf_synadia_nats/
41. NATS — Server 2.12 release, 2025-09 — https://nats.io/blog/nats-server-2.12-release/ · Server 2.14 release — https://nats.io/blog/nats-server-2.14-release/
42. Jepsen — NATS 2.12.1, 2025-12-08 — https://jepsen.io/analyses/nats-2.12.1
43. LavinMQ GitHub (Apache 2.0) — https://github.com/cloudamqp/lavinmq · Releases — https://github.com/cloudamqp/lavinmq/releases
44. AWS — SQS fair queues, 2025-07-21 — https://aws.amazon.com/about-aws/whats-new/2025/07/amazon-sqs-introduces-fair/ · SQS 1 MiB payloads, 2025-08-04 — https://aws.amazon.com/about-aws/whats-new/2025/08/amazon-sqs-max-payload-size-1mib · AWS Price List API, SQS us-east-1 (publication 2026-09-11) — https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSQueueService/current/us-east-1/index.json · SQS pricing — https://aws.amazon.com/sqs/pricing/
45. Google Cloud — Pub/Sub pricing — https://cloud.google.com/pubsub/pricing
46. Google Cloud — Managed Service for Apache Kafka pricing (via search excerpt) — https://cloud.google.com/managed-service-for-apache-kafka/pricing · CUDs — https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/cuds
47. Azure Retail Prices API (Event Hubs, Service Bus; eastus), queried 2026-10-04 — https://prices.azure.com/api/retail/prices · Event Hubs pricing — https://azure.microsoft.com/en-us/pricing/details/event-hubs/
48. Upstash — "Announcing Upstash Workflow and Deprecating Upstash Kafka", 2024-09 — https://upstash.com/blog/workflow-kafka
49. Instaclustr — Product update March 2026 — https://www.instaclustr.com/blog/instaclustr-product-update-march-2026/ · Release log — https://www.instaclustr.com/support/documentation/announcements/release-log/
50. kafbat/kafka-ui — https://github.com/kafbat/kafka-ui · Artifact Hub chart 1.6.5 (2026-08-10) — https://artifacthub.io/packages/helm/kafka-ui/kafka-ui · Factor House (competitor) on Kafbat — https://factorhouse.io/articles/kafbat-ui/
51. AKHQ — https://github.com/tchiotludo/akhq · Factor House (competitor) on AKHQ — https://factorhouse.io/articles/akhq/
52. Conduktor pricing — https://www.conduktor.io/pricing · Feb 2026 release (Community Edition changes) — https://www.conduktor.io/product-releases/february-2026 · Desktop sunset — https://docs.conduktor.io/desktop · Factor House (competitor) review — https://factorhouse.io/articles/conduktor/
53. RabbitMQ — Prometheus & Grafana — https://www.rabbitmq.com/docs/prometheus · Management plugin — https://www.rabbitmq.com/docs/management · Discussion #15146 (stats stay in 4.x), 2025-12 — https://github.com/rabbitmq/rabbitmq-server/discussions/15146 · 4.2.0 release notes (Raft metrics) — https://github.com/rabbitmq/rabbitmq-server/blob/main/release-notes/4.2.0.md
54. Karapace — https://github.com/Aiven-Open/karapace · Apicurio Registry — https://github.com/Apicurio/apicurio-registry · Apicurio CNCF Sandbox — https://github.com/cncf/sandbox/issues/461
55. RabbitMQ — Shovel — https://www.rabbitmq.com/docs/shovel · Federation — https://www.rabbitmq.com/docs/federation · Apache Kafka — Connect — https://kafka.apache.org/documentation/#connect
56. Prometheus JMX exporter — https://github.com/prometheus/jmx_exporter · kafka_exporter — https://github.com/danielqsj/kafka_exporter
57. Laravel — Queues (SQS driver) — https://laravel.com/docs/queues · Symfony — Messenger (AMQP/SQS transports) — https://symfony.com/doc/current/messenger.html
