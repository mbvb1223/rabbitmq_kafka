# PHP client ecosystem for RabbitMQ and Kafka

*As of 2026-10-04. Versions, dates and stars come from the GitHub API, Packagist `p2` metadata and PECL REST, all read on 2026-10-04. GitHub's open-issue counts include PRs.*

## TL;DR for a PHP team

- **RabbitMQ from PHP is still a php-amqplib story.** v3.7.5 shipped 2026-09-28. It is pure PHP, AMQP 0-9-1 only, and tested on PHP 8.5. Releases are slow but steady.
- **RabbitMQ 4.3 breaks php-amqplib's defaults.** `queue_declare('q')` defaults to `durable=false, exclusive=false`, and 4.3 refuses that combination by default (`transient_nonexcl_queues` is now `denied_by_default`). php-amqplib's own test suite failed with 32 errors until PR #1242 fixed it on 2026-09-23. Old tutorial code breaks on our pinned broker.
- **Kafka from PHP still means php-rdkafka 6.0.5 (2024-11-04).** The project is very active, though: 7.0.0alpha1 came out 2026-05-07, about 20 PRs landed in Sept 2026, and it now has a new lead (Ralph Schindler) and a new GitHub org. 7.x has breaking changes: `consume()` returns `null` on timeout.
- **KIP-848 works from PHP today**, because php-rdkafka passes `group.protocol=consumer` straight to librdkafka (GA since 2.12.0). It only works if the image's librdkafka is ≥ 2.12. Our planned `php:8.4-cli` + `install-php-extensions rdkafka` gets Debian trixie's **2.8.0**.
- **Share groups (KIP-932) from PHP:** php-rdkafka has no binding (#616 is waiting for librdkafka GA). A new pure-PHP client, `lisachenko/kafka-client` (created 2026-09-08, 4★, 17 downloads), *claims* a working `KafkaShareConsumer` against Kafka 4.3.1. It is experimental, so treat it as a Q&A footnote.
- **AMQP 1.0 from PHP:** `sigbits/php-amqp-client` v1.0.0 shipped 2026-09-30 (0★). It has no `modified` outcome, so RabbitMQ's per-message annotation features are still out of reach.
- **Symfony's built-in AMQP transport polls with `basic.get`.** Prefetch arrives only as an opt-in `prefetch_count` in Symfony 8.2 (blog post 2026-09-25: 13–18× faster). Demo B's "set prefetch" message doesn't apply to Messenger users on ≤ 8.1.

### What this changes in docs/02 and docs/03

| Doc claim | Status |
|---|---|
| 03 §5 "No PHP AMQP 1.0 client" | **Soften** to "no mature one". sigbits v1.0.x is 4 days old and lacks `modified`, so the Demo A label still holds. |
| 02 §3.1 Kafka "Alternative: None maintained" | **Outdated.** `lisachenko/kafka-client` is pure PHP and actively developed, though unproven. `longlang/phpkafka` is dormant. |
| 02 §3.3 / 03 §5 "share groups: only librdkafka Preview, no php-rdkafka binding" | **True for php-rdkafka.** Add the experimental pure-PHP share consumer as a footnote. |
| 03 §5 "php-rdkafka last stable Nov 2024" | **True.** Don't present it as abandoned: 7.x alpha plus a crash-fix wave in Sept 2026. |
| 03 Setup "PHP 8.4 CLI + install-php-extensions rdkafka" | **Works for Demo B** (classic + cooperative-sticky). Can't demo KIP-848 GA: Debian trixie ships librdkafka 2.8.0. Use `php:8.4-cli-alpine` (Alpine 3.24 → 2.14.1) if needed. |
| 03 §5 "Symfony Messenger ships an AMQP transport" | **Add:** it needs ext-amqp, polls with `basic.get`, and had a quorum-queue retry starvation bug on RabbitMQ 4.3 (#66226, fixed 2026-09-23). |

## Health: RabbitMQ clients

| Name | Latest | Date | PHP | Protocol / features | ★ | Verdict | Link |
|---|---|---|---|---|---|---|---|
| php-amqplib/php-amqplib | v3.7.5 | 2026-09-28 | `^7.2\|\|^8.0` (CI to 8.5 since v3.7.4) | AMQP 0-9-1, pure PHP, needs ext-sockets + ext-mbstring. Confirms, `consume()`/`stopConsume()` (3.2+), PCNTL heartbeat sender | 4,602 | **Healthy, slow cadence** (3 releases in 12 months). 140M downloads | [GH](https://github.com/php-amqplib/php-amqplib) |
| ext-amqp (PECL `amqp`) | 2.2.0 | 2026-01-02 | ≥ 7.4; 8.4/8.5 in CI | AMQP 0-9-1 via rabbitmq-c (v0.18.0, 2026-09-15). PIE-installable. Required by `symfony/amqp-messenger` | 586 | **Maintained, ~1 release/yr** | [PECL](https://pecl.php.net/package/amqp) · [GH](https://github.com/php-amqp/php-amqp) |
| bunny/bunny | 0.5.6 stable / 0.6.0-alpha.4 | 2025-05-22 / 2026-08-03 | `^8.1` | AMQP 0-9-1, async (ReactPHP + fibers) | 749 | **Active, but 0.6 has been alpha for 14 months** | [GH](https://github.com/jakubkulhan/bunny) |
| thesis/amqp | 1.0.2 | 2025-07-19 | `^8.3` | AMQP 0-9-1, async (Amp/Revolt fibers) | 103 | Small; last push 2026-04-08 | [GH](https://github.com/thesis-php/amqp) |
| enqueue/amqp-lib (+ enqueue/rdkafka) | 0.10.27 | 2025-12-21 | `^8.1` | queue-interop wrapper over php-amqplib / ext-rdkafka `^4\|^5\|^6` | 2,220 | **Maintenance mode** (Symfony 8 compat only) | [GH](https://github.com/php-enqueue/enqueue-dev) |
| sigbits/php-amqp-client | v1.0.1 | 2026-10-01 | `^8.3` | **AMQP 1.0**, pure PHP, sync. Outcomes accepted/released/rejected only (no `modified`) | 0 | **Experimental**: single author, 93 downloads | [GH](https://github.com/sigbits/php-amqp-client) |
| timsweb/php-amqp1.0 | none (not on Packagist) | — | ≥ 8.2 | AMQP 1.0 on Revolt | 0 | Stalled (last push 2026-03-17) | [GH](https://github.com/timsweb/php-amqp1.0) |
| crazy-goat/rabbit-stream | v1.4.0 | 2026-09-21 | ≥ 8.1, 64-bit | **Native stream protocol** (5552), super streams, TLS | 0 | **Experimental**: 1 main dev (253 commits), 4 total downloads | [GH](https://github.com/crazy-goat/rabbit-stream) |

The rabbitmq.com client-library pages list no PHP stream-protocol client and no PHP AMQP 1.0 client.

## Health: Kafka clients

| Name | Latest | Date | PHP | Protocol / features | ★ | Verdict | Link |
|---|---|---|---|---|---|---|---|
| php-rdkafka (PECL `rdkafka`) | **6.0.5** stable / 7.0.0alpha1 | 2024-11-04 / 2026-05-07 | 6.x: PECL 7.0–8.99 (CI to 8.3; builds on 8.4/8.5 per installer matrix). 7.x: ≥ 8.1 (CI 8.3–8.5) | librdkafka binding: high/low-level consumer, transactions, OAUTHBEARER, idempotence. KIP-848 via config. **No share consumer** | 2,180 | **Active (new lead), but no stable release in 23 months.** 7.x crash fixes are open (#627–#634) | [GH](https://github.com/php-rdkafka/php-rdkafka) · [PECL](https://pecl.php.net/package/rdkafka) |
| librdkafka (C) | v2.15.1 (2.16.0-RC4 tagged) | 2026-09-09 | — | KIP-848 **GA since 2.12.0** (2025-10-08). KIP-932 share consumer **Preview** in 2.15.0 (2026-06-30), C API only | 1,060 | Healthy (Confluent) | [CHANGELOG](https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md) |
| lisachenko/kafka-client | 4.3.1 | 2026-09-24 | `^8.4` | Pure-PHP wire protocol for Kafka 4.3.1. **Claims** KIP-848 + KIP-932 `KafkaShareConsumer`, transactions, admin | 4 | **Experimental**: 4 weeks old, 17 downloads, 687 of 986 commits from a `claude` account | [GH](https://github.com/lisachenko/kafka-client) |
| longlang/phpkafka (swoole/phpkafka) | v1.2.5 | 2023-10-13 | ≥ 7.1 | Pure PHP, Swoole-friendly | 278 | **Dormant** (last push 2024-02-21). Kafka 4.x compatibility (unverified) | [GH](https://github.com/swoole/phpkafka) |
| idealo/php-rdkafka-ffi | v0.6.0 | 2025-01-25 | `^7.4\|^8.0` | FFI binding to librdkafka | 82 | **Archived** 2025-10-27 | [GH](https://github.com/idealo/php-rdkafka-ffi) |
| Confluent REST Proxy (HTTP) | CP component | — | any | v3 produce, v2 stateful consumer instances only | — | OK for **producers** from PHP-FPM. Bad fit for consumers. Not in the `apache/kafka` image; Confluent Community License (unverified) | [API docs](https://docs.confluent.io/platform/current/kafka-rest/api.html) |

## Health: framework bridges

| Name | Latest | Date | PHP / framework | Notes | ★ | Verdict | Link |
|---|---|---|---|---|---|---|---|
| symfony/amqp-messenger | v8.1.8 | 2026-09-23 | ≥ 8.4.1 (8.x) | Built in. **Requires ext-amqp.** `basic.get` polling. 8.1 added AmqpPriorityStamp and daily quorum delay queues. 8.2 adds opt-in `prefetch_count` | (symfony) | Healthy | [8.2 blog](https://symfony.com/blog/new-in-symfony-8-2-faster-messenger-workers) |
| jwage/phpamqplib-messenger | 0.10.1 | 2026-06-19 | `^8.3` | Messenger on php-amqplib, push `basic.consume`. No 8.1 `daily_delay_queues` yet (#148) | 92 | Active, single maintainer, 0.x | [GH](https://github.com/jwage/phpamqplib-messenger) |
| vladimir-yuldashev/laravel-queue-rabbitmq | v15.0.2 | 2026-09-02 | `^8.0`, Laravel 10–13 | Real queue driver on php-amqplib `^3.6`. Declares durable queues; quorum option | 2,133 | **Maintained** (Laravel 13.x fixes in 2026). GitHub releases stop at v14.4.0, tags continue | [GH](https://github.com/vyuldashev/laravel-queue-rabbitmq) |
| mateusjunges/laravel-kafka | v2.12.1 | 2026-09-29 | 8.2–8.5, Laravel 12/13 | Producer/consumer API, not a Queue driver. Needs ext-rdkafka **`^6.0`**. DLQ, `retryFailedMessages()`, `stopOnFailure()` (v2.12.0) | 736 | **Very active** | [GH](https://github.com/mateusjunges/laravel-kafka) |
| koco/messenger-kafka | v0.19 | 2026-10-01 | `^7.4\|8.*`, Symfony 5.4–8.1 | Messenger transport over ext-rdkafka | 92 | **Risky**: one commit in Oct 2026 after 2.5 years idle; 24 open | [GH](https://github.com/KonstantinCodes/messenger-kafka) |
| hyperf/kafka | v3.2.0 | 2026-06-07 | ≥ 8.2 (Swoole) | Coroutine consumer, wraps longlang/phpkafka | 7 (split) | Depends on a dormant library | [GH](https://github.com/hyperf/kafka) |

## Runtime and process model

| Runtime | Version | Fit for consumers |
|---|---|---|
| supervisord / k8s Deployment | — | Default: one PHP process = one AMQP consumer or one Kafka group member. Restart on `--memory-limit` / `--max-jobs` |
| RoadRunner | v2025.1.15 (2026-06-17); kafka driver v6.0.0-beta.6 | Go consumes (franz-go), and a PHP pool processes. Ack → `MarkCommitRecords` + `AutoCommitMarks`. **`Nack()` without requeue is a no-op.** Out-of-order acks from a parallel pool can commit past in-flight records (inferred from source) |
| FrankenPHP | v1.12.7 (2026-08-07) | Worker mode is HTTP only. Non-HTTP "background workers" are **not released** (PR #2617 open) |
| Swoole | 6.2.3 (2026-09-22); 6.3.0-rc1 | Coroutines help AMQP (Hyperf). php-rdkafka's blocking C calls don't yield to the scheduler (unverified) |
| AMPHP v3 / Revolt | amp 3.1.3, revolt 1.0.9 | Fibers. bunny 0.6 and thesis/amqp run on them. No fiber-native Kafka client except experimental lisachenko |

**Parallelism:**
- **RabbitMQ:** parallelism = consumers × prefetch. Prefetch is per-consumer (`basic_qos(0, N, false)`). On 4.3, `global=true` is **silently downgraded** to per-consumer (`rabbit_channel.erl`, `global_qos` denied by default).
- **Kafka:** members ≤ partitions do useful work, with both classic and KIP-848 groups. Only share groups lift the cap, and from PHP that's experimental.

**Memory:**
- php-rdkafka: topic handles aren't freed until the client is destroyed (#615/#617). Call `newTopic()` once per topic.
- librdkafka's prefetch buffer defaults to `queued.max.messages.kbytes=65536` (64 MB) per partition. php-rdkafka's README still says "1 GB", which is stale.

## Snippets

Verified against php-amqplib `AMQPChannel.php`/`AMQPMessage.php` (master, v3.7.5) and php-rdkafka stubs/UPGRADE.md (7.x) plus PECL 6.0.5. The host names match `compose.yaml`.

### RabbitMQ: publish with confirms (php-amqplib 3.7)

```php
<?php
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

$conn = new AMQPStreamConnection('rabbitmq', 5672, 'app', 'app');
$ch = $conn->channel();

// php-amqplib defaults (durable=false, exclusive=false) are refused by RabbitMQ 4.3
$ch->queue_declare('jobs', durable: true, auto_delete: false,
    arguments: new AMQPTable(['x-queue-type' => 'quorum']));

$ch->confirm_select();
$ch->set_nack_handler(fn (AMQPMessage $m) => throw new RuntimeException('broker nacked'));

$ch->basic_publish(
    new AMQPMessage(json_encode(['id' => 1]), [
        'content_type' => 'application/json',
        'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
    ]),
    exchange: '',
    routing_key: 'jobs',
);
$ch->wait_for_pending_acks(5.0);

$ch->close();
$conn->close();
```

### RabbitMQ: consume with manual ack

```php
<?php
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php-amqplib can't send heartbeats while a callback runs: keep jobs < 2×heartbeat
// or use PCNTLHeartbeatSender
$conn = new AMQPStreamConnection('rabbitmq', 5672, 'app', 'app',
    read_write_timeout: 60.0, heartbeat: 30);
$ch = $conn->channel();
$ch->queue_declare('jobs', durable: true, auto_delete: false,
    arguments: new AMQPTable(['x-queue-type' => 'quorum']));

$ch->basic_qos(0, 10, false);

$ch->basic_consume('jobs', callback: function (AMQPMessage $msg): void {
    try {
        handle(json_decode($msg->getBody(), true, flags: JSON_THROW_ON_ERROR));
        $msg->ack();
    } catch (Throwable) {
        $msg->reject(requeue: true); // reject, not nack: see docs/03 Demo A gotcha
    }
});

pcntl_async_signals(true);
pcntl_signal(SIGTERM, fn () => $ch->stopConsume());

$ch->consume();
$ch->close();
$conn->close();
```

### Kafka: produce (php-rdkafka 6.0.5; also valid on 7.x)

```php
<?php
$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', 'kafka:19092');
$conf->set('enable.idempotence', 'true'); // forces acks=all
$conf->setDrMsgCb(function ($kafka, RdKafka\Message $m): void {
    if ($m->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        error_log('delivery failed: ' . rd_kafka_err2str($m->err));
    }
});

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('jobs'); // create once and reuse: handles leak until the client is destroyed

$topic->producev(RD_KAFKA_PARTITION_UA, 0, json_encode(['id' => 1]), 'order-42', ['type' => 'job']);
$producer->poll(0);

// without flush, buffered messages die with the process
if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
```

### Kafka: consume with manual commit

```php
<?php
$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', 'kafka:19092');
$conf->set('group.id', 'workers');
$conf->set('enable.auto.commit', 'false');
$conf->set('auto.offset.reset', 'earliest');
$conf->set('partition.assignment.strategy', 'cooperative-sticky');

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe(['jobs']);

$running = true;
pcntl_async_signals(true);
pcntl_signal(SIGTERM, function () use (&$running) { $running = false; });

while ($running) {
    $msg = $consumer->consume(1000);
    // 7.x returns null on timeout; 6.x returns a message with err=_TIMED_OUT
    if ($msg === null || $msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
        continue;
    }
    if ($msg->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException($msg->errstr(), $msg->err);
    }

    handle($msg->payload);   // must return within max.poll.interval.ms (300 s default)
    $consumer->commit($msg); // sync, commits offset+1, throws RdKafka\Exception on failure
}

// leave the group now instead of holding partitions for session.timeout.ms (45 s)
$consumer->close();
```

To show KIP-848 instead, replace the `partition.assignment.strategy` line with `group.protocol=consumer` (optionally `group.remote.assignor`). This needs librdkafka ≥ 2.12 and Kafka ≥ 4.0. `session.timeout.ms` becomes the broker-side `group.consumer.session.timeout.ms`.

## Gotchas

1. **RabbitMQ 4.3 rejects php-amqplib's default queue.** `queue_declare('q')` is transient and non-exclusive, so 4.3 refuses it. Declare queues `durable: true`, use quorum, or make them exclusive. The official PHP tutorial now uses quorum queues.
2. **`basic_qos(..., global: true)` is silently per-consumer on 4.3.** No error is raised, so anyone relying on channel-wide prefetch gets more in-flight messages than expected.
3. **Symfony Messenger + RabbitMQ 4.3 quorum queues:** retried messages carried `priority: 0` while new messages default to priority 4. Retries starved behind any backlog, and one team saw waits of more than 2 h (#66226). Fixed by #66228, merged 2026-09-23 into 6.4+. Confirm your patch release includes it. The php-amqplib-based transport wasn't affected.
4. **Messenger's AMQP transport doesn't prefetch** (`basic.get` per message). It's opt-in `prefetch_count` from 8.2, and prefetched messages are redelivered on worker stop.
5. **php-rdkafka 7.x is breaking:**
   - `consume()` returns `null` on timeout.
   - `KafkaConsumer::poll()` (new in alpha1) can abort the process and is proposed for removal (#619).
   - The ecosystem pins `ext-rdkafka ^6` (laravel-kafka `^6.0`, enqueue `^4|^5|^6`), so Composer blocks 7.x until those packages update.
6. **Distro librdkafka decides your Kafka features:**
   - `php:8.4-cli` (trixie): 2.8.0. KIP-848 there is early-access only (it reached Preview in 2.10, GA in 2.12).
   - `php:8.4-cli-alpine` (3.24): 2.14.1.
   - Ubuntu 22.04: 1.8.0, below Kafka 4.x's **1.8.2** floor (KIP-896).
   - Old builds reject `group.protocol` with "No such configuration property" (#591).
7. **Slow PHP job + Kafka:** going past `max.poll.interval.ms` evicts the member, the commit then throws, and the batch is redone by someone else. RabbitMQ's equivalent is the 30-minute consumer timeout, now configurable per quorum queue in 4.3.
8. **laravel-kafka drops failed messages by default.** Without a DLQ the offset is committed and the message isn't consumed again (documented in v2.12.0). Use `retryFailedMessages()` or `stopOnFailure()`.
9. **php-amqplib forked heartbeat process:** `SIGHeartbeatSender` left an orphaned child after `SIGKILL`. Fixed in v3.7.5 (#1244). Prefer `PCNTLHeartbeatSender`, which doesn't fork.
10. **php-rdkafka crash class:** open PRs fix crashes when committing without an assignment (#634) and in the log callback (#633). Test rebalances (scale up and down) before trusting it in production.

## Recent posts / talks (2025–2026)

- Symfony blog, "New in Symfony 8.2: Faster Messenger Workers" (N. Grekas, 2026-09-25): AMQP `prefetch_count`, 13–18× faster.
- Symfony blog, "New in Symfony 8.1: Messenger Improvements" (J. Eguiluz, 2026-05-22): AmqpPriorityStamp, daily quorum delay queues.
- Symfony blog, "Introducing a streaming AMQP transport for Symfony Messenger" (J. Wage, 2025-04-25): php-amqplib, push consume, no C extension.
- symfony/symfony #66226 (2026-09-22): a production write-up of quorum-queue retry starvation on RabbitMQ 4.3.
- php-amqplib PR #1242 (2026-09-23): the clearest public write-up of the 4.3 transient-queue break, from the PHP side.
- laravelkafka.com: "Long-running processes are where Kafka clients fall apart." (undated)
- I found no 2025–2026 conference talk specifically on PHP + Kafka (unverified: the search budget ran out).

## Sources

- php-amqplib: [repo/README](https://github.com/php-amqplib/php-amqplib) · [v3.7.5 release, 2026-09-28](https://github.com/php-amqplib/php-amqplib/releases/tag/v3.7.5) · [PR #1242, 2026-09-23](https://github.com/php-amqplib/php-amqplib/pull/1242) · [PR #1244](https://github.com/php-amqplib/php-amqplib/pull/1244) · [AMQPChannel.php](https://github.com/php-amqplib/php-amqplib/blob/master/PhpAmqpLib/Channel/AMQPChannel.php) · [Packagist](https://packagist.org/packages/php-amqplib/php-amqplib)
- RabbitMQ 4.3: [4.3.0 release notes (deprecated features)](https://github.com/rabbitmq/rabbitmq-server/blob/main/release-notes/4.3.0.md) · [rabbit_channel.erl global_qos handling](https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbit/src/rabbit_channel.erl) · [PHP tutorial send.php](https://github.com/rabbitmq/rabbitmq-tutorials/blob/main/php/send.php) · [AMQP 1.0 client libraries](https://www.rabbitmq.com/client-libraries/amqp-client-libraries) · [devtools](https://www.rabbitmq.com/client-libraries/devtools)
- ext-amqp: [PECL amqp 2.2.0, 2026-01-02](https://pecl.php.net/package/amqp) · [php-amqp/php-amqp](https://github.com/php-amqp/php-amqp) · [rabbitmq-c](https://github.com/alanxz/rabbitmq-c)
- Others (RabbitMQ): [bunny](https://github.com/jakubkulhan/bunny) · [thesis/amqp](https://github.com/thesis-php/amqp) · [enqueue-dev](https://github.com/php-enqueue/enqueue-dev) · [sigbits/php-amqp-client](https://github.com/sigbits/php-amqp-client) ([SettlementOutcome.php](https://github.com/sigbits/php-amqp-client/blob/main/src/Protocol/Performative/SettlementOutcome.php)) · [timsweb/php-amqp1.0](https://github.com/timsweb/php-amqp1.0) · [crazy-goat/rabbit-stream](https://github.com/crazy-goat/rabbit-stream)
- php-rdkafka: [repo/README](https://github.com/php-rdkafka/php-rdkafka) · [UPGRADE.md (6→7)](https://github.com/php-rdkafka/php-rdkafka/blob/7.x/UPGRADE.md) · [PECL rdkafka (6.0.5 2024-11-04, 7.0.0alpha1 2026-05-07)](https://pecl.php.net/package/rdkafka) · [#616 share consumers](https://github.com/php-rdkafka/php-rdkafka/issues/616) · [#591 group.protocol](https://github.com/php-rdkafka/php-rdkafka/issues/591) · [#619 remove poll()](https://github.com/php-rdkafka/php-rdkafka/pull/619) · [#624 support structure / distro table](https://github.com/php-rdkafka/php-rdkafka/pull/624) · [#617 topic handles](https://github.com/php-rdkafka/php-rdkafka/pull/617) · [#634](https://github.com/php-rdkafka/php-rdkafka/pull/634)
- librdkafka: [CHANGELOG (2.12.0 KIP-848 GA; 2.15.0 KIP-932 Preview)](https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md) · [INTRODUCTION.md (KIP-848 config mapping, share consumer)](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md) · [CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) · [Confluent client deprecation (1.8.2 floor)](https://docs.confluent.io/platform/current/clients/deprecate-how-to.html) · [KIP-896](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0)
- Distro/images: [Debian librdkafka (trixie 2.8.0)](https://sources.debian.org/src/librdkafka/) · [Alpine 3.24 librdkafka 2.14.1](https://pkgs.alpinelinux.org/packages?name=librdkafka&branch=v3.24) · [docker-library php tags (8.4-cli = trixie)](https://github.com/docker-library/official-images/blob/master/library/php) · [install-php-extensions supported matrix](https://github.com/mlocati/docker-php-extension-installer/blob/master/data/supported-extensions)
- Other Kafka: [lisachenko/kafka-client](https://github.com/lisachenko/kafka-client) · [swoole/phpkafka](https://github.com/swoole/phpkafka) · [idealo/php-rdkafka-ffi (archived)](https://github.com/idealo/php-rdkafka-ffi) · [Confluent REST Proxy API](https://docs.confluent.io/platform/current/kafka-rest/api.html)
- Frameworks: [symfony/amqp-messenger on Packagist](https://packagist.org/packages/symfony/amqp-messenger) · [Symfony 8.2 blog](https://symfony.com/blog/new-in-symfony-8-2-faster-messenger-workers) · [Symfony 8.1 blog](https://symfony.com/blog/new-in-symfony-8-1-messenger-improvements) · [jwage transport blog](https://symfony.com/blog/introducing-a-streaming-amqp-transport-for-symfony-messenger) · [symfony #66226](https://github.com/symfony/symfony/issues/66226) / [#66228](https://github.com/symfony/symfony/pull/66228) · [jwage/phpamqplib-messenger](https://github.com/jwage/phpamqplib-messenger) · [laravel-queue-rabbitmq](https://github.com/vyuldashev/laravel-queue-rabbitmq) · [laravel-kafka releases](https://github.com/mateusjunges/laravel-kafka/releases) · [laravelkafka.com](https://laravelkafka.com/) · [koco/messenger-kafka](https://github.com/KonstantinCodes/messenger-kafka) · [hyperf/kafka](https://github.com/hyperf/kafka)
- Runtimes: [RoadRunner](https://github.com/roadrunner-server/roadrunner) · [roadrunner-server/kafka item.go / driver.go](https://github.com/roadrunner-server/kafka/tree/master/kafkajobs) · [FrankenPHP PR #2617](https://github.com/php/frankenphp/pull/2617) · [Swoole releases](https://github.com/swoole/swoole-src/releases)
