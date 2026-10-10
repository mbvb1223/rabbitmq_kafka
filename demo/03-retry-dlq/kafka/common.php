<?php

require __DIR__ . '/../../lib/kafka.php';

// like x-delivery-limit=3: 1 attempt + 3 retries, then the DLQ
const MAX_RETRIES = 3;
// one fixed delay per retry topic: backoff would need one topic per delay step
const RETRY_DELAY_MS = 3000;

function consumer(string $group, string $topic): RdKafka\KafkaConsumer
{
    $consumer = new RdKafka\KafkaConsumer(kafka_conf([
        'group.id' => $group,
        'group.protocol' => 'consumer',
        'auto.offset.reset' => 'earliest',
        'enable.auto.commit' => 'false',
    ]));
    $consumer->subscribe([$topic]);

    return $consumer;
}

function producer(): RdKafka\Producer
{
    $conf = kafka_conf(['enable.idempotence' => 'true']);
    // flush() only reports a timeout; a message that failed delivery still counts as done
    $conf->setDrMsgCb(function ($kafka, RdKafka\Message $m) {
        if ($m->err) {
            $GLOBALS['dr_error'] = $m->errstr();
        }
    });

    return new RdKafka\Producer($conf);
}

function send(RdKafka\Producer $producer, string $topic, RdKafka\Message $msg, array $headers): void
{
    // topic handles leak until the producer is destroyed, so create each one once
    static $topics = [];
    $topics[$topic] ??= $producer->newTopic($topic);
    $topics[$topic]->producev(RD_KAFKA_PARTITION_UA, 0, $msg->payload, $msg->key, $headers);
    // the copy must be on the broker before the caller commits the original
    if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException("flush to $topic timed out");
    }
    if (isset($GLOBALS['dr_error'])) {
        throw new RuntimeException("delivery to $topic failed: {$GLOBALS['dr_error']}");
    }
}

// the same fake job as the RabbitMQ side: it fails when it carries x-fail
function handle(array $headers): ?string
{
    usleep(100_000);

    return $headers['x-fail'] ?? null;
}

function now_ms(): int
{
    return (int) (microtime(true) * 1000);
}

function since(array $headers): string
{
    return sprintf('+%5.1fs', (now_ms() - $headers['x-published-at']) / 1000);
}
