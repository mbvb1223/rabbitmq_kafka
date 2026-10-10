<?php

require __DIR__ . '/common.php';

// php dlq.php   prints retry.jobs.dlq from the start; auto-commit is off, so it stores no offsets
$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => 'retry.dlq-reader', // KafkaConsumer requires one even with assign()
    'enable.auto.commit' => 'false',
    'enable.partition.eof' => 'true',
]));
$consumer->assign([new RdKafka\TopicPartition('retry.jobs.dlq', 0, RD_KAFKA_OFFSET_BEGINNING)]);

$n = 0;
while (true) {
    $msg = $consumer->consume(5000);
    if ($msg === null || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF || $msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
        break;
    }
    if ($msg->err) {
        throw new RuntimeException($msg->errstr());
    }
    $h = $msg->headers ?? [];
    echo "[retry.jobs.dlq] {$msg->payload}  " . json_encode([
        'x-failure-reason' => $h['x-failure-reason'] ?? null,
        'x-attempt' => $h['x-attempt'] ?? null,
    ]) . "  (offset {$msg->offset})\n";
    $n++;
}

echo "[retry.jobs.dlq] $n message(s)\n";
$consumer->close();
