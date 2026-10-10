<?php

require __DIR__ . '/../../lib/kafka.php';

// php read.php — prints every record still in compact.users, from the first offset to the end

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => 'compact.reader', // required by php-rdkafka, never committed: assign() bypasses the group
    'enable.partition.eof' => 'true',
    'enable.auto.commit' => 'false',
]));
$consumer->queryWatermarkOffsets('compact.users', 0, $low, $high, 5_000);
$consumer->assign([new RdKafka\TopicPartition('compact.users', 0, RD_KAFKA_OFFSET_BEGINNING)]);

$offsets = [];
while ($high > 0) {
    $msg = $consumer->consume(5_000);
    if ($msg?->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        break;
    }
    if (!$msg = kafka_message($msg)) {
        continue;
    }
    $offsets[] = $msg->offset;
    printf("offset %2d  %-7s %s\n", $msg->offset, $msg->key, $msg->payload ?? '(tombstone)');
}

$missing = $high - count($offsets);
echo count($offsets) . " records, next offset $high" . ($missing ? ", $missing offsets compacted away" : '') . "\n";
$consumer->close();
