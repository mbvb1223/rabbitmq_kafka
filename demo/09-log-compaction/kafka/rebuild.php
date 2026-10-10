<?php

require __DIR__ . '/../../lib/kafka.php';

// php rebuild.php — what a service does on boot: replay the whole topic into an in-memory map

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => 'compact.rebuild', // never committed: a rebuild always starts from the beginning
    'enable.partition.eof' => 'true',
]));
$consumer->queryWatermarkOffsets('compact.users', 0, $low, $high, 5_000);
$consumer->assign([new RdKafka\TopicPartition('compact.users', 0, RD_KAFKA_OFFSET_BEGINNING)]);

$started = microtime(true);
$users = [];
$read = 0;
while ($high > 0) {
    $msg = $consumer->consume(5_000);
    if ($msg?->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        break;
    }
    if (!$msg = kafka_message($msg)) {
        continue;
    }
    $read++;
    if ($msg->key === 'tick') {
        continue;
    }
    if ($msg->payload === null) {
        unset($users[$msg->key]);
    } else {
        $users[$msg->key] = $msg->payload;
    }
}
$consumer->close();

printf("rebuilt from %d records in %d ms:\n", $read, (microtime(true) - $started) * 1000);
foreach ($users as $key => $value) {
    echo "  $key => $value\n";
}
