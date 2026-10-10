<?php

require __DIR__ . '/../../lib/kafka.php';

// php peek.php [topic]   reads partition 0 from the beginning, prints what is still stored, exits
$name = $argv[1] ?? 'ttl.events';

// assign() instead of subscribe(): no group join, no committed offsets, just a look
$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => 'ttl.peek',
    'enable.partition.eof' => 'true',
    'enable.auto.commit' => 'false',
]));
$consumer->assign([new RdKafka\TopicPartition($name, 0, RD_KAFKA_OFFSET_BEGINNING)]);

$seen = 0;
while (true) {
    $msg = $consumer->consume(5000);
    if ($msg === null || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF || $msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
        break;
    }
    if ($msg = kafka_message($msg)) {
        $age = time() - intdiv($msg->timestamp, 1000);
        echo "offset {$msg->offset}: {$msg->payload}  (age {$age} s)\n";
        $seen++;
    }
}

$consumer->queryWatermarkOffsets($name, 0, $low, $high, 5000);
echo "$seen message(s) still in $name, log start offset $low, end offset $high\n";
$consumer->close();
