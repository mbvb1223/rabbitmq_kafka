<?php

require __DIR__ . '/../../lib/kafka.php';

// php consume.php [group]
$group = $argv[1] ?? 'replay.billing';

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => $group,
    'group.protocol' => 'consumer',
    // used when the group has no valid committed offset: new, expired (7 d empty), or out of range
    'auto.offset.reset' => 'earliest',
    'enable.auto.commit' => 'false',
]));

[$committed] = $consumer->getCommittedOffsets([new RdKafka\TopicPartition('replay.events', 0)], 10_000);
echo "[$group] committed offset on the broker: " . ($committed->getOffset() >= 0 ? $committed->getOffset() : 'none') . "\n";

$consumer->subscribe(['replay.events']);
$running = running();
while ($running()) {
    if (!$msg = kafka_message($consumer->consume(1000))) {
        continue;
    }
    echo "[$group] {$msg->payload}  (offset {$msg->offset}, published " . date('H:i:s', intdiv($msg->timestamp, 1000)) . ")\n";
    usleep(500_000);
    // the broker keeps the group's position: nothing to save locally
    $consumer->commit($msg);
}

$consumer->close();
