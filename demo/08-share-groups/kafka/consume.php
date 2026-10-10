<?php

require __DIR__ . '/../../lib/kafka.php';

// php consume.php [name]: a classic consumer group, for comparison with the share group
$who = $argv[1] ?? 'worker-' . getmypid();

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => 'share.classic',
    'group.protocol' => 'consumer',
    'auto.offset.reset' => 'earliest',
    'enable.auto.commit' => 'false',
]));
$consumer->subscribe(['share.jobs']);
echo "[$who] classic group 'share.classic' waiting on share.jobs\n";

$running = running();
$last = null;
while ($running()) {
    $msg = kafka_message($consumer->consume(1000));

    if (($parts = partitions($consumer)) !== $last) {
        echo "[$who] partitions: $parts\n";
        $last = $parts;
    }
    if (!$msg) {
        continue;
    }

    echo "[$who] {$msg->payload}  (offset {$msg->offset})\n";
    usleep(500_000);
    $consumer->commit($msg);
}

$consumer->close();
