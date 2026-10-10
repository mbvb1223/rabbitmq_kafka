<?php

require __DIR__ . '/../../lib/kafka.php';

// php consume.php <group> [name] [first|next]
[, $group, $who, $from] = $argv + [1 => 'workers', 2 => 'worker-' . getmypid(), 3 => 'next'];

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => $group,
    'group.protocol' => 'consumer', // KIP-848: server-side assignment, no stop-the-world rebalance
    // only used when the group has no committed offset yet
    'auto.offset.reset' => $from === 'first' ? 'earliest' : 'latest',
    'enable.auto.commit' => 'false',
]));
$consumer->subscribe(['demo.events']);
echo "[$who] group '$group' waiting on demo.events\n";

$running = running();
$last = null;
while ($running()) {
    $msg = kafka_message($consumer->consume(1000));

    // checked after consume(): the assignment arrives during the poll
    if (($parts = partitions($consumer)) !== $last) {
        echo "[$who] partitions: $parts\n";
        $last = $parts;
    }
    if (!$msg) {
        continue;
    }

    echo "[$who] {$msg->payload}  (partition {$msg->partition}, offset {$msg->offset})\n";
    usleep(200_000);
    // only moves this group's cursor; the message stays in the topic
    $consumer->commit($msg);
}

$consumer->close();
