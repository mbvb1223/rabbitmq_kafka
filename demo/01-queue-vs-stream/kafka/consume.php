<?php

require __DIR__ . '/../../lib/kafka.php';

// php consume.php <group> [name] [first|next]
[, $group, $who, $from] = $argv + [1 => 'workers', 2 => 'worker-' . getmypid(), 3 => 'next'];

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => "qs.$group",
    'group.protocol' => 'consumer', // KIP-848: server-side assignment, no stop-the-world rebalance
    // used when the group has no committed offset, or it fell off retention
    'auto.offset.reset' => $from === 'first' ? 'earliest' : 'latest',
    'enable.auto.commit' => 'false',
]));
$consumer->subscribe(['qs.events']);
echo "[$who] group 'qs.$group' waiting on qs.events\n";

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
