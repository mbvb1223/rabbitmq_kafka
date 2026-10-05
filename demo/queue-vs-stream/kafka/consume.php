<?php

// php consume.php <group> [name] [first|next]
[, $group, $who, $from] = $argv + [1 => 'workers', 2 => 'worker-' . getmypid(), 3 => 'next'];

$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', 'kafka:19092');
$conf->set('group.id', $group);
$conf->set('group.protocol', 'consumer'); // KIP-848: server-side assignment, no stop-the-world rebalance
// only used when the group has no committed offset yet
$conf->set('auto.offset.reset', $from === 'first' ? 'earliest' : 'latest');
$conf->set('enable.auto.commit', 'false');

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe(['demo.events']);
echo "[$who] group '$group' waiting on demo.events\n";

$last = null;
while (true) {
    $parts = array_map(fn (RdKafka\TopicPartition $tp) => $tp->getPartition(), $consumer->getAssignment());
    if ($parts !== $last) {
        echo "[$who] partitions: " . ($parts ? implode(', ', $parts) : 'none (idle)') . "\n";
        $last = $parts;
    }

    $msg = $consumer->consume(1000);
    // php-rdkafka 7 returns null on timeout, 6.x returns an error message
    if ($msg === null || in_array($msg->err, [RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__PARTITION_EOF], true)) {
        continue;
    }
    if ($msg->err) {
        throw new RuntimeException($msg->errstr());
    }

    echo "[$who] {$msg->payload}  (partition {$msg->partition}, offset {$msg->offset})\n";
    usleep(200_000);
    // only moves this group's cursor; the message stays in the topic
    $consumer->commit($msg);
}
