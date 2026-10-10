<?php

require __DIR__ . '/../../lib/kafka.php';

// php regex.php <team> <regex>   subscribe to every topic whose name matches
[, $team, $regex] = $argv + [1 => 'vn-team', 2 => '^routing\.orders\..*\.vn$'];

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => "routing.regex.$team",
    'group.protocol' => 'consumer', // KIP-848: the broker resolves the regex, not the client
    'auto.offset.reset' => 'earliest',
    'enable.auto.commit' => 'false',
]));
// librdkafka treats a name starting with ^ as a regex
$consumer->subscribe([$regex]);
echo "[$team] subscribed to $regex\n";

$running = running();
$start = microtime(true);
$last = null;
while ($running()) {
    $msg = kafka_message($consumer->consume(1000));

    $topics = array_map(fn (RdKafka\TopicPartition $tp) => $tp->getTopic(), $consumer->getAssignment());
    sort($topics);
    if ($topics !== $last) {
        printf("[%s] +%.0fs topics: %s\n", $team, microtime(true) - $start, $topics ? implode(', ', $topics) : 'none');
        $last = $topics;
    }
    if (!$msg) {
        continue;
    }

    echo "[$team] {$msg->payload}  {$msg->topic_name}\n";
    $consumer->commit($msg);
}

$consumer->close();
