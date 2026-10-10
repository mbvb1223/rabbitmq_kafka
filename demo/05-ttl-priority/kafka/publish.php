<?php

require __DIR__ . '/../../lib/kafka.php';

// php publish.php <events|high|low> [count]
[, $what, $count] = $argv + [1 => 'events', 2 => 5];
$name = $what === 'events' ? 'ttl.events' : "ttl.jobs.$what";

$producer = new RdKafka\Producer(kafka_conf(['enable.idempotence' => 'true']));
$topic = $producer->newTopic($name);

$at = date('H:i:s');
for ($i = 1; $i <= (int) $count; $i++) {
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, "$what $i, published $at");
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
echo "published $count -> $name\n";
