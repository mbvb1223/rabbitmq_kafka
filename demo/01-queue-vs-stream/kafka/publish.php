<?php

require __DIR__ . '/../../lib/kafka.php';

// php publish.php [count]
$count = (int) ($argv[1] ?? 6);

$producer = new RdKafka\Producer(kafka_conf(['enable.idempotence' => 'true']));
$topic = $producer->newTopic('demo.events');

$at = date('H:i:s');
for ($i = 1; $i <= $count; $i++) {
    // explicit round-robin so the per-partition split is visible; real code passes RD_KAFKA_PARTITION_UA + a key
    $topic->produce(($i - 1) % 2, 0, "event $i @ $at", "order-$i");
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
echo "published $count -> demo.events\n";
