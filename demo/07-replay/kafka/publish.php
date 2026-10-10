<?php

require __DIR__ . '/../../lib/kafka.php';

// php publish.php [count]
$count = (int) ($argv[1] ?? 3);

$producer = new RdKafka\Producer(kafka_conf(['enable.idempotence' => 'true']));
$topic = $producer->newTopic('replay.events');

$at = date('H:i:s');
for ($i = 1; $i <= $count; $i++) {
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, "event $i @ $at", "order-$i");
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
echo "published $count -> replay.events at $at UTC\n";
