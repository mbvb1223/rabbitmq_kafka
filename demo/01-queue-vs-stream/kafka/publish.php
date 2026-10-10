<?php

require __DIR__ . '/../../lib/kafka.php';

// php publish.php [count]
$count = (int) ($argv[1] ?? 6);

$conf = kafka_conf(['enable.idempotence' => 'true']);
// flush() returns NO_ERROR once the queue drains, even if some messages failed
$failed = 0;
$conf->setDrMsgCb(function (RdKafka\Producer $kafka, RdKafka\Message $msg) use (&$failed): void {
    if ($msg->err) {
        $failed++;
    }
});
$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('qs.events');

$at = date('H:i:s');
for ($i = 1; $i <= $count; $i++) {
    // explicit round-robin so the per-partition split is visible; real code passes RD_KAFKA_PARTITION_UA + a key
    $topic->produce(($i - 1) % 2, 0, "event $i @ $at", "order-$i");
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
if ($failed) {
    throw new RuntimeException("$failed not delivered");
}
echo "published $count -> qs.events\n";
