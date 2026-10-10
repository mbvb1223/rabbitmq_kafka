<?php

require __DIR__ . '/../../lib/kafka.php';

// php publish.php <events|high|low> [count]
[, $what, $count] = $argv + [1 => 'events', 2 => 5];
$name = $what === 'events' ? 'ttl.events' : "ttl.jobs.$what";

$conf = kafka_conf(['enable.idempotence' => 'true']);
// flush() returns NO_ERROR once the queue drains, even if some messages failed
$failed = 0;
$conf->setDrMsgCb(function (RdKafka\Producer $kafka, RdKafka\Message $msg) use (&$failed): void {
    if ($msg->err) {
        $failed++;
    }
});
$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic($name);

$at = date('H:i:s');
for ($i = 1; $i <= (int) $count; $i++) {
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, "$what $i, published $at");
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
if ($failed) {
    throw new RuntimeException("$failed not delivered");
}
echo "published $count -> $name\n";
