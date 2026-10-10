<?php

require __DIR__ . '/../../lib/kafka.php';

// php publish.php [updates|tick]
$what = $argv[1] ?? 'updates';

$conf = kafka_conf(['enable.idempotence' => 'true']);
// flush() returns NO_ERROR once the queue drains, even if some messages failed
$failed = 0;
$conf->setDrMsgCb(function (RdKafka\Producer $kafka, RdKafka\Message $msg) use (&$failed): void {
    if ($msg->err) {
        $failed++;
    }
});
$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('compact.users');

$records = $what === 'tick'
    // compaction never touches the active segment: one more record after segment.ms rolls it
    ? [['tick', 'tick @ ' . gmdate('H:i:s') . ' UTC']]
    : [
        ['user-1', 'name=An v1'],
        ['user-2', 'name=Binh v1'],
        ['user-1', 'name=An v2'],
        ['user-3', 'name=Chi v1'],
        ['user-1', 'name=An v3'],
        ['user-2', 'name=Binh v2'],
        ['user-3', null], // tombstone: "user-3 was deleted"
    ];

foreach ($records as [$key, $value]) {
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $value, $key);
    echo "$key => " . ($value ?? '(tombstone)') . "\n";
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
if ($failed) {
    throw new RuntimeException("$failed not delivered");
}
echo 'published ' . count($records) . " -> compact.users\n";
