<?php

require __DIR__ . '/../../lib/kafka.php';

// php publish.php [updates|tick]
$what = $argv[1] ?? 'updates';

$producer = new RdKafka\Producer(kafka_conf(['enable.idempotence' => 'true']));
$topic = $producer->newTopic('compact.users');

$records = $what === 'tick'
    // compaction never touches the active segment: one more record after segment.ms rolls it
    ? [['tick', 'tick @ ' . date('H:i:s')]]
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
echo 'published ' . count($records) . " -> compact.users\n";
