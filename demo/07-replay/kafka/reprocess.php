<?php

require __DIR__ . '/../../lib/kafka.php';

// php reprocess.php <since, e.g. "-2 minutes", "2026-10-10 05:40 UTC">: reads from that time to the current end, then exits
$since = new DateTimeImmutable($argv[1] ?? '-5 minutes');

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    // RdKafka\KafkaConsumer requires a group.id, but assign() + no commit never joins or moves the group
    'group.id' => 'replay.oneoff',
    'enable.auto.commit' => 'false',
]));

$meta = $consumer->getMetadata(false, $consumer->newTopic('replay.events'), 10_000);
$wanted = [];
foreach ($meta->getTopics()->current()->getPartitions() as $p) {
    $wanted[] = new RdKafka\TopicPartition('replay.events', $p->getId(), $since->getTimestamp() * 1000);
}

// per partition: the first offset whose timestamp is >= $since, or -1 if nothing is that new
$start = [];
$end = [];
foreach ($consumer->offsetsForTimes($wanted, 10_000) as $tp) {
    $consumer->queryWatermarkOffsets('replay.events', $tp->getPartition(), $low, $high, 10_000);
    echo "[oneoff] partition {$tp->getPartition()}: since {$since->format('H:i:s')} = offset {$tp->getOffset()}, end = $high\n";
    if ($tp->getOffset() >= 0 && $tp->getOffset() < $high) {
        $start[] = $tp;
        $end[$tp->getPartition()] = $high;
    }
}
$consumer->assign($start);

while ($end) {
    if (!$msg = kafka_message($consumer->consume(1000))) {
        continue;
    }
    echo "[oneoff] {$msg->payload}  (offset {$msg->offset}, published " . date('H:i:s', intdiv($msg->timestamp, 1000)) . ")\n";
    if ($msg->offset + 1 >= $end[$msg->partition]) {
        unset($end[$msg->partition]);
    }
}
echo "[oneoff] done, no group offsets touched\n";
$consumer->close();
