<?php

require __DIR__ . '/../../lib/kafka.php';
require __DIR__ . '/../job.php';

// php publish.php [count]
$count = (int) ($argv[1] ?? 10);

$producer = new RdKafka\Producer(kafka_conf(['enable.idempotence' => 'true']));
$topic = $producer->newTopic('hol.jobs');

for ($i = 1; $i <= $count; $i++) {
    // odd jobs -> partition 0, even -> partition 1, so job 3 has 5, 7, 9 queued behind it
    $topic->produce(($i - 1) % 2, 0, job($i), "job-$i");
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
echo "published jobs 1-$count -> hol.jobs (odd on partition 0, even on 1; job " . BAD_JOB . " is the bad one)\n";
