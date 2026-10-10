<?php

require __DIR__ . '/common.php';

// php publish.php [count] [failures, e.g. 3:timeout,4:validation]
[, $count, $failures] = $argv + [1 => 5, 2 => '3:timeout'];

$fail = [];
foreach (array_filter(explode(',', $failures)) as $spec) {
    [$id, $reason] = explode(':', $spec) + [1 => 'timeout'];
    $fail[(int) $id] = $reason;
}

$producer = producer();
$topic = $producer->newTopic('retry.jobs');
for ($i = 1; $i <= (int) $count; $i++) {
    $headers = ['x-published-at' => (string) now_ms()];
    if (isset($fail[$i])) {
        $headers['x-fail'] = $fail[$i];
    }
    $topic->producev(RD_KAFKA_PARTITION_UA, 0, "job $i", "job-$i", $headers);
}

if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}
echo "published $count -> retry.jobs" . ($fail ? " (failing: $failures)" : '') . "\n";
