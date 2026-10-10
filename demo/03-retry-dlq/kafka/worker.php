<?php

require __DIR__ . '/common.php';

// php worker.php   a failed job is copied to retry.jobs.retry so this partition keeps moving
$consumer = consumer('retry.workers', 'retry.jobs');
$producer = producer();
echo "[worker] waiting on retry.jobs\n";

$running = running();
while ($running()) {
    if (!$msg = kafka_message($consumer->consume(1000))) {
        continue;
    }
    $h = $msg->headers ?? [];
    $line = sprintf('[%s] %s  attempt=1', since($h), $msg->payload);

    if ($reason = handle($h)) {
        echo "$line FAIL ($reason) -> retry.jobs.retry, then commit\n";
        send($producer, 'retry.jobs.retry', $msg, $h + [
            'x-attempt' => '1',
            'x-due-at' => (string) (now_ms() + RETRY_DELAY_MS),
            'x-failure-reason' => $reason,
        ]);
    } else {
        echo "$line ok -> commit\n";
    }
    $consumer->commit($msg);
}

$consumer->close();
