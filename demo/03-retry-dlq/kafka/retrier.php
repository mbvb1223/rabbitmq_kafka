<?php

require __DIR__ . '/common.php';

// php retrier.php   waits until each job is due, retries it, then re-queues it or sends it to the DLQ
$consumer = consumer('retry.retrier', 'retry.jobs.retry');
$producer = producer();
echo "[retrier] waiting on retry.jobs.retry\n";

$running = running();
while ($running()) {
    if (!$msg = kafka_message($consumer->consume(1000))) {
        continue;
    }
    $h = $msg->headers ?? [];

    // Every message here has the same delay, so they arrive in due order and sleeping on the
    // head never holds back one due earlier. Production code would pausePartitions() and
    // keep polling, so a long delay can't exceed max.poll.interval.ms.
    while ($running() && ($wait = (int) $h['x-due-at'] - now_ms()) > 0) {
        usleep(min($wait, 200) * 1000);
    }
    if (!$running()) {
        break; // not committed, so the job is retried after a restart
    }

    $attempt = (int) $h['x-attempt'] + 1;
    $line = sprintf('[%s] %s  attempt=%d', since($h), $msg->payload, $attempt);

    if (!$reason = handle($h)) {
        echo "$line ok -> commit\n";
    } elseif ($attempt > MAX_RETRIES) {
        echo "$line FAIL ($reason) -> retry.jobs.dlq, then commit\n";
        send($producer, 'retry.jobs.dlq', $msg, ['x-attempt' => (string) $attempt] + $h);
    } else {
        echo "$line FAIL ($reason) -> retry.jobs.retry (due in " . RETRY_DELAY_MS / 1000 . "s), then commit\n";
        send($producer, 'retry.jobs.retry', $msg, [
            'x-attempt' => (string) $attempt,
            'x-due-at' => (string) (now_ms() + RETRY_DELAY_MS),
        ] + $h);
    }
    $consumer->commit($msg);
}

$consumer->close();
