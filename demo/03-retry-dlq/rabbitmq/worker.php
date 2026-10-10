<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php worker.php [reject|nack|route]
$mode = $argv[1] ?? 'reject';

$ch = channel();
$ch->confirm_select();
// without a handler php-amqplib ignores a broker nack, and the original would still be acked
$ch->set_nack_handler(fn () => throw new RuntimeException('broker nacked the DLQ copy'));
$ch->basic_qos(0, 1, false);
echo "[worker] mode '$mode', waiting on " . JOBS . "\n";

$ch->basic_consume(JOBS, callback: function (AMQPMessage $msg) use ($ch, $mode): void {
    $h = headers($msg);
    $counts = sprintf('delivery-count=%d acquired-count=%d', $h['x-delivery-count'] ?? 0, $h['x-acquired-count'] ?? 0);
    $line = sprintf('[%s] %s  %-42s', since($h), $msg->getBody(), $counts);
    usleep(100_000);

    $reason = $h['x-fail'] ?? null;
    if ($reason === null) {
        echo "$line ok -> ack\n";
        $msg->ack();
        return;
    }

    if ($mode === 'nack') {
        echo "$line FAIL ($reason) -> nack(requeue)\n";
        usleep(500_000);
        $msg->nack(true);
        return;
    }

    // route: a validation error never succeeds; a timeout gets its retries first, and
    // on the last one we route it ourselves before the broker dead-letters it to retry.dead
    $lastTry = ($h['x-delivery-count'] ?? 0) >= DELIVERY_LIMIT;
    if ($mode === 'route' && ($reason === 'validation' || $lastTry)) {
        echo "$line FAIL ($reason) -> publish to retry.dlq (x-failure-reason=$reason), then ack\n";
        $ch->basic_publish(new AMQPMessage($msg->getBody(), [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'application_headers' => new AMQPTable($h + ['x-failure-reason' => $reason]),
        ]), exchange: 'retry.dlq');
        // ack only once the DLQ copy is confirmed, or a crash here loses the job
        $ch->wait_for_pending_acks(5.0);
        $msg->ack();
        return;
    }

    echo "$line FAIL ($reason) -> reject(requeue)\n";
    // reject, not nack: since 4.3 only reject counts toward x-delivery-limit and the backoff
    $msg->reject(true);
});

$ch->consume();
