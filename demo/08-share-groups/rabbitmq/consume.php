<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php consume.php [name]
$who = $argv[1] ?? 'worker-' . getmypid();

$ch = channel();
// like Kafka's record_limit + max.poll.records=1: one unacked job per worker
$ch->basic_qos(0, 1, false);

echo "[$who] waiting on " . JOBS . "\n";
$ch->basic_consume(JOBS, no_ack: false, callback: function (AMQPMessage $msg) use ($who): void {
    // x-delivery-count counts failed deliveries (reject, crash), absent on the first; nack and consumer timeout don't count
    $headers = $msg->has('application_headers') ? $msg->get('application_headers')->getNativeData() : [];
    $delivery = ($headers['x-delivery-count'] ?? 0) + 1;
    echo "[$who] {$msg->getBody()}  (delivery $delivery)\n";
    usleep(500_000);

    if (str_contains($msg->getBody(), 'poison')) {
        // reject, not nack: since 4.3 nack doesn't count toward x-delivery-limit
        $msg->reject(requeue: true);
        return;
    }
    $msg->ack();
});

$ch->consume();
