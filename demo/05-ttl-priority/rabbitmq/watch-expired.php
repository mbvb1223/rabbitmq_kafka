<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php watch-expired.php   (nobody consumes ttl.jobs, so everything published there ends up here)
$ch = channel();
$ch->basic_qos(0, 1, false);
echo "[dlq] waiting on " . EXPIRED . "\n";

$ch->basic_consume(EXPIRED, no_ack: false, callback: function (AMQPMessage $msg): void {
    $headers = $msg->get('application_headers')->getNativeData();
    $death = $headers['x-death'][0];
    printf("[dlq] %s -> dead-lettered %s, reason=%s, from %s\n",
        $msg->getBody(), date('H:i:s'), $death['reason'], $death['queue']);
    $msg->ack();
});

$ch->consume();
