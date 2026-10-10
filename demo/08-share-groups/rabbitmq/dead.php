<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php dead.php: print what was dead-lettered, and why
$ch = channel();
$ch->basic_qos(0, 10, false);

echo "[dead] waiting on " . DEAD . "\n";
$ch->basic_consume(DEAD, no_ack: false, callback: function (AMQPMessage $msg): void {
    $headers = $msg->get('application_headers')->getNativeData();
    $death = $headers['x-death'][0];
    echo "[dead] {$msg->getBody()}  reason={$death['reason']} from={$death['queue']} "
        . "deliveries=" . ($headers['x-delivery-count'] ?? '?') . "\n";
    $msg->ack();
});

$ch->consume();
