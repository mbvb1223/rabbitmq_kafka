<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php consume-prio.php [prefetch]
$prefetch = (int) ($argv[1] ?? 1);

$ch = channel();
// priority only reorders what is still in the queue; prefetched messages are already on their way
$ch->basic_qos(0, $prefetch, false);
echo "[worker] prefetch $prefetch, waiting on " . PRIO . "\n";

$ch->basic_consume(PRIO, no_ack: false, callback: function (AMQPMessage $msg): void {
    echo "[worker] {$msg->getBody()}\n";
    usleep(300_000);
    $msg->ack();
});

$ch->consume();
