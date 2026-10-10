<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php consume.php <jobs|ordered> [name] [prefetch]
[, $mode, $who, $prefetch] = $argv + [1 => 'jobs', 2 => 'worker-' . getmypid(), 3 => 1];
$name = queue($mode);

$ch = channel();
// how many unacked jobs the broker pushes to this worker before waiting for an ack
$ch->basic_qos(0, (int) $prefetch, false);

echo "[$who] waiting on $name (prefetch $prefetch)\n";

$ch->basic_consume($name, no_ack: false, callback: function (AMQPMessage $msg) use ($who): void {
    echo "[$who] " . work($msg->getBody()) . ($msg->isRedelivered() ? '  (redelivered)' : '') . "\n";
    $msg->ack();
});

$ch->consume();
