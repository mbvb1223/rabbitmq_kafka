<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php publish.php [count] [poison job number]
[, $count, $poison] = $argv + [1 => 9, 2 => 0];

$ch = channel();
$ch->confirm_select();

$at = date('H:i:s');
for ($i = 1; $i <= (int) $count; $i++) {
    $body = "job $i @ $at" . ($i === (int) $poison ? ' (poison)' : '');
    $ch->basic_publish(new AMQPMessage($body, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
        exchange: '', routing_key: JOBS);
}
$ch->wait_for_pending_acks(5.0);

echo "published $count -> " . JOBS . ($poison ? " (job $poison is poison)" : '') . "\n";
$ch->getConnection()->close();
