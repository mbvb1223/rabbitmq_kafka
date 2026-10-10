<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php publish.php <queue|stream> [count]
[, $mode, $count] = $argv + [1 => 'queue', 2 => 6];
$name = target($mode);

$ch = channel();
$ch->confirm_select();
// without a handler php-amqplib silently drops basic.nack
$ch->set_nack_handler(fn () => throw new RuntimeException('broker nacked a publish'));

$at = date('H:i:s');
for ($i = 1; $i <= (int) $count; $i++) {
    $ch->basic_publish(
        new AMQPMessage("event $i @ $at", ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
        exchange: '',
        routing_key: $name,
    );
}
$ch->wait_for_pending_acks(5.0);

echo "published $count -> $name\n";
$ch->getConnection()->close();
