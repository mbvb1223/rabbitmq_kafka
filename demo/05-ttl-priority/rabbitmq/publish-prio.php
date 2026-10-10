<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php publish-prio.php <count> <priority 0-31>
[, $count, $priority] = $argv + [1 => 10, 2 => 0];

$ch = channel();
$ch->confirm_select();

for ($i = 1; $i <= (int) $count; $i++) {
    $ch->basic_publish(new AMQPMessage("p$priority job $i", [
        'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        'priority' => (int) $priority,
    ]), exchange: '', routing_key: PRIO);
}
$ch->wait_for_pending_acks(5.0);

echo "published $count with priority $priority -> " . PRIO . "\n";
$ch->getConnection()->close();
