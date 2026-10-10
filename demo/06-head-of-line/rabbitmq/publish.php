<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php publish.php <jobs|ordered> [count]
[, $mode, $count] = $argv + [1 => 'jobs', 2 => 10];
$name = queue($mode);

$ch = channel();
$ch->confirm_select();

for ($i = 1; $i <= (int) $count; $i++) {
    $ch->basic_publish(
        new AMQPMessage(job($i), ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
        exchange: '',
        routing_key: $name,
    );
}
$ch->wait_for_pending_acks(5.0);

echo "published jobs 1-$count -> $name (job " . BAD_JOB . " is the slow one)\n";
$ch->getConnection()->close();
