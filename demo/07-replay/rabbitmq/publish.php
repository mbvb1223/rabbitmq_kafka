<?php

require __DIR__ . '/stream.php';

use PhpAmqpLib\Message\AMQPMessage;

// php publish.php [count]
$count = (int) ($argv[1] ?? 3);

$ch = stream();
$ch->confirm_select();

$at = date('H:i:s');
for ($i = 1; $i <= $count; $i++) {
    $ch->basic_publish(new AMQPMessage("event $i @ $at", [
        'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        'timestamp' => time(),
    ]), exchange: '', routing_key: STREAM);
}
$ch->wait_for_pending_acks(5.0);

echo "published $count -> " . STREAM . "\n";
$ch->getConnection()->close();
