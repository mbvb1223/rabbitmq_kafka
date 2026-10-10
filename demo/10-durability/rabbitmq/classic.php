<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// RABBITMQ_PORT=5681 php classic.php declare   (the queue lives on node 1 only)
// RABBITMQ_PORT=5683 php classic.php publish   (5 persistent messages with confirms)
$ch = rabbit();
if (($argv[1] ?? 'publish') === 'declare') {
    $ch->queue_delete('durable.classic');
    $ch->queue_declare('durable.classic', durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'classic']));
    echo "declared durable.classic on port " . getenv('RABBITMQ_PORT') . "\n";
    exit;
}

$acks = $nacks = 0;
$ch->confirm_select();
$ch->set_ack_handler(function () use (&$acks): void { $acks++; });
$ch->set_nack_handler(function () use (&$nacks): void { $nacks++; });
for ($i = 1; $i <= 5; $i++) {
    $ch->basic_publish(new AMQPMessage("order $i", ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
        routing_key: 'durable.classic');
}
$ch->wait_for_pending_acks(5.0);
echo "confirmed $acks, nacked $nacks\n";

try {
    [, $count] = $ch->queue_declare('durable.classic', passive: true);
    echo "durable.classic holds $count messages\n";
} catch (Throwable $e) {
    echo substr($e->getMessage(), 0, 110) . "\n";
}
