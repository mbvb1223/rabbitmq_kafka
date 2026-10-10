<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php publish.php — the same 7 updates as kafka/publish.php, into a stream

$ch = rabbit();
$ch->queue_declare('compact.users', durable: true, auto_delete: false,
    arguments: new AMQPTable(['x-queue-type' => 'stream']));
$ch->confirm_select();

$updates = [
    ['user-1', 'name=An v1'],
    ['user-2', 'name=Binh v1'],
    ['user-1', 'name=An v2'],
    ['user-3', 'name=Chi v1'],
    ['user-1', 'name=An v3'],
    ['user-2', 'name=Binh v2'],
    ['user-3', ''], // no tombstone concept: an empty body is just another message
];

foreach ($updates as [$key, $value]) {
    $ch->basic_publish(
        new AMQPMessage($value, [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'application_headers' => new AMQPTable(['key' => $key]),
        ]),
        exchange: '',
        routing_key: 'compact.users',
    );
}
$ch->wait_for_pending_acks(5.0);

echo 'published ' . count($updates) . " -> compact.users (stream)\n";
$ch->getConnection()->close();
