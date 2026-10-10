<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php publish.php                 the 8 orders in ../orders.php
// php publish.php <routing-key>   one order with that key, e.g. orders.cancelled.th
$orders = isset($argv[1]) ? [9 => $argv[1]] : ORDERS;

$ch = channel();
$ch->confirm_select();
$ch->set_ack_handler(fn (AMQPMessage $m) => print("  confirmed: {$m->getRoutingKey()}\n"));
$ch->set_nack_handler(fn (AMQPMessage $m) => print("  NACK: {$m->getRoutingKey()}\n"));
// mandatory: when no queue is bound for the key, the broker hands the message back instead of dropping it
$ch->set_return_listener(function (int $code, string $text, string $exchange, string $key): void {
    echo "  returned: $key  ($code $text)\n";
});

foreach ($orders as $id => $key) {
    $ch->basic_publish(
        new AMQPMessage("order $id", ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
        exchange: EXCHANGE,
        routing_key: $key,
        mandatory: true,
    );
    echo "published order $id  $key\n";
}
$ch->wait_for_pending_acks_returns(5.0);

$ch->getConnection()->close();
