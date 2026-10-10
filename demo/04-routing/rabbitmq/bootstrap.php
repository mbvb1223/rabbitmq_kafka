<?php

require __DIR__ . '/../../lib/rabbitmq.php';
require __DIR__ . '/../orders.php';

use PhpAmqpLib\Channel\AMQPChannel;

const EXCHANGE = 'routing.orders';

function channel(): AMQPChannel
{
    $ch = rabbit();
    $ch->exchange_declare(EXCHANGE, 'topic', durable: true, auto_delete: false);

    return $ch;
}
