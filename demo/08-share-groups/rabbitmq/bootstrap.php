<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;

const JOBS = 'share.jobs';
const DEAD = 'share.dead';

function channel(): AMQPChannel
{
    $ch = rabbit();

    $ch->exchange_declare('share.dlx', 'fanout', durable: true, auto_delete: false);
    $ch->queue_declare(DEAD, durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'quorum']));
    $ch->queue_bind(DEAD, 'share.dlx');

    // same number as Kafka's default share.delivery.count.limit, but 5 returns = 6 deliveries; here the message gets a destination
    $ch->queue_declare(JOBS, durable: true, auto_delete: false, arguments: new AMQPTable([
        'x-queue-type' => 'quorum',
        'x-delivery-limit' => 5,
        'x-dead-letter-exchange' => 'share.dlx',
    ]));

    return $ch;
}
