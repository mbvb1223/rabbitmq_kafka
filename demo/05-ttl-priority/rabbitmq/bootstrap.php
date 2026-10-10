<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;

const JOBS = 'ttl.jobs';
const EXPIRED = 'ttl.expired';
const PRIO = 'ttl.prio';

function channel(): AMQPChannel
{
    $ch = rabbit();

    $ch->exchange_declare('ttl.dlx', 'fanout', durable: true, auto_delete: false);
    $ch->queue_declare(EXPIRED, durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'quorum']));
    $ch->queue_bind(EXPIRED, 'ttl.dlx');

    // queue-level default TTL; a per-message `expiration` can only make it shorter
    $ch->queue_declare(JOBS, durable: true, auto_delete: false, arguments: new AMQPTable([
        'x-queue-type' => 'quorum',
        'x-message-ttl' => 10_000,
        'x-dead-letter-exchange' => 'ttl.dlx',
    ]));

    // 4.3: quorum queues have 32 priorities (0-31) built in, no x-max-priority needed
    $ch->queue_declare(PRIO, durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'quorum']));

    return $ch;
}
