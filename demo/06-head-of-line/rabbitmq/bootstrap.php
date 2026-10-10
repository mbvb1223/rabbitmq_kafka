<?php

require __DIR__ . '/../../lib/rabbitmq.php';
require __DIR__ . '/../job.php';

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;

function channel(): AMQPChannel
{
    $ch = rabbit();

    $ch->queue_declare('hol.jobs', durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'quorum']));
    // queue arguments are immutable, so single active consumer needs its own queue
    $ch->queue_declare('hol.ordered', durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'quorum', 'x-single-active-consumer' => true]));

    return $ch;
}

function queue(string $mode): string
{
    return match ($mode) {
        'jobs' => 'hol.jobs',
        'ordered' => 'hol.ordered',
        default => throw new InvalidArgumentException("mode must be jobs|ordered, got '$mode'"),
    };
}
