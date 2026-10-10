<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;

const QUEUE = 'qs.queue';
const STREAM = 'qs.stream';

function channel(): AMQPChannel
{
    $ch = rabbit();

    // php-amqplib defaults durable=false, auto_delete=true: 4.3 refuses the first, quorum/stream queues the second
    $ch->queue_declare(QUEUE, durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'quorum']));
    $ch->queue_declare(STREAM, durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'stream']));

    return $ch;
}

function target(string $mode): string
{
    return match ($mode) {
        'queue' => QUEUE,
        'stream' => STREAM,
        default => throw new InvalidArgumentException("mode must be queue|stream, got '$mode'"),
    };
}
