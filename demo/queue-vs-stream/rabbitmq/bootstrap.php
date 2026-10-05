<?php

require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Wire\AMQPTable;

const QUEUE = 'demo.queue';
const STREAM = 'demo.stream';

function channel(): AMQPChannel
{
    $conn = new AMQPStreamConnection(getenv('RABBITMQ_HOST') ?: 'localhost', 5672, 'app', 'app');
    $ch = $conn->channel();

    // php-amqplib defaults (durable=false, exclusive=false) are refused by RabbitMQ 4.3
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
