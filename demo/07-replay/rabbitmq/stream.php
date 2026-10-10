<?php

require __DIR__ . '/../../lib/rabbitmq.php';

date_default_timezone_set('UTC');

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

const STREAM = 'replay.events';

function stream(): AMQPChannel
{
    $ch = rabbit();
    // retention is the replay limit: segments older than 7 days are deleted, read or not
    $ch->queue_declare(STREAM, durable: true, auto_delete: false, arguments: new AMQPTable([
        'x-queue-type' => 'stream',
        'x-max-age' => '7D',
    ]));
    // streams over AMQP 0-9-1 refuse consumers without a prefetch or with auto-ack
    $ch->basic_qos(0, 10, false);

    return $ch;
}

function describe(AMQPMessage $msg): string
{
    $offset = $msg->get('application_headers')->getNativeData()['x-stream-offset'];

    return "{$msg->getBody()}  (offset $offset, published " . date('H:i:s', $msg->get('timestamp')) . ')';
}
