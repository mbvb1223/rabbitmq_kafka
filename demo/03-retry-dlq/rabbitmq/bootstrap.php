<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

const JOBS = 'retry.jobs';
const QUORUM = ['x-queue-type' => 'quorum'];
const DELIVERY_LIMIT = 3;

function channel(): AMQPChannel
{
    $ch = rabbit();

    // the whole retry policy: 5 queue arguments, no extra code
    $ch->queue_declare(JOBS, durable: true, auto_delete: false, arguments: new AMQPTable(QUORUM + [
        'x-delivery-limit' => DELIVERY_LIMIT,
        'x-delayed-retry-type' => 'failed',
        'x-delayed-retry-min' => 3000,
        'x-delayed-retry-max' => 10000,
        'x-dead-letter-exchange' => 'retry.dlx',
    ]));

    $ch->exchange_declare('retry.dlx', 'fanout', durable: true, auto_delete: false);
    $ch->queue_declare('retry.dead', durable: true, auto_delete: false, arguments: new AMQPTable(QUORUM));
    $ch->queue_bind('retry.dead', 'retry.dlx');

    // Act 3: one DLQ per failure reason, chosen by a header the consumer sets.
    // A reason with no binding falls through to retry.dead instead of being dropped.
    $ch->exchange_declare('retry.dlq', 'headers', durable: true, auto_delete: false,
        arguments: new AMQPTable(['alternate-exchange' => 'retry.dlx']));
    foreach (['validation', 'timeout'] as $reason) {
        $ch->queue_declare("retry.dlq.$reason", durable: true, auto_delete: false, arguments: new AMQPTable(QUORUM));
        // plain 'all' ignores x-* headers, so the binding would match every message
        $ch->queue_bind("retry.dlq.$reason", 'retry.dlq',
            arguments: new AMQPTable(['x-match' => 'all-with-x', 'x-failure-reason' => $reason]));
    }

    return $ch;
}

function headers(AMQPMessage $msg): array
{
    return $msg->has('application_headers') ? $msg->get('application_headers')->getNativeData() : [];
}

// seconds since the job was published, so the backoff gaps are readable
function since(array $headers): string
{
    return sprintf('+%5.1fs', microtime(true) - $headers['x-published-at'] / 1000);
}
