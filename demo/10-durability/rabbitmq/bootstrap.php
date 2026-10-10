<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Wire\AMQPTable;

const QUEUE = 'durable.orders';

// run against node 1 (RABBITMQ_PORT=5681): client-local puts the queue leader on the node you're connected to
$ch = rabbit();
$ch->queue_delete(QUEUE);
$ch->queue_declare(QUEUE, durable: true, auto_delete: false, arguments: new AMQPTable([
    'x-queue-type' => 'quorum',
    'x-queue-leader-locator' => 'client-local',
]));
echo 'declared ' . QUEUE . " (quorum, 3 replicas)\n";
