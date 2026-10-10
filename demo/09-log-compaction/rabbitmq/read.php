<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php read.php — replays the stream from the first offset, exits when idle for 2 s

$ch = rabbit();
$ch->basic_qos(0, 100, false);

$count = 0;
$ch->basic_consume('compact.users', no_ack: false, arguments: new AMQPTable(['x-stream-offset' => 'first']),
    callback: function (AMQPMessage $msg) use (&$count): void {
        $headers = $msg->get('application_headers')->getNativeData();
        printf("offset %2d  %-7s %s\n", $headers['x-stream-offset'], $headers['key'], $msg->getBody() ?: '(empty)');
        $count++;
        $msg->ack();
    });

try {
    while (true) {
        $ch->wait(timeout: 2);
    }
} catch (AMQPTimeoutException) {
}

echo "$count records: every version is still there\n";
$ch->getConnection()->close();
