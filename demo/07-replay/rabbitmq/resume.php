<?php

require __DIR__ . '/stream.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php resume.php — over AMQP 0-9-1 the broker doesn't remember a stream consumer's position, so we do
const SAVED = __DIR__ . '/billing.offset';

$saved = is_file(SAVED) ? (int) file_get_contents(SAVED) : null;
$from = $saved === null ? 'first' : $saved + 1;
echo $saved === null
    ? "[billing] no saved offset, starting from 'first'\n"
    : "[billing] saved offset $saved, resuming from $from\n";

$ch = stream();
$ch->basic_consume(STREAM, no_ack: false, arguments: new AMQPTable(['x-stream-offset' => $from]),
    callback: function (AMQPMessage $msg): void {
        echo '[billing] ' . describe($msg) . "\n";
        usleep(1_500_000);
        // after the work: a crash before this line replays one message, it never skips one
        file_put_contents(SAVED, $msg->get('application_headers')->getNativeData()['x-stream-offset']);
        $msg->ack();
    });
$ch->consume();
