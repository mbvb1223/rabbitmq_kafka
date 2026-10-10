<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php consume.php <queue|stream> [name] [offset: first|last|next|<int>|<interval e.g. 5m, 1h>]
[, $mode, $who, $offset] = $argv + [1 => 'queue', 2 => 'worker-' . getmypid(), 3 => 'next'];
$name = target($mode);

$ch = channel();
// streams over AMQP 0-9-1 refuse consumers without a prefetch or with auto-ack
$ch->basic_qos(0, 1, false);

$args = [];
if ($mode === 'stream') {
    // a PHP int is an offset; a string like '5m' is "from 5 minutes ago"
    $args['x-stream-offset'] = ctype_digit($offset) ? (int) $offset : $offset;
}

echo "[$who] waiting on $name" . ($args ? " from offset '$offset'" : '') . "\n";

$ch->basic_consume($name, no_ack: false, arguments: new AMQPTable($args),
    callback: function (AMQPMessage $msg) use ($who, $mode): void {
        $pos = $mode === 'stream'
            ? '  (offset ' . $msg->get('application_headers')->getNativeData()['x-stream-offset'] . ')'
            : '';
        echo "[$who] {$msg->getBody()}$pos\n";
        usleep(200_000);
        // queue: deletes the message. stream: only grants credit for the next one
        $msg->ack();
    });

$ch->consume();
