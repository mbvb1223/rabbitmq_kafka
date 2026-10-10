<?php

require __DIR__ . '/stream.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php consume.php [first|last|next|<offset>|<interval: 30s, 5m, 1h>|@<time: "@-2 minutes", @05:40>]
$from = $argv[1] ?? 'next';
$offset = match (true) {
    // a PHP int is always an offset, even when it looks like a unix time
    ctype_digit($from) => (int) $from,
    // AMQPTable sends a DateTimeInterface as an AMQP timestamp (seconds)
    str_starts_with($from, '@') => new DateTimeImmutable(substr($from, 1)),
    default => $from,
};

$ch = stream();
$shown = $offset instanceof DateTimeInterface ? $offset->format('Y-m-d H:i:s') . ' (timestamp)' : var_export($offset, true);
echo "[audit] attaching to " . STREAM . " at $shown\n";

$ch->basic_consume(STREAM, no_ack: false, arguments: new AMQPTable(['x-stream-offset' => $offset]),
    callback: function (AMQPMessage $msg): void {
        echo '[audit] ' . describe($msg) . "\n";
        $msg->ack();
    });
$ch->consume();
