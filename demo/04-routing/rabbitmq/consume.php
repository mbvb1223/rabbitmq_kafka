<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php consume.php <team> <pattern>...   * = one word, # = zero or more words
[, $team] = $argv + [1 => 'audit'];
$patterns = array_slice($argv, 2) ?: ['orders.#'];
$queue = "routing.$team";

$ch = channel();
$ch->queue_declare($queue, durable: true, auto_delete: false,
    arguments: new AMQPTable(['x-queue-type' => 'quorum']));
// the consumer subscribes itself; the publisher and the other teams don't change
foreach ($patterns as $pattern) {
    $ch->queue_bind($queue, EXCHANGE, $pattern);
}
$ch->basic_qos(0, 10, false);

echo "[$team] queue $queue bound to " . implode(', ', $patterns) . "\n";

$ch->basic_consume($queue, no_ack: false, callback: function (AMQPMessage $msg) use ($team): void {
    echo "[$team] {$msg->getBody()}  {$msg->getRoutingKey()}\n";
    $msg->ack();
});

$ch->consume();
