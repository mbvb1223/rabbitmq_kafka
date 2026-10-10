<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Exception\AMQPExceptionInterface;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

// RABBITMQ_PORT=5683 php publish.php [seconds] [messages per second]
[, $seconds, $rate] = $argv + [1 => 30, 2 => 2_000];

// every id the broker confirmed; verify.php checks they are all still in the queue
$log = fopen(__DIR__ . '/confirmed-durable.orders.log', 'w');
$confirmed = $nacked = 0;

$ch = rabbit();
$ch->confirm_select();
$ch->set_ack_handler(function (AMQPMessage $m) use ($log, &$confirmed): void {
    $confirmed++;
    fwrite($log, $m->getBody() . "\n");
});
$ch->set_nack_handler(function () use (&$nacked): void { $nacked++; });
echo 'publishing to durable.orders via port ' . getenv('RABBITMQ_PORT') . " with publisher confirms\n";

$start = microtime(true);
$sent = 0;
$report = 1;
try {
    while (($elapsed = microtime(true) - $start) < $seconds) {
        while ($sent < $elapsed * $rate) {
            $sent++;
            $ch->basic_publish(new AMQPMessage((string) $sent, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
                routing_key: 'durable.orders');
        }
        try {
            $ch->wait_for_pending_acks(0.05);
        } catch (AMQPTimeoutException) {
            // no majority, or a leader election in progress: confirms are late, keep going
        }
        if ($elapsed >= $report) {
            printf("[%2ds] sent %d, confirmed %d, nacked %d, waiting %d\n", $report++, $sent, $confirmed, $nacked, $sent - $confirmed - $nacked);
        }
    }
    $ch->wait_for_pending_acks(10.0);
} catch (AMQPTimeoutException) {
} catch (AMQPExceptionInterface $e) {
    // the node we were connected to died: whatever wasn't confirmed is the app's to retry
    echo 'connection lost: ' . $e::class . "\n";
}
printf("done: sent %d, confirmed %d, nacked %d, never confirmed %d -> rabbitmq/confirmed-durable.orders.log\n",
    $sent, $confirmed, $nacked, $sent - $confirmed - $nacked);
