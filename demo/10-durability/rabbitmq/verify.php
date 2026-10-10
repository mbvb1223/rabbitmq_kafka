<?php

require __DIR__ . '/../../lib/rabbitmq.php';

use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

// RABBITMQ_PORT=5682 php verify.php: drains durable.orders, every confirmed id must be there
$confirmed = array_flip(file(__DIR__ . '/confirmed-durable.orders.log', FILE_IGNORE_NEW_LINES));

$present = [];
$records = 0;
$ch = rabbit();
$ch->basic_qos(0, 1000, false);
$ch->basic_consume('durable.orders', callback: function (AMQPMessage $m) use (&$present, &$records): void {
    $present[$m->getBody()] = true;
    $records++;
    $m->ack();
});
try {
    while (true) {
        $ch->wait(timeout: 3);
    }
} catch (AMQPTimeoutException) {
    // 3 s without a message: the queue is drained
}

$lost = array_keys(array_diff_key($confirmed, $present));
sort($lost);
printf("confirmed %d, in queue %d (%d duplicates), LOST %d\n", count($confirmed), count($present), $records - count($present), count($lost));
if ($lost) {
    echo 'lost ids: ' . implode(', ', array_slice($lost, 0, 10)) . (count($lost) > 10 ? ', ...' : '') . "\n";
}
