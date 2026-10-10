<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

// php publish.php [count] [failures, e.g. 3:timeout,4:validation]
[, $count, $failures] = $argv + [1 => 5, 2 => '3:timeout'];

// the job carries its own failure so every run is deterministic
$fail = [];
foreach (array_filter(explode(',', $failures)) as $spec) {
    [$id, $reason] = explode(':', $spec) + [1 => 'timeout'];
    $fail[(int) $id] = $reason;
}

$ch = channel();
$ch->confirm_select();

for ($i = 1; $i <= (int) $count; $i++) {
    $headers = ['x-published-at' => (int) (microtime(true) * 1000)];
    if (isset($fail[$i])) {
        $headers['x-fail'] = $fail[$i];
    }
    $ch->basic_publish(new AMQPMessage("job $i", [
        'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        'application_headers' => new AMQPTable($headers),
    ]), exchange: '', routing_key: JOBS);
}
$ch->wait_for_pending_acks(5.0);

echo "published $count -> " . JOBS . ($fail ? ' (failing: ' . $failures . ')' : '') . "\n";
$ch->getConnection()->close();
