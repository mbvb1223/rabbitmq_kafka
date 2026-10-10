<?php

require __DIR__ . '/bootstrap.php';

use PhpAmqpLib\Message\AMQPMessage;

// php publish-ttl.php <ttl-ms|->...   one message per argument; "-" = no expiration, the queue's 10 s applies
$ttls = array_slice($argv, 1) ?: ['3000'];

$ch = channel();
$ch->confirm_select();

foreach ($ttls as $i => $ttl) {
    $props = ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT, 'timestamp' => time()];
    if ($ttl !== '-') {
        $props['expiration'] = $ttl; // a string of milliseconds, per message
    }
    $body = 'job ' . ($i + 1) . ' (ttl ' . ($ttl === '-' ? 'from queue: 10000' : $ttl) . ' ms), published ' . date('H:i:s');
    $ch->basic_publish(new AMQPMessage($body, $props), exchange: '', routing_key: JOBS);
    echo "$body -> " . JOBS . "\n";
}
$ch->wait_for_pending_acks(5.0);
$ch->getConnection()->close();
