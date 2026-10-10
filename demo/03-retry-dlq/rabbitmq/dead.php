<?php

require __DIR__ . '/bootstrap.php';

// php dead.php [queue]   drains a dead-letter queue and prints why each job is there
$queue = $argv[1] ?? 'retry.dead';

$ch = channel();
$n = 0;
while ($msg = $ch->basic_get($queue)) {
    $h = headers($msg);
    $death = $h['x-death'][0] ?? [];
    echo "[$queue] {$msg->getBody()}  " . json_encode(array_filter([
        'x-first-death-reason' => $h['x-first-death-reason'] ?? null,
        'x-death.reason' => $death['reason'] ?? null,
        'x-death.queue' => $death['queue'] ?? null,
        'x-death.count' => $death['count'] ?? null,
        'x-delivery-count' => $h['x-delivery-count'] ?? null,
        'x-failure-reason' => $h['x-failure-reason'] ?? null,
    ], fn ($v) => $v !== null)) . "\n";
    $msg->ack();
    $n++;
}

echo "[$queue] $n message(s)\n";
$ch->getConnection()->close();
