<?php

require __DIR__ . '/bootstrap.php';

// php unbind.php <team> <pattern>   the team stops receiving that slice; nothing else changes
[, $team, $pattern] = $argv + [1 => 'audit', 2 => 'orders.#'];

$ch = channel();
$ch->queue_unbind("routing.$team", EXCHANGE, $pattern);
echo "routing.$team no longer bound to $pattern\n";

$ch->getConnection()->close();
