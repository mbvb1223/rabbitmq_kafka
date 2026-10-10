<?php

require __DIR__ . '/../../lib/kafka.php';

// php filter.php <team> <pattern>   * and # like a RabbitMQ binding (approximation: # here needs at least one word), matched here in PHP
[, $team, $pattern] = $argv + [1 => 'vn-team', 2 => 'orders.*.vn'];
$regex = '/^' . strtr(preg_quote($pattern, '/'), ['\*' => '[^.]+', '\#' => '.*']) . '$/';

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => "routing.filter.$team",
    'group.protocol' => 'consumer',
    'auto.offset.reset' => 'earliest',
    'enable.auto.commit' => 'false',
]));
$consumer->subscribe(['routing.orders']);
echo "[$team] reading ALL of routing.orders, keeping $pattern\n";

$running = running();
$read = $kept = $reported = 0;
while ($running()) {
    $msg = kafka_message($consumer->consume(1000));
    if (!$msg) {
        if ($read !== $reported) {
            echo "[$team] read $read, kept $kept\n";
            $reported = $read;
        }
        continue;
    }

    // every order crossed the network and got decoded before we could look at it
    $read++;
    $key = "orders.{$msg->headers['event']}.{$msg->headers['country']}";
    if (preg_match($regex, $key)) {
        $kept++;
        echo "[$team] kept  {$msg->payload}  $key\n";
    } else {
        echo "[$team]   skip {$msg->payload}  $key\n";
    }
    $consumer->commit($msg);
}

$consumer->close();
