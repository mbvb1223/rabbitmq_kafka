<?php

require __DIR__ . '/../../lib/kafka.php';
require __DIR__ . '/../orders.php';

// php publish.php                 the 8 orders to one topic, routing.orders (event + country in headers)
// php publish.php split           each order to the topic of its slice, routing.orders.<event>.<country>
// php publish.php <routing-key>   one order to routing.<routing-key>, e.g. orders.cancelled.vn
$mode = $argv[1] ?? 'single';
$orders = in_array($mode, ['single', 'split'], true) ? ORDERS : [9 => $mode];

$conf = kafka_conf(['enable.idempotence' => 'true', 'message.timeout.ms' => 5000, 'topic.metadata.propagation.max.ms' => 3000]);
$conf->setDrMsgCb(function (RdKafka\Producer $producer, RdKafka\Message $msg): void {
    if ($msg->err) {
        echo "  failed: {$msg->errstr()}\n";
    }
});
$producer = new RdKafka\Producer($conf);

foreach ($orders as $id => $key) {
    [, $event, $country] = explode('.', $key);
    // the producer owns the layout: a new slice for a consumer means a new topic here
    $topic = $producer->newTopic($mode === 'single' ? 'routing.orders' : "routing.$key");
    $topic->producev(RD_KAFKA_PARTITION_UA, 0, "order $id", "order-$id", ['event' => $event, 'country' => $country]);
    echo "published order $id  -> {$topic->getName()}\n";
}

$producer->flush(10_000);
