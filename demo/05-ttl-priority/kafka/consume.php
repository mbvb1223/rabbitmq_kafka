<?php

require __DIR__ . '/../../lib/kafka.php';

// php consume.php   priority you build: check ttl.jobs.high before taking each ttl.jobs.low message
function subscriber(string $topic): RdKafka\KafkaConsumer
{
    $consumer = new RdKafka\KafkaConsumer(kafka_conf([
        'group.id' => 'ttl.workers',
        'group.protocol' => 'consumer',
        'auto.offset.reset' => 'earliest',
        'enable.auto.commit' => 'false',
    ]));
    $consumer->subscribe([$topic]);

    return $consumer;
}

function process(RdKafka\KafkaConsumer $consumer, RdKafka\Message $msg): void
{
    echo "[worker] {$msg->payload}\n";
    usleep(300_000);
    $consumer->commit($msg);
}

$running = running();
// two consumers so each has its own prefetch buffer; one consumer on both topics hands out
// the low messages it already fetched before a later high one
$high = subscriber('ttl.jobs.high');
echo "[worker] waiting for ttl.jobs.high partitions\n";
// run one worker: each topic has 1 partition, so a second worker waits here forever
while ($running() && partitions($high) === 'none (idle)') {
    if ($msg = kafka_message($high->consume(500))) {
        process($high, $msg);
    }
}
$low = subscriber('ttl.jobs.low');
echo "[worker] high: partition " . partitions($high) . ", now polling low too\n";

while ($running()) {
    if ($msg = kafka_message($high->consume(100))) {
        process($high, $msg);
        continue;
    }
    // only when high looks empty, so a steady stream of high jobs starves low forever
    // (and after max.poll.interval.ms without a consume(), the low member leaves the group)
    if ($msg = kafka_message($low->consume(100))) {
        process($low, $msg);
    }
}

$high->close();
$low->close();
