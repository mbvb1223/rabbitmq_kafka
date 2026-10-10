<?php

require __DIR__ . '/../../lib/kafka.php';

// php verify.php <topic>: every acked id must still be in the topic
$topic = $argv[1] ?? 'durable.acks1';

$acked = array_flip(file(__DIR__ . "/acked-$topic.log", FILE_IGNORE_NEW_LINES));

// php-rdkafka wants a group.id even for assign(); nothing is committed
$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => 'durable.verify',
    'enable.auto.commit' => 'false',
    'enable.partition.eof' => 'true',
]));
$consumer->assign([new RdKafka\TopicPartition($topic, 0, RD_KAFKA_OFFSET_BEGINNING)]);

$present = [];
$records = 0;
while (true) {
    $msg = $consumer->consume(10_000);
    if ($msg === null || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF || $msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
        break;
    }
    if ($msg->err) {
        throw new RuntimeException($msg->errstr());
    }
    $present[$msg->payload] = true;
    $records++;
}
$consumer->close();

$lost = array_keys(array_diff_key($acked, $present));
sort($lost);
printf("acked %d, present %d (%d duplicates), LOST %d\n", count($acked), count($present), $records - count($present), count($lost));
if ($lost) {
    echo 'lost ids: ' . implode(', ', array_slice($lost, 0, 10)) . (count($lost) > 10 ? ', ...' : '') . "\n";
}
