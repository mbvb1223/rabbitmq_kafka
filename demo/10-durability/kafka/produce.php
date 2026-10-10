<?php

require __DIR__ . '/../../lib/kafka.php';

// php produce.php <topic> <acks: 1|all> [seconds] [messages per second]
[, $topic, $acks, $seconds, $rate] = $argv + [1 => 'durable.acks1', 2 => '1', 3 => 30, 4 => 10_000];

// every id the broker acknowledged; verify.php checks they are all still in the topic
$log = fopen(__DIR__ . "/acked-$topic.log", 'w');
$acked = $failed = 0;
$lastError = '';

$conf = kafka_conf([
    'acks' => $acks,
    // idempotence requires acks=all; without it retries can duplicate, so verify.php counts unique ids
    'enable.idempotence' => $acks === 'all' ? 'true' : 'false',
    'message.timeout.ms' => 30_000,
]);
$conf->setDrMsgCb(function (RdKafka\Producer $p, RdKafka\Message $m) use ($log, &$acked, &$failed, &$lastError): void {
    if ($m->err) {
        $failed++;
        $lastError = rd_kafka_err2str($m->err);
        return;
    }
    $acked++;
    fwrite($log, "$m->payload\n");
});

$producer = new RdKafka\Producer($conf);
$out = $producer->newTopic($topic);
$leader = $producer->getMetadata(false, $out, 5_000)->getTopics()->current()->getPartitions()->current()->getLeader();
echo "producing to $topic with acks=$acks, partition 0 leader: kafka-$leader\n";

$running = running();
$start = microtime(true);
$sent = 0;
$report = 1;
while ($running() && ($elapsed = microtime(true) - $start) < $seconds) {
    try {
        while ($sent < $elapsed * $rate) {
            $out->produce(RD_KAFKA_PARTITION_UA, 0, (string) ($sent + 1));
            $sent++;
        }
    } catch (RdKafka\Exception) {
        // local queue full while the leader is gone: let delivery reports drain
    }
    $producer->poll(10);
    if ($elapsed >= $report) {
        printf("[%2ds] sent %d, acked %d, failed %d%s\n", $report++, $sent, $acked, $failed, $lastError ? "  last error: $lastError" : '');
        $lastError = '';
    }
}

$producer->flush(35_000);
printf("done: sent %d, acked %d, failed %d -> kafka/acked-%s.log\n", $sent, $acked, $failed, $topic);
