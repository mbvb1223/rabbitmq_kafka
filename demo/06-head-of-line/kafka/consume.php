<?php

require __DIR__ . '/../../lib/kafka.php';
require __DIR__ . '/../job.php';

// php consume.php [name] [slow|poison|dlq]
[, $who, $mode] = $argv + [1 => 'worker-' . getmypid(), 2 => 'slow'];

$consumer = new RdKafka\KafkaConsumer(kafka_conf([
    'group.id' => 'hol.workers',
    'group.protocol' => 'consumer',
    'auto.offset.reset' => 'earliest',
    'enable.auto.commit' => 'false',
]));
$consumer->subscribe(['hol.jobs']);

$producer = new RdKafka\Producer(kafka_conf(['enable.idempotence' => 'true']));
$dlq = $producer->newTopic('hol.dlq');

echo "[$who] group 'hol.workers' waiting on hol.jobs ($mode)\n";

$running = running();
$last = null;
while ($running()) {
    $msg = kafka_message($consumer->consume(1000));

    if (($parts = partitions($consumer)) !== $last) {
        echo "[$who] partitions: $parts\n";
        $last = $parts;
    }
    if (!$msg) {
        continue;
    }

    try {
        echo "[$who] " . work($msg->payload, poison: $mode !== 'slow') . "  (partition {$msg->partition}, offset {$msg->offset})\n";
    } catch (RuntimeException $e) {
        if ($mode === 'poison') {
            echo "[$who] CRASH on {$e->getMessage()} at partition {$msg->partition}, offset {$msg->offset}. Exit without commit\n";
            // a real crash skips close() and holds the partitions for the 45 s session timeout; leave cleanly so the restart is quick
            $consumer->close();
            exit(1);
        }
        // dlq: park the job and commit past it, so the partition moves again
        $dlq->produce(RD_KAFKA_PARTITION_UA, 0, $msg->payload, $msg->key);
        $producer->flush(10_000);
        echo "[$who] {$e->getMessage()}: parked in hol.dlq, committing past offset {$msg->offset}\n";
    }
    $consumer->commit($msg);
}

$consumer->close();
