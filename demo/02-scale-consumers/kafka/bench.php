<?php

require __DIR__ . '/../../lib/kafka.php';
require __DIR__ . '/../fleet.php';

const TOPIC = 'scale.jobs';
const GROUP = 'scale.workers';
const JOBS = 200;
// KIP-848 members pick up assignment changes on their heartbeat, every 5 s by default
const SETTLE_SECONDS = 6;

// php bench.php <workers>
$workers = (int) ($argv[1] ?? 1);
// leftovers from an aborted run are skipped, so the count is always this run's 200
$run = uniqid();

$work = function (int $id, Closure $report) use ($run): void {
    $consumer = new RdKafka\KafkaConsumer(kafka_conf([
        'group.id' => GROUP,
        'group.protocol' => 'consumer',
        'auto.offset.reset' => 'earliest',
        'enable.auto.commit' => 'false',
    ]));
    $consumer->subscribe([TOPIC]);

    $running = running();
    $last = null;
    while ($running()) {
        $msg = kafka_message($consumer->consume(100));
        // the assignment arrives during the poll, so report it after consume()
        $parts = $consumer->getAssignment() ? partitions($consumer) : '';
        if ($parts !== $last) {
            $report("assigned $parts");
            $last = $parts;
        }
        if (!$msg) {
            continue;
        }
        if (str_starts_with($msg->payload, $run)) {
            usleep(50_000); // an I/O-bound job: an HTTP call, a DB write
            $report('done');
        }
        $consumer->commit($msg);
    }
    $consumer->close();
};

$fleet = new Fleet($workers, $work);

$producer = new RdKafka\Producer(kafka_conf(['enable.idempotence' => 'true']));
$partitions = count($producer->getMetadata(false, $producer->newTopic(TOPIC), 5000)->getTopics()->current()->getPartitions());
echo "Kafka: $workers worker(s), topic " . TOPIC . " has $partitions partitions, " . JOBS . " jobs x 50 ms\n";

// settled = every partition has an owner and nothing moved for a heartbeat or so
$fleet->waitFor(
    fn () => count($fleet->ready) === $workers && $fleet->owned() === $partitions
        && microtime(true) - $fleet->lastChange > SETTLE_SECONDS,
    60,
    fn () => sprintf('  waiting for the group to settle: %d/%d partitions owned by %d/%d workers',
        $fleet->owned(), $partitions, $fleet->owners(), $workers),
);

$start = microtime(true);
$topic = $producer->newTopic(TOPIC);
for ($i = 1; $i <= JOBS; $i++) {
    // explicit round-robin so every partition gets the same share
    $topic->produce(($i - 1) % $partitions, 0, "$run job $i");
}
if ($producer->flush(10_000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('flush timed out');
}

$fleet->waitFor(fn () => $fleet->total() >= JOBS, 60,
    fn () => sprintf('  %d/%d done', $fleet->total(), JOBS));
$seconds = microtime(true) - $start;
$fleet->stop();
$fleet->report($seconds, 'jobs per worker:');
