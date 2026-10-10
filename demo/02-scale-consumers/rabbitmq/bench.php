<?php

require __DIR__ . '/../../lib/rabbitmq.php';
require __DIR__ . '/../fleet.php';

use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

const QUEUE = 'scale.jobs';
const JOBS = 200;

// php bench.php <workers> [--prefetch=N] [--backlog]
[$workers, $prefetch, $backlog] = [1, 10, false];
foreach (array_slice($argv, 1) as $arg) {
    match (true) {
        str_starts_with($arg, '--prefetch=') => $prefetch = (int) substr($arg, 11),
        $arg === '--backlog' => $backlog = true,
        default => $workers = (int) $arg,
    };
}
// leftovers from an aborted run are skipped, so the count is always this run's 200
$run = uniqid();

function declare_queue(): PhpAmqpLib\Channel\AMQPChannel
{
    $ch = rabbit();
    $ch->queue_declare(QUEUE, durable: true, auto_delete: false,
        arguments: new AMQPTable(['x-queue-type' => 'quorum']));

    return $ch;
}

function publish(string $run): void
{
    $ch = declare_queue();
    $ch->confirm_select();
    for ($i = 1; $i <= JOBS; $i++) {
        $ch->basic_publish(new AMQPMessage("$run job $i"), routing_key: QUEUE);
    }
    $ch->wait_for_pending_acks(10.0);
    $ch->getConnection()->close();
}

$work = function (int $id, Closure $report) use ($prefetch, $run): void {
    $running = true;
    pcntl_signal(SIGTERM, function () use (&$running) { $running = false; });
    pcntl_signal(SIGINT, function () use (&$running) { $running = false; });

    $ch = declare_queue();
    // 0 = unlimited: the broker pushes as many unacked messages as this consumer can buffer
    $ch->basic_qos(0, $prefetch, false);
    $ch->basic_consume(QUEUE, no_ack: false, callback: function (AMQPMessage $msg) use ($run, $report) {
        if (str_starts_with($msg->getBody(), $run)) {
            usleep(50_000); // an I/O-bound job: an HTTP call, a DB write
            $report('done');
        }
        $msg->ack();
    });
    $report('ready');

    while ($running) {
        try {
            $ch->wait(timeout: 0.2);
        } catch (AMQPTimeoutException) {
        }
    }
    $ch->getConnection()->close();
};

// once, before forking: 20 workers racing to create a fresh quorum queue get their consumers cancelled
declare_queue()->getConnection()->close();

echo "RabbitMQ: $workers worker(s), prefetch $prefetch, " . JOBS . " jobs x 50 ms" . ($backlog ? ', backlog published first' : '') . "\n";

if ($backlog) {
    publish($run);
    $start = microtime(true);
    // workers join one by one, like an autoscaler reacting to a backlog
    $fleet = new Fleet($workers, $work, stagger: 0.1);
} else {
    $fleet = new Fleet($workers, $work);
    $fleet->waitFor(fn () => count($fleet->ready) === $workers, 30);
    $start = microtime(true);
    publish($run);
}

$fleet->waitFor(fn () => $fleet->total() >= JOBS, 60,
    fn () => sprintf('  %d/%d done', $fleet->total(), JOBS));
$seconds = microtime(true) - $start;
$fleet->stop();
$fleet->report($seconds, 'jobs per worker:');
