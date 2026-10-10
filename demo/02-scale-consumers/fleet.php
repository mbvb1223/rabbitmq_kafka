<?php

// The bench harness both sides share: fork N worker processes (PHP's one-process-one-consumer
// model) and let each one report back to the parent over a socket pair, one line per event:
// "ready", "assigned 0,3" (Kafka only), "done".
final class Fleet
{
    public array $ready = [];    // worker id => true
    public array $done = [];     // worker id => jobs processed
    public array $assigned = []; // worker id => partitions (Kafka only)
    public float $lastChange;    // when an assignment last changed

    private array $pids = [];
    private array $socks = [];
    private array $buffers = [];

    // $work(int $id, Closure $report) runs in the child; it must return when it gets SIGTERM.
    // Create clients inside $work: librdkafka threads and AMQP sockets don't survive a fork.
    public function __construct(int $workers, Closure $work, float $stagger = 0)
    {
        for ($id = 1; $id <= $workers; $id++) {
            if ($id > 1) {
                usleep((int) ($stagger * 1_000_000));
            }
            [$parent, $child] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();
            if ($pid === 0) {
                fclose($parent);
                $work($id, fn (string $event) => fwrite($child, "$event\n"));
                exit(0);
            }
            fclose($child);
            stream_set_blocking($parent, false);
            $this->pids[$id] = $pid;
            $this->socks[$id] = $parent;
            $this->buffers[$id] = '';
            $this->done[$id] = 0;
        }
        $this->lastChange = microtime(true);

        // docker's --init forwards Ctrl-C to this process only, so pass it on to the workers
        pcntl_async_signals(true);
        pcntl_signal(SIGINT, function () {
            $this->stop();
            exit(130);
        });
    }

    // Reads worker reports until $until() is true; $progress() is printed once a second
    public function waitFor(Closure $until, float $timeout, ?Closure $progress = null): void
    {
        $deadline = microtime(true) + $timeout;
        $tick = 0;
        while (!$until()) {
            if (microtime(true) > $deadline) {
                $this->stop();
                throw new RuntimeException("timed out after {$timeout}s");
            }
            if ($progress && time() !== $tick) {
                $tick = time();
                echo "\r", str_pad($progress(), 79);
            }
            $read = $this->socks;
            $write = $except = null;
            if (!@stream_select($read, $write, $except, 0, 100_000)) {
                continue;
            }
            foreach ($read as $id => $sock) {
                $this->buffers[$id] .= fread($sock, 65536);
                while (($nl = strpos($this->buffers[$id], "\n")) !== false) {
                    $this->handle($id, substr($this->buffers[$id], 0, $nl));
                    $this->buffers[$id] = substr($this->buffers[$id], $nl + 1);
                }
            }
        }
        if ($progress) {
            echo "\r", str_pad($progress(), 79), "\n";
        }
    }

    public function stop(): void
    {
        foreach ($this->pids as $pid) {
            posix_kill($pid, SIGTERM);
        }
        foreach ($this->pids as $pid) {
            pcntl_waitpid($pid, $status);
        }
        $this->pids = [];
    }

    public function owned(): int
    {
        return count(array_filter(explode(',', implode(',', $this->assigned)), 'strlen'));
    }

    public function owners(): int
    {
        return count(array_filter($this->assigned, fn (string $p) => $p !== ''));
    }

    public function total(): int
    {
        return array_sum($this->done);
    }

    public function report(float $seconds, string $title): void
    {
        $max = max(1, ...array_values($this->done));
        echo "\n$title\n";
        foreach ($this->done as $id => $jobs) {
            // compare with '', not falsiness: partition "0" is falsy in PHP
            $parts = isset($this->assigned[$id]) ? '  partitions: ' . ($this->assigned[$id] !== '' ? $this->assigned[$id] : '-') : '';
            printf("  w%02d %4d %-25s%s\n", $id, $jobs, str_repeat('#', (int) round($jobs / $max * 25)), $parts);
        }
        $busy = count(array_filter($this->done));
        printf("\n  busy %d/%d, idle %d   %d jobs in %.2f s = %d jobs/s\n\n",
            $busy, count($this->done), count($this->done) - $busy, $this->total(), $seconds, $this->total() / $seconds);
    }

    private function handle(int $id, string $line): void
    {
        [$event, $arg] = explode(' ', $line, 2) + [1 => ''];
        match ($event) {
            'ready' => $this->ready[$id] = true,
            'done' => $this->done[$id]++,
            'assigned' => $this->assign($id, $arg),
        };
    }

    private function assign(int $id, string $partitions): void
    {
        $this->ready[$id] = true;
        if (($this->assigned[$id] ?? null) !== $partitions) {
            $this->assigned[$id] = $partitions;
            $this->lastChange = microtime(true);
        }
    }
}
