<?php

// runs inside the demo-kafka-php container (see bin/kafka-php), so the broker is kafka:19092
function kafka_conf(array $settings = []): RdKafka\Conf
{
    $conf = new RdKafka\Conf();
    $conf->set('bootstrap.servers', getenv('KAFKA_BOOTSTRAP') ?: 'kafka:19092');
    foreach ($settings as $key => $value) {
        $conf->set($key, (string) $value);
    }

    return $conf;
}

// Returns a "still running?" check. Without close(), a killed consumer keeps its
// partitions until the 45 s session timeout and new consumers sit idle meanwhile.
function running(): Closure
{
    $running = true;
    pcntl_async_signals(true);
    $stop = function () use (&$running) { $running = false; };
    pcntl_signal(SIGINT, $stop);
    pcntl_signal(SIGTERM, $stop);

    return function () use (&$running): bool { return $running; };
}

// null when there is nothing to process. php-rdkafka 7 returns null on timeout, 6.x an error message
function kafka_message(?RdKafka\Message $msg): ?RdKafka\Message
{
    if ($msg === null || in_array($msg->err, [RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__PARTITION_EOF], true)) {
        return null;
    }
    if ($msg->err) {
        throw new RuntimeException($msg->errstr());
    }

    return $msg;
}

function partitions(RdKafka\KafkaConsumer $consumer): string
{
    $parts = array_map(fn (RdKafka\TopicPartition $tp) => $tp->getPartition(), $consumer->getAssignment());

    return $parts ? implode(', ', $parts) : 'none (idle)';
}
