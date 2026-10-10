<?php

require __DIR__ . '/../vendor/autoload.php';

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

function rabbit(): AMQPChannel
{
    // guest/guest only works from localhost inside the container, so compose creates app/app
    $conn = new AMQPStreamConnection(
        getenv('RABBITMQ_HOST') ?: 'localhost',
        (int) (getenv('RABBITMQ_PORT') ?: 5672),
        'app',
        'app',
    );

    return $conn->channel();
}
