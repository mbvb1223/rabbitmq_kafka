#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
../bin/rabbit rabbitmqctl delete_queue demo.queue >/dev/null 2>&1
../bin/rabbit rabbitmqctl delete_queue demo.stream >/dev/null 2>&1
../bin/reset-topic demo.events 2
../bin/kafka kafka-consumer-groups --delete --group workers --group analytics --group billing --group audit >/dev/null 2>&1
../bin/kafka kafka-share-groups --delete --group share-workers >/dev/null 2>&1
true
