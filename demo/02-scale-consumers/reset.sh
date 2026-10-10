#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
../bin/rabbit rabbitmqctl delete_queue scale.jobs >/dev/null 2>&1
# also drops scale.workers' committed offsets, and undoes the --alter from Kafka Act 2
../bin/reset-topic scale.jobs 4
../bin/kafka kafka-consumer-groups --delete --group scale.workers >/dev/null 2>&1
true
