#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
../bin/rabbit rabbitmqctl delete_queue hol.jobs >/dev/null 2>&1
../bin/rabbit rabbitmqctl delete_queue hol.ordered >/dev/null 2>&1
# deleting the topic also drops the hol.workers group's committed offsets
../bin/reset-topic hol.jobs 2
../bin/reset-topic hol.dlq 1
