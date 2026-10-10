#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
for q in ttl.jobs ttl.expired ttl.prio; do
  ../bin/rabbit rabbitmqctl delete_queue "$q" >/dev/null 2>&1
done
../bin/reset-topic ttl.events 1 --config retention.ms=10000
../bin/reset-topic ttl.jobs.high 1
../bin/reset-topic ttl.jobs.low 1
