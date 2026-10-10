#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
../bin/rabbit rabbitmqctl delete_queue replay.events >/dev/null 2>&1
rm -f rabbitmq/billing.offset
../bin/reset-topic replay.events 1
