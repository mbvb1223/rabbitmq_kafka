#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
for q in $(../bin/rabbit rabbitmqctl list_queues -q --no-table-headers name | grep '^routing\.'); do
  ../bin/rabbit rabbitmqctl delete_queue "$q" >/dev/null
done
for g in $(../bin/kafka kafka-consumer-groups --list | grep '^routing\.'); do
  ../bin/kafka kafka-consumer-groups --delete --group "$g" >/dev/null
done
../bin/kafka kafka-topics --delete --if-exists --topic routing.orders.cancelled.vn
../bin/reset-topic routing.orders 1
for slice in created.vn created.us paid.vn paid.us refunded.vn refunded.us; do
  ../bin/reset-topic "routing.orders.$slice" 1
done
