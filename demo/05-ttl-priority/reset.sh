#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
for q in ttl.jobs ttl.expired ttl.prio; do
  ../bin/rabbit rabbitmqctl delete_queue "$q" >/dev/null 2>&1
done
../bin/rabbit rabbitmqadmin -u app -p app delete exchange --name ttl.dlx >/dev/null 2>&1
../bin/reset-topic ttl.events 1 --config retention.ms=10000
../bin/reset-topic ttl.jobs.high 1
../bin/reset-topic ttl.jobs.low 1
../bin/kafka kafka-consumer-groups --delete --group ttl.workers 2>&1 \
  | grep -q 'not empty' && echo 'consume.php is still running: Ctrl-C it and rerun ./reset.sh'
true
