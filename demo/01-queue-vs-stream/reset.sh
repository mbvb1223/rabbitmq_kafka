#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
# a console consumer whose docker client died without a TTY keeps running inside the kafka container
docker compose -f ../../compose.yaml exec -T kafka pkill -f 'group qs.share' && sleep 3
../bin/rabbit rabbitmqctl delete_queue qs.queue >/dev/null 2>&1
../bin/rabbit rabbitmqctl delete_queue qs.stream >/dev/null 2>&1
../bin/reset-topic qs.events 2
../bin/kafka kafka-consumer-groups --delete --group qs.workers --group qs.analytics --group qs.billing --group qs.audit 2>&1 \
  | grep -q 'not empty' && echo 'a consumer is still running: Ctrl-C it and rerun ./reset.sh'
../bin/kafka kafka-share-groups --delete --group qs.share >/dev/null 2>&1
true
