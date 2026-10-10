#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
docker rm -f share.stuck >/dev/null 2>&1
# a console consumer whose docker client died without a TTY keeps running inside the kafka container
docker compose -f ../../compose.yaml exec -T kafka pkill -f 'group share.workers' && sleep 3
../bin/rabbit rabbitmqctl delete_queue share.jobs >/dev/null 2>&1
../bin/rabbit rabbitmqctl delete_queue share.dead >/dev/null 2>&1
# a share group can only be deleted while it has no members
../bin/kafka kafka-share-groups --delete --group share.workers >/dev/null 2>&1
../bin/kafka kafka-consumer-groups --delete --group share.classic >/dev/null 2>&1
../bin/reset-topic share.jobs 1
# share groups start at "latest" by default and would miss jobs published before the first member joined
../bin/kafka kafka-configs --entity-type groups --entity-name share.workers --alter \
  --add-config share.auto.offset.reset=earliest >/dev/null && echo "group share.workers: share.auto.offset.reset=earliest"
