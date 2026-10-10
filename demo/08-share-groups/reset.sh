#!/bin/sh
# Deletes this demo's queues, exchange and groups and recreates topic share.jobs; the PHP scripts redeclare the rest
cd "$(dirname "$0")"
docker rm -f share.stuck >/dev/null 2>&1
# a console consumer whose docker client died without a TTY keeps running inside the kafka container
docker compose -f ../../compose.yaml exec -T kafka pkill -f 'group share.workers' && sleep 3
../bin/rabbit rabbitmqctl delete_queue share.jobs >/dev/null 2>&1
../bin/rabbit rabbitmqctl delete_queue share.dead >/dev/null 2>&1
../bin/rabbit rabbitmqadmin -u app -p app delete exchange --name share.dlx >/dev/null 2>&1
# a share.stuck removed <45 s ago is still a member, and a share group with members can't be deleted (the tool exits 0 either way)
while ../bin/kafka kafka-share-groups --delete --group share.workers 2>&1 | grep -q 'not EMPTY'; do sleep 5; done
../bin/kafka kafka-consumer-groups --delete --group share.classic >/dev/null 2>&1
../bin/reset-topic share.jobs 1
# share groups start at "latest" by default and would miss jobs published before the first member joined
../bin/kafka kafka-configs --entity-type groups --entity-name share.workers --alter \
  --add-config share.auto.offset.reset=earliest >/dev/null && echo "group share.workers: share.auto.offset.reset=earliest"
