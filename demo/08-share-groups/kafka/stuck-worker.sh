#!/bin/sh
# ./stuck-worker.sh        start a share-group member in its own container, then freeze it
# ./stuck-worker.sh stop   remove it
# docker pause = a PHP worker stuck in a long job: still a member, never acknowledges
set -e
if [ "$1" = stop ]; then docker rm -f share.stuck >/dev/null 2>&1; echo "share.stuck removed"; exit; fi
docker rm -f share.stuck >/dev/null 2>&1 || true
# a 60 s long-poll keeps a fetch waiting at the broker while frozen, so the next published record is acquired for it
docker run -d --rm --name share.stuck --network "${KAFKA_NETWORK:-rabbitmq-kafka-demo_default}" apache/kafka:4.3.1 \
  /opt/kafka/bin/kafka-console-share-consumer.sh --bootstrap-server kafka:19092 --topic share.jobs --group share.workers \
  --command-property share.acquire.mode=record_limit --command-property max.poll.records=1 \
  --command-property fetch.max.wait.ms=60000 >/dev/null
echo "share.stuck joining share.workers..."
sleep 10
docker pause share.stuck >/dev/null
echo "share.stuck is frozen. Publish now: it will hold one record until the 30 s lock expires"
