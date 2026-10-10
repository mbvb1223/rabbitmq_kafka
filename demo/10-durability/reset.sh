#!/bin/sh
# ./reset.sh kafka|rabbitmq: restarts any killed node, recreates this demo's topics or queue
cd "$(dirname "$0")"
case "$1" in
kafka)
  docker compose --profile kafka start >/dev/null 2>&1
  rm -f kafka/acked-*.log
  for t in durable.acks1:1 durable.acksall:2; do
    topic=${t%:*}
    ./kafka-cli kafka-topics --delete --if-exists --topic "$topic"
    # deletion is async: create fails with "marked for deletion" for a few seconds
    until ./kafka-cli kafka-topics --create --topic "$topic" --partitions 1 --replication-factor 3 \
      --config min.insync.replicas="${t#*:}" >/dev/null 2>&1; do sleep 1; done
    ./kafka-cli kafka-topics --describe --topic "$topic" | tail -1
  done
  ;;
rabbitmq)
  docker compose --profile rabbitmq start >/dev/null 2>&1
  rm -f rabbitmq/confirmed-*.log
  ./rabbit-cli rabbitmqctl delete_queue durable.classic >/dev/null 2>&1
  RABBITMQ_PORT=5681 php rabbitmq/bootstrap.php
  ./rabbit-cli rabbitmq-queues quorum_status durable.orders --formatter=json 2>/dev/null \
    | php -r 'foreach (json_decode(stream_get_contents(STDIN), true) as $r) echo "  {$r["Node Name"]}: {$r["Raft State"]}\n";'
  ;;
*) echo "usage: ./reset.sh kafka|rabbitmq"; exit 1 ;;
esac
