#!/bin/sh
# A share-group member. There is no PHP share consumer, so this is Kafka's Java console tool.
# record_limit + max.poll.records=1 acquires one record at a time, like RabbitMQ prefetch 1.
# Extra args pass through, e.g. --release, --reject, --timeout-ms 20000
exec "$(dirname "$0")/../../bin/kafka" kafka-console-share-consumer --topic share.jobs --group share.workers \
  --command-property share.acquire.mode=record_limit --command-property max.poll.records=1 \
  --formatter-property print.offset=true --formatter-property print.delivery=true "$@"
