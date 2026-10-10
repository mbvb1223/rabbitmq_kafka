#!/bin/sh
# Deletes this demo's queues (and their bindings) and the retry.dlq exchange, and recreates its topics; the PHP scripts redeclare the rest
cd "$(dirname "$0")"
for q in retry.jobs retry.dead retry.dlq.validation retry.dlq.timeout; do
  ../bin/rabbit rabbitmqctl delete_queue "$q" >/dev/null 2>&1
done
# an exchange declared with other arguments fails the redeclare (PRECONDITION_FAILED); rabbitmqctl can't delete exchanges
../bin/rabbit rabbitmqadmin -u app -p app delete exchange --name retry.dlq >/dev/null 2>&1
../bin/reset-topic retry.jobs 1
../bin/reset-topic retry.jobs.retry 1
../bin/reset-topic retry.jobs.dlq 1
