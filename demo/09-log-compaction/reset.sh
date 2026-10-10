#!/bin/sh
# Deletes and recreates everything this demo uses
cd "$(dirname "$0")"
../bin/rabbit rabbitmqctl delete_queue compact.users >/dev/null 2>&1
# compaction settings tuned so it shows up within a minute; production keeps the defaults
# (7-day segments, 50% dirty ratio, 1-day tombstone retention)
../bin/reset-topic compact.users 1 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config min.compaction.lag.ms=0 \
  --config delete.retention.ms=10000
