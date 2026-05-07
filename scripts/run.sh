#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f build/mini-kafka.jar ]; then
  ./scripts/build.sh
fi

PORT="${PORT:-9092}"
DATA_DIR="${DATA_DIR:-data}"
BROKERS="${BROKERS:-3}"
SEGMENT_MAX_RECORDS="${SEGMENT_MAX_RECORDS:-50}"
MIN_IN_SYNC_REPLICAS="${MIN_IN_SYNC_REPLICAS:-2}"

PORT="$PORT" \
DATA_DIR="$DATA_DIR" \
BROKERS="$BROKERS" \
SEGMENT_MAX_RECORDS="$SEGMENT_MAX_RECORDS" \
MIN_IN_SYNC_REPLICAS="$MIN_IN_SYNC_REPLICAS" \
java -jar build/mini-kafka.jar
