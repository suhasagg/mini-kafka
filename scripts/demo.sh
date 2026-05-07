#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:9092}"

curl -s "$BASE_URL/health" | jq

curl -s -X POST "$BASE_URL/topics?name=orders&partitions=3&replicationFactor=2" | jq

curl -s -X POST "$BASE_URL/produce?topic=orders&key=user-1&acks=all" --data 'order-created:1001' | jq
curl -s -X POST "$BASE_URL/produce?topic=orders&key=user-2&acks=all" --data 'order-created:1002' | jq
curl -s -X POST "$BASE_URL/produce?topic=orders&acks=1" --data 'order-created:1003' | jq

curl -s "$BASE_URL/cluster/metadata" | jq
curl -s "$BASE_URL/consumer/poll?group=payments-service&topic=orders&max=10&autoCommit=true" | jq
curl -s "$BASE_URL/consumer/offsets?group=payments-service" | jq

curl -s -X POST "$BASE_URL/brokers/broker-1/down" | jq
curl -s "$BASE_URL/cluster/metadata" | jq
curl -s -X POST "$BASE_URL/brokers/broker-1/up" | jq
curl -s "$BASE_URL/cluster/metadata" | jq
