# Mini Kafka Java

A dependency-free implementation of a Kafka-like distributed message queue in Java.

It uses Java's built-in HTTP server instead of Kafka's binary protocol, and it simulates a multi-broker Kafka cluster inside a single JVM so you can run and inspect it locally without ZooKeeper, KRaft, Maven, Gradle, Docker, or external libraries.

## Contents

- Topic creation
- Partitions
- Append-only partition logs
- Immutable segment files
- Producer API
- Consumer fetch API
- Consumer groups
- Offset commit and offset lookup
- Hash-based key partitioning
- Round-robin partitioning for messages without keys
- Multi-broker cluster simulation
- Partition leaders and followers
- Replication factor
- In-sync replicas, ISR
- `acks=0`, `acks=1`, and `acks=all`
- Minimum in-sync replica enforcement
- Broker down/up simulation
- Leader election after broker failure
- Follower catch-up after broker recovery
- Cluster metadata API
- Segment inspection API
- Plain `javac` build scripts
- Custom Java test runner

---

## 1. Architecture

```text
                        ┌──────────────────────────────┐
                        │          HTTP Client          │
                        │ produce / consume / commit    │
                        └───────────────┬──────────────┘
                                        │
                                        ▼
                        ┌──────────────────────────────┐
                        │       MiniKafkaServer         │
                        │ Java built-in HttpServer      │
                        └───────────────┬──────────────┘
                                        │
                                        ▼
                        ┌──────────────────────────────┐
                        │       MiniKafkaCluster        │
                        │ - topic metadata              │
                        │ - partition routing           │
                        │ - leader election             │
                        │ - replication coordination    │
                        │ - ISR tracking                │
                        └───────────────┬──────────────┘
                                        │
          ┌─────────────────────────────┼─────────────────────────────┐
          ▼                             ▼                             ▼
┌───────────────────┐         ┌───────────────────┐         ┌───────────────────┐
│ broker-0          │         │ broker-1          │         │ broker-2          │
│ alive/down state  │         │ alive/down state  │         │ alive/down state  │
│ partition logs    │         │ partition logs    │         │ partition logs    │
└─────────┬─────────┘         └─────────┬─────────┘         └─────────┬─────────┘
          │                             │                             │
          ▼                             ▼                             ▼
┌───────────────────┐         ┌───────────────────┐         ┌───────────────────┐
│ topic-A-0 log     │         │ topic-A-0 replica │         │ topic-A-1 leader  │
│ segment-000.log   │         │ segment-000.log   │         │ segment-000.log   │
│ segment-050.log   │         │ segment-050.log   │         │ segment-050.log   │
└───────────────────┘         └───────────────────┘         └───────────────────┘
```

---

## 2. Kafka-like concepts implemented

| Kafka concept | Implemented here |
|---|---|
| Broker | `BrokerNode` |
| Topic | `TopicMetadata` |
| Partition | `TopicPartition` |
| Partition leader | `PartitionMetadata.leaderId` |
| Follower replica | `PartitionMetadata.replicas` excluding leader |
| Replication factor | `TopicMetadata.replicationFactor` |
| ISR | `PartitionMetadata.inSyncReplicas` |
| Producer | `MiniKafkaCluster.produce()` and `/produce` |
| Consumer | `/consume` and `/consumer/poll` |
| Consumer group | `ConsumerGroupCoordinator` |
| Offset commit | `ConsumerGroupCoordinator.commit()` |
| Log segment | `LogSegment` |
| Append-only log | `AppendOnlyLog` |
| Key partitioning | `HashUtil.partitionForKey()` |
| Round-robin partitioning | `TopicMetadata.nextRoundRobinPartition()` |
| acks | query parameter `acks=0`, `acks=1`, `acks=all` |
| min.insync.replicas | `BrokerConfig.minInSyncReplicas` |
| Leader election | `MiniKafkaCluster.electLeaderIfNeeded()` |
| Follower catch-up | `MiniKafkaCluster.catchUpReplica()` |

---

## 3. Requirements

- Java 17+
- Bash shell

No Maven or Gradle is required.

Check Java:

```bash
java -version
javac -version
```

---

## 4. Build

```bash
./scripts/build.sh
```

This creates:

```text
build/mini-kafka.jar
```

---

## 5. Run tests

```bash
./scripts/test.sh
```

Expected output:

```text
[PASS] AppendOnlyLogTest
[PASS] MiniKafkaClusterTest
[PASS] ConsumerGroupCoordinatorTest
[PASS] FailoverTest
All tests passed.
```

---

## 6. Run server

```bash
./scripts/run.sh
```

Default server:

```text
http://localhost:9092
```

Default cluster:

```text
brokers = 3
segmentMaxRecords = 50
minInSyncReplicas = 2
```

You can override settings:

```bash
PORT=19092 \
DATA_DIR=/tmp/mini-kafka-data \
BROKERS=5 \
SEGMENT_MAX_RECORDS=100 \
MIN_IN_SYNC_REPLICAS=2 \
./scripts/run.sh
```

---

## 7. API examples

### 7.1 Health

```bash
curl -s http://localhost:9092/health | jq
```

Example response:

```json
{
  "status": "UP",
  "service": "mini-kafka",
  "brokers": 3,
  "aliveBrokers": 3
}
```

---

### 7.2 Create a topic

```bash
curl -s -X POST \
  'http://localhost:9092/topics?name=orders&partitions=3&replicationFactor=2' | jq
```

Example response:

```json
{
  "status": "OK",
  "topic": "orders",
  "partitions": 3,
  "replicationFactor": 2
}
```

---

### 7.3 List topics

```bash
curl -s http://localhost:9092/topics | jq
```

---

### 7.4 Produce a message with key partitioning

```bash
curl -s -X POST \
  'http://localhost:9092/produce?topic=orders&key=user-1&acks=all' \
  --data 'order-created:1001' | jq
```

Example response:

```json
{
  "status": "OK",
  "topic": "orders",
  "partition": 1,
  "offset": 0,
  "leader": "broker-1",
  "acks": 2
}
```

---

### 7.5 Produce without a key using round-robin routing

```bash
curl -s -X POST \
  'http://localhost:9092/produce?topic=orders&acks=1' \
  --data 'order-created:1002' | jq
```

---

### 7.6 Fetch directly from a partition offset

```bash
curl -s \
  'http://localhost:9092/consume?topic=orders&partition=0&offset=0&max=10' | jq
```

Example response:

```json
{
  "topic": "orders",
  "partition": 0,
  "offset": 0,
  "records": [
    {
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "key": "user-1",
      "value": "order-created:1001",
      "timestamp": 1730000000000
    }
  ]
}
```

---

### 7.7 Poll as a consumer group

```bash
curl -s \
  'http://localhost:9092/consumer/poll?group=payments-service&topic=orders&max=10&autoCommit=true' | jq
```

With `autoCommit=true`, the group offset advances to the next offset after the returned records.

---

### 7.8 Commit a consumer offset manually

```bash
curl -s -X POST \
  'http://localhost:9092/consumer/commit?group=payments-service&topic=orders&partition=0&offset=5' | jq
```

---

### 7.9 View consumer group offsets

```bash
curl -s \
  'http://localhost:9092/consumer/offsets?group=payments-service' | jq
```

---

### 7.10 View cluster metadata

```bash
curl -s http://localhost:9092/cluster/metadata | jq
```

This shows topics, partitions, leaders, replicas, ISR, and broker liveness.

---

### 7.11 View partition segments

```bash
curl -s \
  'http://localhost:9092/segments?topic=orders&partition=0' | jq
```

---

### 7.12 Simulate broker failure

```bash
curl -s -X POST http://localhost:9092/brokers/broker-1/down | jq
```

Produce while the broker is down:

```bash
curl -s -X POST \
  'http://localhost:9092/produce?topic=orders&key=user-9&acks=all' \
  --data 'order-created:1009' | jq
```

Bring the broker back. Its replicas will catch up from leaders:

```bash
curl -s -X POST http://localhost:9092/brokers/broker-1/up | jq
```

---

## 8. Demo script

```bash
./scripts/demo.sh
```

---

## 9. Source code map

```text
src/main/java/com/example/minikafka
├── Main.java
├── cluster
│   ├── BrokerNode.java
│   ├── MiniKafkaCluster.java
│   ├── PartitionMetadata.java
│   ├── ReplicationException.java
│   └── TopicMetadata.java
├── config
│   └── BrokerConfig.java
├── consumer
│   └── ConsumerGroupCoordinator.java
├── log
│   ├── AppendOnlyLog.java
│   └── LogSegment.java
├── model
│   ├── MessageRecord.java
│   ├── ProduceResult.java
│   └── TopicPartition.java
├── server
│   └── MiniKafkaServer.java
└── util
    ├── Encoding.java
    ├── HashUtil.java
    ├── JsonUtil.java
    ├── QueryString.java
    └── Time.java
```

---

## 10. Produce path

```text
Client POST /produce
    ↓
MiniKafkaServer parses topic, key, acks
    ↓
MiniKafkaCluster.produce()
    ↓
Choose partition:
    - key present    -> hash(key) % partitionCount
    - key absent     -> round-robin partition
    ↓
Find partition leader
    ↓
If leader is down, elect a new leader from alive ISR
    ↓
If acks=all, ensure live ISR count >= minInSyncReplicas
    ↓
Append to leader's partition log
    ↓
Replicate same offset to follower logs
    ↓
Return success depending on acks mode
```

---

## 11. Consume path

```text
Direct consume:
Client GET /consume?topic=T&partition=P&offset=O
    ↓
Read from partition leader log
    ↓
Return records from offset O

Consumer group poll:
Client GET /consumer/poll?group=G&topic=T
    ↓
Load committed offset for each topic partition
    ↓
Fetch records from each partition
    ↓
If autoCommit=true, commit next offset
    ↓
Return records
```

---

## 12. Storage layout

```text
data/
└── broker-0/
    └── orders-0/
        ├── segment-00000000000000000000.log
        └── segment-00000000000000000050.log
```

Each segment line is a tab-separated record:

```text
offset timestamp base64(key) base64(value)
```

Example:

```text
0    1730000000000    dXNlci0x    b3JkZXItY3JlYXRlZDoxMDAx
```

---

## 13. Important limitations

This is not production Kafka. It intentionally avoids many real Kafka features:

- No Kafka binary protocol
- No KRaft/Raft metadata quorum
- No controller quorum
- No real networking between brokers
- No page-cache optimization
- No zero-copy transfer
- No batch compression
- No exactly-once transactions
- No idempotent producers
- No producer sequence numbers
- No log compaction by key
- No retention cleanup scheduler
- No ACL/authentication/authorization
- No TLS
- No rack-aware replica placement
- No consumer group rebalancing protocol
- No partitions assigned per consumer member
- No schema registry

The goal is to make Kafka's core architecture understandable from source code.

---

## 14. Clean generated data

```bash
rm -rf data build
```
