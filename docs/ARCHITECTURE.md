# Mini Kafka Java - Detailed Architecture

## 1. Goal

Mini Kafka Java is a compact Java implementation of the most important ideas behind Apache Kafka:

- append-only commit logs
- topic partitions
- broker nodes
- partition leaders
- follower replication
- in-sync replicas
- producer acknowledgements
- consumer offsets
- consumer groups

It is intentionally implemented as a single-process multi-broker simulation. This keeps the source code small enough to study while still showing the important distributed-systems tradeoffs.

---

## 2. Component responsibilities

| Component | Responsibility |
|---|---|
| `MiniKafkaServer` | HTTP API layer. Converts REST requests into cluster operations. |
| `MiniKafkaCluster` | Main coordinator. Owns metadata, brokers, topic routing, produce, fetch, replication, leader election, and recovery. |
| `BrokerNode` | Represents one broker. Owns partition replica logs and alive/down state. |
| `TopicMetadata` | Stores topic-level metadata: partition count, replication factor, and partition metadata. |
| `PartitionMetadata` | Stores partition-level metadata: leader, replicas, and ISR. |
| `AppendOnlyLog` | Persistent partition log made of segment files. Appends and reads records by offset. |
| `LogSegment` | One immutable-ish append file with a base offset. |
| `ConsumerGroupCoordinator` | Stores committed offsets per consumer group and topic partition. |
| `MessageRecord` | One Kafka-like record: topic, partition, offset, key, value, timestamp. |
| `ProduceResult` | Response metadata from a successful produce operation. |

---

## 3. Broker model

A broker is represented by `BrokerNode`.

Each broker has:

- a broker id, such as `broker-0`
- an alive/down flag
- a set of local partition replica logs

```text
BrokerNode
  id = broker-0
  alive = true
  replicas:
    orders-0 -> AppendOnlyLog
    payments-2 -> AppendOnlyLog
```

In real Kafka, brokers are separate processes communicating over the network. Here, all brokers are Java objects inside one process.

---

## 4. Topic and partition model

A topic has N partitions. Each partition has:

- exactly one leader
- one or more replicas
- a subset of replicas that are currently in-sync

Example for topic `orders` with 3 partitions and replication factor 2:

```text
orders-0: leader=broker-0, replicas=[broker-0, broker-1], ISR=[broker-0, broker-1]
orders-1: leader=broker-1, replicas=[broker-1, broker-2], ISR=[broker-1, broker-2]
orders-2: leader=broker-2, replicas=[broker-2, broker-0], ISR=[broker-2, broker-0]
```

The first replica is initially selected as leader. Replicas are assigned round-robin across brokers.

---

## 5. Partition routing

Producer routing uses two strategies:

### Keyed messages

If the producer supplies a key:

```text
partition = positiveHash(key) % partitionCount
```

This keeps messages with the same key on the same partition, preserving per-key ordering.

### Unkeyed messages

If no key is supplied:

```text
partition = nextRoundRobinPartition()
```

This spreads load across partitions.

---

## 6. Write path

```text
POST /produce?topic=orders&key=user-1&acks=all
    ↓
MiniKafkaServer
    ↓
MiniKafkaCluster.produce()
    ↓
Choose partition
    ↓
Find leader
    ↓
Check ISR if acks=all
    ↓
Append to leader log
    ↓
Append same record to follower logs
    ↓
Return topic, partition, offset, leader, ack count
```

The leader assigns the offset. Followers receive the exact same record and offset.

---

## 7. Ack modes

| Mode | Meaning in this project |
|---|---|
| `acks=0` | Fire-and-forget after routing. The server still appends locally because this is HTTP-based, but it returns without requiring follower acknowledgement. |
| `acks=1` | Success after the leader append succeeds. |
| `acks=all` | Success only if enough in-sync replicas acknowledge based on `minInSyncReplicas`. |

For `acks=all`, the system checks live ISR count before appending. If too few in-sync replicas are alive, it rejects the write.

---

## 8. Replication

Replication is synchronous inside `MiniKafkaCluster.produce()`.

```text
Leader append offset 10
    ↓
Follower broker-1 force-appends offset 10
    ↓
Follower broker-2 force-appends offset 10
```

If a follower is down, it is removed from ISR. When it comes back, the recovery path catches it up from the current leader.

---

## 9. ISR behavior

ISR means in-sync replica set.

In this project:

- A live replica in sync is part of ISR.
- A down replica is removed from ISR.
- A recovered replica catches up from leader and rejoins ISR.
- `acks=all` uses ISR and `minInSyncReplicas` to decide whether writes are safe.

This mirrors the key high-level idea of Kafka's durability model.

---

## 10. Leader election

If a partition leader is down, `MiniKafkaCluster` elects a new leader.

Election preference:

1. alive replica already in ISR
2. alive replica from replica list

```text
old leader broker-1 down
replicas = [broker-1, broker-2]
ISR = [broker-2]
new leader = broker-2
```

Real Kafka has stricter controller-driven leader election. This project keeps the idea simple and visible.

---

## 11. Broker recovery

When a broker is marked up:

```text
POST /brokers/broker-1/up
    ↓
Broker becomes alive
    ↓
For each replica on that broker:
        read missing records from partition leader
        append missing records to recovered broker
        add broker back to ISR
```

This is a simplified follower catch-up flow.

---

## 12. Append-only log

Each topic partition replica is stored as an append-only log.

```text
data/broker-0/orders-0/
  segment-00000000000000000000.log
  segment-00000000000000000050.log
```

Each log has a monotonically increasing next offset.

The main operations are:

- `append(topic, partition, key, value)`
- `forceAppend(record)` for replica catch-up and follower replication
- `readFrom(offset, maxRecords)`
- `endOffset()`

---

## 13. Log segments

A log is split into segment files. Each segment starts at a base offset.

Example:

```text
segment-00000000000000000000.log  offsets 0..49
segment-00000000000000000050.log  offsets 50..99
```

The segment size is controlled by `SEGMENT_MAX_RECORDS`.

This mimics Kafka's segmented log design, where old segments can later be deleted, compacted, or transferred.

---

## 14. Record format

Records are stored line-by-line:

```text
offset<TAB>timestamp<TAB>base64(key)<TAB>base64(value)
```

Base64 is used so values can contain spaces, quotes, commas, or newlines without breaking the file format.

---

## 15. Consumer groups

The consumer group coordinator stores offsets:

```text
group payments-service
  orders-0 -> offset 15
  orders-1 -> offset 20
  orders-2 -> offset 9
```

`/consumer/poll` reads from each partition starting at the committed offset.

If `autoCommit=true`, the coordinator commits the next offset after returned messages.

---

## 16. Offset semantics

A committed offset means the next offset to read.

If a consumer reads offsets 0, 1, 2, it should commit offset 3.

This matches Kafka's offset convention.

---

## 17. Failure scenarios

### Follower down

- Leader can still accept writes if live ISR count satisfies min ISR.
- Down follower leaves ISR.
- Recovered follower catches up and rejoins ISR.

### Leader down

- Cluster elects a new leader from alive replicas.
- New writes go to new leader.
- Old leader catches up when it comes back.

### Too few ISR replicas

With `acks=all`, writes are rejected to avoid acknowledging unsafe writes.

---

## 18. Production gaps

Real Kafka includes many additional systems:

- KRaft metadata quorum
- controller election
- network protocol and request pipelining
- batch compression
- zero-copy transfer using `sendfile`
- page cache optimization
- producer idempotence
- exactly-once transactions
- transactional offset commits
- consumer group join/sync/rebalance protocol
- rack-aware placement
- tiered storage
- ACLs and TLS
- metrics and quotas

This project focuses on the core mechanics that are easiest to understand in source code.
