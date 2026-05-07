package com.example.minikafka.cluster;

import com.example.minikafka.config.BrokerConfig;
import com.example.minikafka.consumer.ConsumerGroupCoordinator;
import com.example.minikafka.model.MessageRecord;
import com.example.minikafka.model.ProduceResult;
import com.example.minikafka.model.TopicPartition;
import com.example.minikafka.util.HashUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class MiniKafkaCluster {
    private final BrokerConfig config;
    private final Map<String, BrokerNode> brokers = new LinkedHashMap<>();
    private final Map<String, TopicMetadata> topics = new ConcurrentHashMap<>();
    private final ConsumerGroupCoordinator consumerGroups = new ConsumerGroupCoordinator();

    public MiniKafkaCluster(BrokerConfig config) {
        this.config = config;
        for (int i = 0; i < config.brokerCount(); i++) {
            String id = "broker-" + i;
            brokers.put(id, new BrokerNode(id, config.dataDir(), config.segmentMaxRecords()));
        }
    }

    public synchronized void createTopic(String name, int partitions, int replicationFactor) {
        validateTopicName(name);
        if (topics.containsKey(name)) {
            throw new IllegalArgumentException("topic already exists: " + name);
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be positive");
        }
        if (replicationFactor <= 0 || replicationFactor > brokers.size()) {
            throw new IllegalArgumentException("replicationFactor must be between 1 and broker count");
        }
        TopicMetadata topic = new TopicMetadata(name, partitions, replicationFactor);
        List<String> brokerIds = new ArrayList<>(brokers.keySet());
        for (int p = 0; p < partitions; p++) {
            List<String> replicas = new ArrayList<>();
            for (int r = 0; r < replicationFactor; r++) {
                replicas.add(brokerIds.get((p + r) % brokerIds.size()));
            }
            PartitionMetadata pm = new PartitionMetadata(name, p, replicas.get(0), replicas);
            topic.putPartition(pm);
            TopicPartition tp = new TopicPartition(name, p);
            for (String brokerId : replicas) {
                brokers.get(brokerId).createReplica(tp);
            }
        }
        topics.put(name, topic);
    }

    public synchronized ProduceResult produce(String topicName, String key, String value, String acks) {
        TopicMetadata topic = requireTopic(topicName);
        int partition = key == null || key.isEmpty()
                ? topic.nextRoundRobinPartition()
                : HashUtil.partitionForKey(key, topic.partitionCount());
        return produceToPartition(topicName, partition, key, value, acks);
    }

    public synchronized ProduceResult produceToPartition(String topicName, int partition, String key, String value, String acks) {
        TopicMetadata topic = requireTopic(topicName);
        PartitionMetadata pm = topic.partition(partition);
        TopicPartition tp = new TopicPartition(topicName, partition);
        electLeaderIfNeeded(pm);
        BrokerNode leader = requireBroker(pm.leaderId());
        if (!leader.alive()) {
            throw new ReplicationException("no alive leader for " + tp);
        }

        String normalizedAcks = normalizeAcks(acks);
        if ("all".equals(normalizedAcks)) {
            int liveIsr = liveIsrCount(pm);
            if (liveIsr < config.minInSyncReplicas()) {
                throw new ReplicationException("not enough live ISR for " + tp + ": liveIsr=" + liveIsr
                        + ", minInSyncReplicas=" + config.minInSyncReplicas());
            }
        }

        MessageRecord leaderRecord = leader.append(tp, key, value);
        int acknowledgements = 1;
        Set<String> liveReplicas = new LinkedHashSet<>();
        liveReplicas.add(leader.id());

        for (String replicaId : pm.replicas()) {
            if (replicaId.equals(leader.id())) {
                continue;
            }
            BrokerNode replica = requireBroker(replicaId);
            if (!replica.alive()) {
                pm.removeFromIsr(replicaId);
                continue;
            }
            replica.forceAppend(tp, leaderRecord);
            pm.addToIsr(replicaId);
            liveReplicas.add(replicaId);
            acknowledgements++;
        }

        pm.addToIsr(leader.id());
        for (String replicaId : pm.replicas()) {
            if (!liveReplicas.contains(replicaId)) {
                pm.removeFromIsr(replicaId);
            }
        }

        if ("all".equals(normalizedAcks) && acknowledgements < config.minInSyncReplicas()) {
            throw new ReplicationException("write did not reach min ISR for " + tp);
        }
        return new ProduceResult(topicName, partition, leaderRecord.offset(), leader.id(), acknowledgements);
    }

    public synchronized List<MessageRecord> fetch(String topicName, int partition, long offset, int maxRecords) {
        TopicMetadata topic = requireTopic(topicName);
        PartitionMetadata pm = topic.partition(partition);
        TopicPartition tp = new TopicPartition(topicName, partition);
        electLeaderIfNeeded(pm);
        BrokerNode leader = requireBroker(pm.leaderId());
        return leader.readFrom(tp, offset, Math.max(maxRecords, 0));
    }

    public synchronized List<MessageRecord> poll(String groupId, String topicName, int maxRecords, boolean autoCommit) {
        TopicMetadata topic = requireTopic(topicName);
        List<MessageRecord> result = new ArrayList<>();
        for (PartitionMetadata pm : topic.partitions()) {
            if (result.size() >= maxRecords) {
                break;
            }
            TopicPartition tp = new TopicPartition(topicName, pm.partition());
            long offset = consumerGroups.committedOffset(groupId, tp);
            List<MessageRecord> records = fetch(topicName, pm.partition(), offset, maxRecords - result.size());
            result.addAll(records);
            if (autoCommit && !records.isEmpty()) {
                long nextOffset = records.get(records.size() - 1).offset() + 1;
                consumerGroups.commit(groupId, tp, nextOffset);
            }
        }
        return result;
    }

    public synchronized void commit(String groupId, String topicName, int partition, long nextOffset) {
        requireTopic(topicName).partition(partition);
        consumerGroups.commit(groupId, new TopicPartition(topicName, partition), nextOffset);
    }

    public synchronized Map<String, Object> offsets(String groupId) {
        return consumerGroups.offsetsForGroup(groupId);
    }

    public synchronized void brokerDown(String brokerId) {
        BrokerNode broker = requireBroker(brokerId);
        broker.markDown();
        for (TopicMetadata topic : topics.values()) {
            for (PartitionMetadata pm : topic.partitions()) {
                if (pm.replicas().contains(brokerId)) {
                    pm.removeFromIsr(brokerId);
                }
                electLeaderIfNeeded(pm);
            }
        }
    }

    public synchronized void brokerUp(String brokerId) {
        BrokerNode broker = requireBroker(brokerId);
        broker.markUp();
        for (TopicMetadata topic : topics.values()) {
            for (PartitionMetadata pm : topic.partitions()) {
                if (pm.replicas().contains(brokerId)) {
                    catchUpReplica(pm, brokerId);
                    pm.addToIsr(brokerId);
                }
                electLeaderIfNeeded(pm);
            }
        }
    }

    public synchronized Map<String, Object> metadata() {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> brokerMaps = new ArrayList<>();
        for (BrokerNode broker : brokers.values()) {
            brokerMaps.add(broker.toJsonMap());
        }
        result.put("brokers", brokerMaps);
        List<Map<String, Object>> topicMaps = new ArrayList<>();
        topics.values().stream()
                .sorted((a, b) -> a.name().compareTo(b.name()))
                .forEach(topic -> topicMaps.add(topic.toJsonMap()));
        result.put("topics", topicMaps);
        result.put("minInSyncReplicas", config.minInSyncReplicas());
        return result;
    }

    public synchronized List<Map<String, Object>> segmentInfo(String topicName, int partition) {
        TopicMetadata topic = requireTopic(topicName);
        PartitionMetadata pm = topic.partition(partition);
        TopicPartition tp = new TopicPartition(topicName, partition);
        BrokerNode leader = requireBroker(pm.leaderId());
        return leader.segmentInfo(tp);
    }

    public synchronized List<Map<String, Object>> listTopics() {
        List<Map<String, Object>> list = new ArrayList<>();
        topics.values().stream()
                .sorted((a, b) -> a.name().compareTo(b.name()))
                .forEach(topic -> list.add(topic.toJsonMap()));
        return list;
    }

    public int brokerCount() {
        return brokers.size();
    }

    public long aliveBrokerCount() {
        return brokers.values().stream().filter(BrokerNode::alive).count();
    }

    private void catchUpReplica(PartitionMetadata pm, String brokerId) {
        TopicPartition tp = new TopicPartition(pm.topic(), pm.partition());
        BrokerNode target = requireBroker(brokerId);
        BrokerNode leader = requireBroker(pm.leaderId());
        if (!leader.alive()) {
            electLeaderIfNeeded(pm);
            leader = requireBroker(pm.leaderId());
        }
        if (leader.id().equals(target.id())) {
            return;
        }
        long targetEnd = target.endOffset(tp);
        List<MessageRecord> missing = leader.readFrom(tp, targetEnd, Integer.MAX_VALUE);
        for (MessageRecord record : missing) {
            target.forceAppend(tp, record);
        }
    }

    private void electLeaderIfNeeded(PartitionMetadata pm) {
        BrokerNode current = requireBroker(pm.leaderId());
        if (current.alive()) {
            return;
        }
        for (String replicaId : pm.inSyncReplicas()) {
            BrokerNode candidate = requireBroker(replicaId);
            if (candidate.alive()) {
                pm.leaderId(candidate.id());
                return;
            }
        }
        for (String replicaId : pm.replicas()) {
            BrokerNode candidate = requireBroker(replicaId);
            if (candidate.alive()) {
                pm.leaderId(candidate.id());
                pm.addToIsr(candidate.id());
                return;
            }
        }
        throw new ReplicationException("no alive replica for " + pm.topic() + "-" + pm.partition());
    }

    private int liveIsrCount(PartitionMetadata pm) {
        int count = 0;
        for (String replicaId : pm.inSyncReplicas()) {
            BrokerNode broker = requireBroker(replicaId);
            if (broker.alive()) {
                count++;
            }
        }
        return count;
    }

    private TopicMetadata requireTopic(String name) {
        TopicMetadata topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("unknown topic: " + name);
        }
        return topic;
    }

    private BrokerNode requireBroker(String brokerId) {
        BrokerNode broker = brokers.get(brokerId);
        if (broker == null) {
            throw new IllegalArgumentException("unknown broker: " + brokerId);
        }
        return broker;
    }

    private String normalizeAcks(String acks) {
        if (acks == null || acks.isBlank()) {
            return "1";
        }
        String normalized = acks.trim().toLowerCase();
        if (normalized.equals("0") || normalized.equals("1") || normalized.equals("all")) {
            return normalized;
        }
        throw new IllegalArgumentException("acks must be one of 0, 1, all");
    }

    private void validateTopicName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("topic name is required");
        }
        if (!name.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("topic name may contain only letters, numbers, dot, underscore, and dash");
        }
    }
}
