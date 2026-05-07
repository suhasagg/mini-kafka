package com.example.minikafka.cluster;

import com.example.minikafka.log.AppendOnlyLog;
import com.example.minikafka.model.MessageRecord;
import com.example.minikafka.model.TopicPartition;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class BrokerNode {
    private final String id;
    private final Path brokerDir;
    private final int segmentMaxRecords;
    private volatile boolean alive = true;
    private final Map<TopicPartition, AppendOnlyLog> logs = new ConcurrentHashMap<>();

    public BrokerNode(String id, Path dataDir, int segmentMaxRecords) {
        this.id = id;
        this.brokerDir = dataDir.resolve(id);
        this.segmentMaxRecords = segmentMaxRecords;
    }

    public String id() {
        return id;
    }

    public boolean alive() {
        return alive;
    }

    public void markDown() {
        alive = false;
    }

    public void markUp() {
        alive = true;
    }

    public AppendOnlyLog createReplica(TopicPartition tp) {
        try {
            return logs.computeIfAbsent(tp, ignored -> {
                try {
                    return new AppendOnlyLog(tp.topic(), tp.partition(), brokerDir.resolve(tp.id()), segmentMaxRecords);
                } catch (IOException e) {
                    throw new ReplicationException("failed to create replica " + tp + " on " + id, e);
                }
            });
        } catch (ReplicationException e) {
            throw e;
        }
    }

    public boolean hasReplica(TopicPartition tp) {
        return logs.containsKey(tp);
    }

    public MessageRecord append(TopicPartition tp, String key, String value) {
        ensureAlive();
        try {
            return createReplica(tp).append(key, value);
        } catch (IOException e) {
            throw new ReplicationException("append failed on " + id + " for " + tp, e);
        }
    }

    public void forceAppend(TopicPartition tp, MessageRecord record) {
        ensureAlive();
        try {
            createReplica(tp).forceAppend(record);
        } catch (IOException e) {
            throw new ReplicationException("force append failed on " + id + " for " + tp, e);
        }
    }

    public List<MessageRecord> readFrom(TopicPartition tp, long offset, int maxRecords) {
        ensureAlive();
        try {
            return createReplica(tp).readFrom(offset, maxRecords);
        } catch (IOException e) {
            throw new ReplicationException("read failed on " + id + " for " + tp, e);
        }
    }

    public long endOffset(TopicPartition tp) {
        AppendOnlyLog log = createReplica(tp);
        return log.endOffset();
    }

    public List<Map<String, Object>> segmentInfo(TopicPartition tp) {
        return createReplica(tp).segmentInfo();
    }

    public List<TopicPartition> replicas() {
        List<TopicPartition> result = new ArrayList<>(logs.keySet());
        result.sort(TopicPartition::compareTo);
        return result;
    }

    public Map<String, Object> toJsonMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", id);
        map.put("alive", alive);
        List<String> replicaIds = new ArrayList<>();
        for (TopicPartition tp : replicas()) {
            replicaIds.add(tp.id());
        }
        map.put("replicas", replicaIds);
        return map;
    }

    private void ensureAlive() {
        if (!alive) {
            throw new ReplicationException("broker is down: " + id);
        }
    }
}
