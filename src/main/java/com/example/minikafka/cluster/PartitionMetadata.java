package com.example.minikafka.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PartitionMetadata {
    private final String topic;
    private final int partition;
    private String leaderId;
    private final List<String> replicas;
    private final Set<String> inSyncReplicas;

    public PartitionMetadata(String topic, int partition, String leaderId, List<String> replicas) {
        this.topic = topic;
        this.partition = partition;
        this.leaderId = leaderId;
        this.replicas = new ArrayList<>(replicas);
        this.inSyncReplicas = new LinkedHashSet<>(replicas);
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public String leaderId() {
        return leaderId;
    }

    public void leaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public List<String> replicas() {
        return new ArrayList<>(replicas);
    }

    public Set<String> inSyncReplicas() {
        return new LinkedHashSet<>(inSyncReplicas);
    }

    public void removeFromIsr(String brokerId) {
        inSyncReplicas.remove(brokerId);
    }

    public void addToIsr(String brokerId) {
        if (replicas.contains(brokerId)) {
            inSyncReplicas.add(brokerId);
        }
    }

    public Map<String, Object> toJsonMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("topic", topic);
        map.put("partition", partition);
        map.put("leader", leaderId);
        map.put("replicas", replicas);
        map.put("isr", new ArrayList<>(inSyncReplicas));
        return map;
    }
}
