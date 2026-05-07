package com.example.minikafka.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class TopicMetadata {
    private final String name;
    private final int partitionCount;
    private final int replicationFactor;
    private final Map<Integer, PartitionMetadata> partitions = new LinkedHashMap<>();
    private final AtomicInteger roundRobin = new AtomicInteger();

    public TopicMetadata(String name, int partitionCount, int replicationFactor) {
        this.name = name;
        this.partitionCount = partitionCount;
        this.replicationFactor = replicationFactor;
    }

    public String name() {
        return name;
    }

    public int partitionCount() {
        return partitionCount;
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    public void putPartition(PartitionMetadata metadata) {
        partitions.put(metadata.partition(), metadata);
    }

    public PartitionMetadata partition(int partition) {
        PartitionMetadata metadata = partitions.get(partition);
        if (metadata == null) {
            throw new IllegalArgumentException("unknown partition " + partition + " for topic " + name);
        }
        return metadata;
    }

    public List<PartitionMetadata> partitions() {
        return new ArrayList<>(partitions.values());
    }

    public int nextRoundRobinPartition() {
        return Math.floorMod(roundRobin.getAndIncrement(), partitionCount);
    }

    public Map<String, Object> toJsonMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", name);
        map.put("partitions", partitionCount);
        map.put("replicationFactor", replicationFactor);
        List<Map<String, Object>> partitionMaps = new ArrayList<>();
        for (PartitionMetadata partition : partitions()) {
            partitionMaps.add(partition.toJsonMap());
        }
        map.put("partitionMetadata", partitionMaps);
        return map;
    }
}
