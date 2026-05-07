package com.example.minikafka.model;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ProduceResult {
    private final String topic;
    private final int partition;
    private final long offset;
    private final String leader;
    private final int acknowledgements;

    public ProduceResult(String topic, int partition, long offset, String leader, int acknowledgements) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.leader = leader;
        this.acknowledgements = acknowledgements;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public String leader() {
        return leader;
    }

    public int acknowledgements() {
        return acknowledgements;
    }

    public Map<String, Object> toJsonMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("status", "OK");
        map.put("topic", topic);
        map.put("partition", partition);
        map.put("offset", offset);
        map.put("leader", leader);
        map.put("acks", acknowledgements);
        return map;
    }
}
