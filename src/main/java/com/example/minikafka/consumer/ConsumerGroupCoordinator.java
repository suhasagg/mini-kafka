package com.example.minikafka.consumer;

import com.example.minikafka.model.TopicPartition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ConsumerGroupCoordinator {
    private final Map<String, Map<TopicPartition, Long>> offsets = new ConcurrentHashMap<>();

    public long committedOffset(String groupId, TopicPartition tp) {
        return offsets.computeIfAbsent(groupId, ignored -> new ConcurrentHashMap<>()).getOrDefault(tp, 0L);
    }

    public void commit(String groupId, TopicPartition tp, long nextOffset) {
        if (nextOffset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        offsets.computeIfAbsent(groupId, ignored -> new ConcurrentHashMap<>()).put(tp, nextOffset);
    }

    public Map<String, Object> offsetsForGroup(String groupId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("group", groupId);
        Map<String, Object> groupOffsets = new LinkedHashMap<>();
        Map<TopicPartition, Long> current = offsets.getOrDefault(groupId, Map.of());
        current.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> groupOffsets.put(entry.getKey().id(), entry.getValue()));
        result.put("offsets", groupOffsets);
        return result;
    }
}
