package com.example.minikafka.model;

import java.util.Objects;

public final class TopicPartition implements Comparable<TopicPartition> {
    private final String topic;
    private final int partition;

    public TopicPartition(String topic, int partition) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0");
        }
        this.topic = topic;
        this.partition = partition;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public String id() {
        return topic + "-" + partition;
    }

    @Override
    public int compareTo(TopicPartition other) {
        int topicCompare = topic.compareTo(other.topic);
        if (topicCompare != 0) {
            return topicCompare;
        }
        return Integer.compare(partition, other.partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicPartition that)) return false;
        return partition == that.partition && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return id();
    }
}
