package com.example.minikafka.model;

import com.example.minikafka.util.Encoding;

import java.util.LinkedHashMap;
import java.util.Map;

public final class MessageRecord {
    private final String topic;
    private final int partition;
    private final long offset;
    private final String key;
    private final String value;
    private final long timestamp;

    public MessageRecord(String topic, int partition, long offset, String key, String value, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key == null ? "" : key;
        this.value = value == null ? "" : value;
        this.timestamp = timestamp;
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

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    public String toLogLine() {
        return offset + "\t" + timestamp + "\t" + Encoding.b64(key) + "\t" + Encoding.b64(value);
    }

    public static MessageRecord fromLogLine(String topic, int partition, String line) {
        String[] parts = line.split("\t", 4);
        if (parts.length != 4) {
            throw new IllegalArgumentException("invalid log line: " + line);
        }
        long offset = Long.parseLong(parts[0]);
        long timestamp = Long.parseLong(parts[1]);
        String key = Encoding.fromB64(parts[2]);
        String value = Encoding.fromB64(parts[3]);
        return new MessageRecord(topic, partition, offset, key, value, timestamp);
    }

    public Map<String, Object> toJsonMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("topic", topic);
        map.put("partition", partition);
        map.put("offset", offset);
        map.put("key", key);
        map.put("value", value);
        map.put("timestamp", timestamp);
        return map;
    }
}
