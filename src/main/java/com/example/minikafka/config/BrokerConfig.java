package com.example.minikafka.config;

import java.nio.file.Path;

public final class BrokerConfig {
    private final int port;
    private final Path dataDir;
    private final int brokerCount;
    private final int segmentMaxRecords;
    private final int minInSyncReplicas;

    public BrokerConfig(int port, Path dataDir, int brokerCount, int segmentMaxRecords, int minInSyncReplicas) {
        if (brokerCount <= 0) {
            throw new IllegalArgumentException("brokerCount must be positive");
        }
        if (segmentMaxRecords <= 0) {
            throw new IllegalArgumentException("segmentMaxRecords must be positive");
        }
        if (minInSyncReplicas <= 0) {
            throw new IllegalArgumentException("minInSyncReplicas must be positive");
        }
        this.port = port;
        this.dataDir = dataDir;
        this.brokerCount = brokerCount;
        this.segmentMaxRecords = segmentMaxRecords;
        this.minInSyncReplicas = minInSyncReplicas;
    }

    public int port() {
        return port;
    }

    public Path dataDir() {
        return dataDir;
    }

    public int brokerCount() {
        return brokerCount;
    }

    public int segmentMaxRecords() {
        return segmentMaxRecords;
    }

    public int minInSyncReplicas() {
        return minInSyncReplicas;
    }

    public static BrokerConfig fromEnvironment() {
        int port = intEnv("PORT", 9092);
        Path dataDir = Path.of(strEnv("DATA_DIR", "data"));
        int brokers = intEnv("BROKERS", 3);
        int segmentMaxRecords = intEnv("SEGMENT_MAX_RECORDS", 50);
        int minIsr = intEnv("MIN_IN_SYNC_REPLICAS", 2);
        return new BrokerConfig(port, dataDir, brokers, segmentMaxRecords, minIsr);
    }

    private static String strEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int intEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value.trim());
    }
}
