package com.example.minikafka.util;

public final class HashUtil {
    private HashUtil() {}

    public static int positiveHash(String value) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x01000193;
        }
        return hash & 0x7fffffff;
    }

    public static int partitionForKey(String key, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return positiveHash(key == null ? "" : key) % partitionCount;
    }
}
