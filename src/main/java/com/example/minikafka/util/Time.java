package com.example.minikafka.util;

public final class Time {
    private Time() {}

    public static long nowMillis() {
        return System.currentTimeMillis();
    }
}
