package com.example.minikafka.cluster;

public final class ReplicationException extends RuntimeException {
    public ReplicationException(String message) {
        super(message);
    }

    public ReplicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
