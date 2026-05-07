package com.example.minikafka;

import com.example.minikafka.consumer.ConsumerGroupCoordinator;
import com.example.minikafka.model.TopicPartition;

public final class ConsumerGroupCoordinatorTest {
    public static void run() {
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator();
        TopicPartition tp = new TopicPartition("orders", 0);
        TestSupport.assertEquals(0L, coordinator.committedOffset("g1", tp), "default offset");
        coordinator.commit("g1", tp, 42);
        TestSupport.assertEquals(42L, coordinator.committedOffset("g1", tp), "committed offset");
    }
}
