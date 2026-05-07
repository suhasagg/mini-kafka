package com.example.minikafka;

import com.example.minikafka.cluster.MiniKafkaCluster;
import com.example.minikafka.config.BrokerConfig;
import com.example.minikafka.model.MessageRecord;
import com.example.minikafka.model.ProduceResult;

import java.nio.file.Path;
import java.util.List;

public final class MiniKafkaClusterTest {
    public static void run() throws Exception {
        Path dir = TestSupport.tempDir("mini-kafka-cluster-test");
        try {
            MiniKafkaCluster cluster = new MiniKafkaCluster(new BrokerConfig(0, dir, 3, 10, 2));
            cluster.createTopic("orders", 3, 2);

            ProduceResult result = cluster.produce("orders", "user-1", "created", "all");
            TestSupport.assertEquals("orders", result.topic(), "topic");
            TestSupport.assertTrue(result.offset() == 0L, "offset should be zero");
            TestSupport.assertTrue(result.acknowledgements() >= 2, "acks should include leader and follower");

            List<MessageRecord> fetched = cluster.fetch("orders", result.partition(), 0, 10);
            TestSupport.assertEquals(1, fetched.size(), "fetch one record");
            TestSupport.assertEquals("created", fetched.get(0).value(), "value");

            List<MessageRecord> polled = cluster.poll("g1", "orders", 10, true);
            TestSupport.assertEquals(1, polled.size(), "poll one record");
            TestSupport.assertTrue(cluster.offsets("g1").toString().contains("orders-" + result.partition()), "offset should be committed");
        } finally {
            TestSupport.deleteRecursively(dir);
        }
    }
}
