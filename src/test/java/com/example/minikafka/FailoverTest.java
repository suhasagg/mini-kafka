package com.example.minikafka;

import com.example.minikafka.cluster.MiniKafkaCluster;
import com.example.minikafka.config.BrokerConfig;
import com.example.minikafka.model.MessageRecord;
import com.example.minikafka.model.ProduceResult;

import java.nio.file.Path;
import java.util.List;

public final class FailoverTest {
    public static void run() throws Exception {
        Path dir = TestSupport.tempDir("mini-kafka-failover-test");
        try {
            MiniKafkaCluster cluster = new MiniKafkaCluster(new BrokerConfig(0, dir, 3, 10, 1));
            cluster.createTopic("payments", 1, 3);
            ProduceResult first = cluster.produce("payments", "k1", "before-failure", "all");
            TestSupport.assertEquals("broker-0", first.leader(), "initial leader");

            cluster.brokerDown("broker-0");
            ProduceResult second = cluster.produce("payments", "k1", "after-failure", "1");
            TestSupport.assertTrue(!second.leader().equals("broker-0"), "leader should fail over");

            cluster.brokerUp("broker-0");
            List<MessageRecord> records = cluster.fetch("payments", 0, 0, 10);
            TestSupport.assertEquals(2, records.size(), "records after recovery");
            TestSupport.assertEquals("before-failure", records.get(0).value(), "first value");
            TestSupport.assertEquals("after-failure", records.get(1).value(), "second value");
        } finally {
            TestSupport.deleteRecursively(dir);
        }
    }
}
