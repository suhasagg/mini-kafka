package com.example.minikafka;

import com.example.minikafka.cluster.MiniKafkaCluster;
import com.example.minikafka.config.BrokerConfig;
import com.example.minikafka.server.MiniKafkaServer;

public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.fromEnvironment();
        MiniKafkaCluster cluster = new MiniKafkaCluster(config);
        MiniKafkaServer server = new MiniKafkaServer(config, cluster);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}
