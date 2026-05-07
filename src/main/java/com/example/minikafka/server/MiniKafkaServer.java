package com.example.minikafka.server;

import com.example.minikafka.cluster.MiniKafkaCluster;
import com.example.minikafka.config.BrokerConfig;
import com.example.minikafka.model.MessageRecord;
import com.example.minikafka.model.ProduceResult;
import com.example.minikafka.util.JsonUtil;
import com.example.minikafka.util.QueryString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public final class MiniKafkaServer {
    private final BrokerConfig config;
    private final MiniKafkaCluster cluster;
    private final HttpServer server;

    public MiniKafkaServer(BrokerConfig config, MiniKafkaCluster cluster) throws IOException {
        this.config = config;
        this.cluster = cluster;
        this.server = HttpServer.create(new InetSocketAddress(config.port()), 0);
        this.server.createContext("/health", this::handleHealth);
        this.server.createContext("/topics", this::handleTopics);
        this.server.createContext("/produce", this::handleProduce);
        this.server.createContext("/consume", this::handleConsume);
        this.server.createContext("/consumer/poll", this::handleConsumerPoll);
        this.server.createContext("/consumer/commit", this::handleConsumerCommit);
        this.server.createContext("/consumer/offsets", this::handleConsumerOffsets);
        this.server.createContext("/cluster/metadata", this::handleMetadata);
        this.server.createContext("/brokers", this::handleBrokers);
        this.server.createContext("/segments", this::handleSegments);
        this.server.setExecutor(Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors())));
    }

    public void start() {
        server.start();
        System.out.println("MiniKafkaServer listening on http://localhost:" + config.port());
    }

    public void stop() {
        server.stop(0);
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "GET");
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "UP");
            response.put("service", "mini-kafka");
            response.put("brokers", cluster.brokerCount());
            response.put("aliveBrokers", cluster.aliveBrokerCount());
            sendJson(exchange, 200, response);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleTopics(HttpExchange exchange) throws IOException {
        try {
            if (exchange.getRequestMethod().equalsIgnoreCase("POST")) {
                Map<String, String> q = QueryString.parse(exchange.getRequestURI());
                String name = required(q, "name");
                int partitions = intParam(q, "partitions", 1);
                int replicationFactor = intParam(q, "replicationFactor", 1);
                cluster.createTopic(name, partitions, replicationFactor);
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("status", "OK");
                response.put("topic", name);
                response.put("partitions", partitions);
                response.put("replicationFactor", replicationFactor);
                sendJson(exchange, 200, response);
            } else if (exchange.getRequestMethod().equalsIgnoreCase("GET")) {
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("topics", cluster.listTopics());
                sendJson(exchange, 200, response);
            } else {
                throw new IllegalArgumentException("method not allowed");
            }
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleProduce(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "POST");
            Map<String, String> q = QueryString.parse(exchange.getRequestURI());
            String topic = required(q, "topic");
            String key = q.getOrDefault("key", "");
            String acks = q.getOrDefault("acks", "1");
            String value = body(exchange);
            ProduceResult result = cluster.produce(topic, key, value, acks);
            sendJson(exchange, 200, result.toJsonMap());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleConsume(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "GET");
            Map<String, String> q = QueryString.parse(exchange.getRequestURI());
            String topic = required(q, "topic");
            int partition = intParam(q, "partition", 0);
            long offset = longParam(q, "offset", 0);
            int max = intParam(q, "max", 10);
            List<MessageRecord> records = cluster.fetch(topic, partition, offset, max);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("topic", topic);
            response.put("partition", partition);
            response.put("offset", offset);
            response.put("records", recordMaps(records));
            sendJson(exchange, 200, response);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleConsumerPoll(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "GET");
            Map<String, String> q = QueryString.parse(exchange.getRequestURI());
            String group = required(q, "group");
            String topic = required(q, "topic");
            int max = intParam(q, "max", 10);
            boolean autoCommit = Boolean.parseBoolean(q.getOrDefault("autoCommit", "false"));
            List<MessageRecord> records = cluster.poll(group, topic, max, autoCommit);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("group", group);
            response.put("topic", topic);
            response.put("autoCommit", autoCommit);
            response.put("records", recordMaps(records));
            sendJson(exchange, 200, response);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleConsumerCommit(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "POST");
            Map<String, String> q = QueryString.parse(exchange.getRequestURI());
            String group = required(q, "group");
            String topic = required(q, "topic");
            int partition = intParam(q, "partition", 0);
            long offset = longParam(q, "offset", 0);
            cluster.commit(group, topic, partition, offset);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "OK");
            response.put("group", group);
            response.put("topic", topic);
            response.put("partition", partition);
            response.put("offset", offset);
            sendJson(exchange, 200, response);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleConsumerOffsets(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "GET");
            Map<String, String> q = QueryString.parse(exchange.getRequestURI());
            String group = required(q, "group");
            sendJson(exchange, 200, cluster.offsets(group));
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleMetadata(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "GET");
            sendJson(exchange, 200, cluster.metadata());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleSegments(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "GET");
            Map<String, String> q = QueryString.parse(exchange.getRequestURI());
            String topic = required(q, "topic");
            int partition = intParam(q, "partition", 0);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("topic", topic);
            response.put("partition", partition);
            response.put("segments", cluster.segmentInfo(topic, partition));
            sendJson(exchange, 200, response);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleBrokers(HttpExchange exchange) throws IOException {
        try {
            requireMethod(exchange, "POST");
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            if (parts.length != 4) {
                throw new IllegalArgumentException("expected /brokers/{brokerId}/down or /brokers/{brokerId}/up");
            }
            String brokerId = parts[2];
            String action = parts[3];
            if (action.equals("down")) {
                cluster.brokerDown(brokerId);
            } else if (action.equals("up")) {
                cluster.brokerUp(brokerId);
            } else {
                throw new IllegalArgumentException("unknown broker action: " + action);
            }
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "OK");
            response.put("broker", brokerId);
            response.put("action", action);
            sendJson(exchange, 200, response);
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private List<Map<String, Object>> recordMaps(List<MessageRecord> records) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (MessageRecord record : records) {
            list.add(record.toJsonMap());
        }
        return list;
    }

    private void requireMethod(HttpExchange exchange, String method) {
        if (!exchange.getRequestMethod().equalsIgnoreCase(method)) {
            throw new IllegalArgumentException("method not allowed, expected " + method);
        }
    }

    private String required(Map<String, String> params, String name) {
        String value = params.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required parameter: " + name);
        }
        return value;
    }

    private int intParam(Map<String, String> params, String name, int defaultValue) {
        String value = params.get(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private long longParam(Map<String, String> params, String name, long defaultValue) {
        String value = params.get(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
    }

    private String body(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void sendJson(HttpExchange exchange, int status, Map<String, Object> map) throws IOException {
        send(exchange, status, JsonUtil.object(map));
    }

    private void sendError(HttpExchange exchange, Exception e) throws IOException {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "ERROR");
        response.put("error", e.getMessage());
        send(exchange, 400, JsonUtil.object(response));
    }

    private void send(HttpExchange exchange, int status, String response) throws IOException {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
