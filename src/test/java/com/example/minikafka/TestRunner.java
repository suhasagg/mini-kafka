package com.example.minikafka;

public final class TestRunner {
    private TestRunner() {}

    public static void main(String[] args) throws Exception {
        run("AppendOnlyLogTest", AppendOnlyLogTest::run);
        run("MiniKafkaClusterTest", MiniKafkaClusterTest::run);
        run("ConsumerGroupCoordinatorTest", ConsumerGroupCoordinatorTest::run);
        run("FailoverTest", FailoverTest::run);
        System.out.println("All tests passed.");
    }

    private static void run(String name, ThrowingRunnable runnable) throws Exception {
        runnable.run();
        System.out.println("[PASS] " + name);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
