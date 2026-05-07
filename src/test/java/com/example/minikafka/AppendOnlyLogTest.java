package com.example.minikafka;

import com.example.minikafka.log.AppendOnlyLog;
import com.example.minikafka.model.MessageRecord;

import java.nio.file.Path;
import java.util.List;

public final class AppendOnlyLogTest {
    public static void run() throws Exception {
        Path dir = TestSupport.tempDir("mini-kafka-log-test");
        try {
            AppendOnlyLog log = new AppendOnlyLog("orders", 0, dir, 2);
            MessageRecord r0 = log.append("k1", "v1");
            MessageRecord r1 = log.append("k2", "v2");
            MessageRecord r2 = log.append("k3", "v3");

            TestSupport.assertEquals(0L, r0.offset(), "first offset");
            TestSupport.assertEquals(1L, r1.offset(), "second offset");
            TestSupport.assertEquals(2L, r2.offset(), "third offset");
            TestSupport.assertEquals(3L, log.endOffset(), "end offset");
            TestSupport.assertTrue(log.segmentInfo().size() >= 2, "segment rollover expected");

            List<MessageRecord> records = log.readFrom(1, 10);
            TestSupport.assertEquals(2, records.size(), "read size");
            TestSupport.assertEquals("v2", records.get(0).value(), "read value");
        } finally {
            TestSupport.deleteRecursively(dir);
        }
    }
}
