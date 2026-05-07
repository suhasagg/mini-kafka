package com.example.minikafka.log;

import com.example.minikafka.model.MessageRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public final class LogSegment {
    private final String topic;
    private final int partition;
    private final long baseOffset;
    private final Path file;
    private int recordCount;

    public LogSegment(String topic, int partition, long baseOffset, Path file) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.baseOffset = baseOffset;
        this.file = file;
        Files.createDirectories(file.getParent());
        if (!Files.exists(file)) {
            Files.createFile(file);
        }
        this.recordCount = countLines(file);
    }

    public long baseOffset() {
        return baseOffset;
    }

    public Path file() {
        return file;
    }

    public int recordCount() {
        return recordCount;
    }

    public void append(MessageRecord record) throws IOException {
        Files.writeString(file, record.toLogLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        recordCount++;
    }

    public List<MessageRecord> readFrom(long offset, int maxRecords) throws IOException {
        List<MessageRecord> records = new ArrayList<>();
        try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
            for (String line : (Iterable<String>) lines::iterator) {
                if (line == null || line.isBlank()) {
                    continue;
                }
                MessageRecord record = MessageRecord.fromLogLine(topic, partition, line);
                if (record.offset() >= offset) {
                    records.add(record);
                    if (records.size() >= maxRecords) {
                        break;
                    }
                }
            }
        }
        return records;
    }

    public List<MessageRecord> readAll() throws IOException {
        return readFrom(0, Integer.MAX_VALUE);
    }

    private int countLines(Path file) throws IOException {
        try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
            return (int) lines.filter(line -> !line.isBlank()).count();
        }
    }

    public static String fileName(long baseOffset) {
        return String.format("segment-%020d.log", baseOffset);
    }
}
