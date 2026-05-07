package com.example.minikafka.log;

import com.example.minikafka.model.MessageRecord;
import com.example.minikafka.util.Time;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class AppendOnlyLog {
    private final String topic;
    private final int partition;
    private final Path directory;
    private final int segmentMaxRecords;
    private final List<LogSegment> segments = new ArrayList<>();
    private long nextOffset;

    public AppendOnlyLog(String topic, int partition, Path directory, int segmentMaxRecords) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.directory = directory;
        this.segmentMaxRecords = segmentMaxRecords;
        Files.createDirectories(directory);
        loadSegments();
    }

    public synchronized MessageRecord append(String key, String value) throws IOException {
        MessageRecord record = new MessageRecord(topic, partition, nextOffset, key, value, Time.nowMillis());
        appendInternal(record);
        return record;
    }

    public synchronized void forceAppend(MessageRecord record) throws IOException {
        if (record.offset() < nextOffset) {
            return;
        }
        if (record.offset() > nextOffset) {
            throw new IOException("cannot append offset " + record.offset() + " when nextOffset is " + nextOffset);
        }
        appendInternal(record);
    }

    public synchronized List<MessageRecord> readFrom(long offset, int maxRecords) throws IOException {
        List<MessageRecord> result = new ArrayList<>();
        for (LogSegment segment : segments) {
            if (result.size() >= maxRecords) {
                break;
            }
            result.addAll(segment.readFrom(offset, maxRecords - result.size()));
        }
        return result;
    }

    public synchronized List<MessageRecord> readAll() throws IOException {
        return readFrom(0, Integer.MAX_VALUE);
    }

    public synchronized long endOffset() {
        return nextOffset;
    }

    public synchronized List<Map<String, Object>> segmentInfo() {
        List<Map<String, Object>> info = new ArrayList<>();
        for (LogSegment segment : segments) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("baseOffset", segment.baseOffset());
            map.put("file", segment.file().toString());
            map.put("records", segment.recordCount());
            info.add(map);
        }
        return info;
    }

    private void appendInternal(MessageRecord record) throws IOException {
        LogSegment active = activeSegment();
        if (active.recordCount() >= segmentMaxRecords) {
            active = createSegment(record.offset());
        }
        active.append(record);
        nextOffset = record.offset() + 1;
    }

    private LogSegment activeSegment() throws IOException {
        if (segments.isEmpty()) {
            return createSegment(0);
        }
        return segments.get(segments.size() - 1);
    }

    private LogSegment createSegment(long baseOffset) throws IOException {
        Path file = directory.resolve(LogSegment.fileName(baseOffset));
        LogSegment segment = new LogSegment(topic, partition, baseOffset, file);
        segments.add(segment);
        segments.sort(Comparator.comparingLong(LogSegment::baseOffset));
        return segment;
    }

    private void loadSegments() throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "segment-*.log")) {
            for (Path file : stream) {
                files.add(file);
            }
        }
        files.sort(Comparator.comparing(Path::getFileName));
        long maxOffset = -1;
        for (Path file : files) {
            long baseOffset = parseBaseOffset(file.getFileName().toString());
            LogSegment segment = new LogSegment(topic, partition, baseOffset, file);
            segments.add(segment);
            for (MessageRecord record : segment.readAll()) {
                maxOffset = Math.max(maxOffset, record.offset());
            }
        }
        if (segments.isEmpty()) {
            createSegment(0);
        }
        nextOffset = maxOffset + 1;
    }

    private long parseBaseOffset(String fileName) {
        String stripped = fileName.replace("segment-", "").replace(".log", "");
        return Long.parseLong(stripped);
    }
}
