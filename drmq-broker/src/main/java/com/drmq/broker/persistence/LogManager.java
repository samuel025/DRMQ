package com.drmq.broker.persistence;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Manages all log segments across different topics.
 * Handles directory structure and recovery on startup.
 */
public class LogManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogManager.class);
    private static final String DEFAULT_DATA_DIR = "./data";
    private static final String LOG_FILE_SUFFIX = ".log";
    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("^[A-Za-z0-9._-]+$");

    private final Path dataDir;
    private final BrokerConfig config;
    
    // topic -> (baseOffset -> LogSegment)
    private final Map<String, ConcurrentSkipListMap<Long, LogSegment>> topicSegments = new ConcurrentHashMap<>();

    public LogManager(BrokerConfig config) throws IOException {
        this.config = config;
        this.dataDir = Paths.get(config.getDataDir());
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        logger.info("LogManager initialized with data directory: {}", dataDir.toAbsolutePath());
    }

    public LogManager(String dataDirStr) throws IOException {
        this(new BrokerConfig(-1, dataDirStr));
    }

    public LogManager() throws IOException {
        this(DEFAULT_DATA_DIR);
    }

    /**
     * Get the active log segment for a topic (the one with the highest base offset).
     * If no segment exists, creates the first one starting at offset 0.
     */
    public LogSegment getOrCreateActiveSegment(String topic) throws IOException {
        validateTopic(topic);
        
        ConcurrentSkipListMap<Long, LogSegment> segments = topicSegments.computeIfAbsent(topic, k -> new ConcurrentSkipListMap<>());
        
        if (!segments.isEmpty()) {
            return segments.lastEntry().getValue();
        }
        
        synchronized (segments) {
            if (!segments.isEmpty()) {
                return segments.lastEntry().getValue();
            }
            
            Path topicDir = dataDir.resolve(topic);
            if (!Files.exists(topicDir)) {
                Files.createDirectories(topicDir);
            }
            Path logPath = topicDir.resolve(String.format("%020d" + LOG_FILE_SUFFIX, 0L));
            LogSegment segment = new LogSegment(logPath);
            segments.put(0L, segment);
            BrokerMetrics.get().registerLogSegment(topic, segment);
            return segment;
        }
    }

    /**
     * Rolls over to a new log segment for a topic.
     */
    public LogSegment rollNewSegment(String topic, long baseOffset) throws IOException {
        validateTopic(topic);
        ConcurrentSkipListMap<Long, LogSegment> segments = topicSegments.get(topic);
        if (segments == null) {
            return getOrCreateActiveSegment(topic);
        }

        synchronized (segments) {
            if (!segments.isEmpty() && segments.lastKey() >= baseOffset) {
                return segments.lastEntry().getValue();
            }
            
            Path topicDir = dataDir.resolve(topic);
            Path logPath = topicDir.resolve(String.format("%020d" + LOG_FILE_SUFFIX, baseOffset));
            LogSegment segment = new LogSegment(logPath);
            segments.put(baseOffset, segment);
            BrokerMetrics.get().registerLogSegment(topic, segment);
            logger.info("Rolled new log segment for topic {}: {}", topic, logPath.getFileName());
            return segment;
        }
    }

    /**
     * Get the segment that should contain the given offset.
     */
    public LogSegment getSegmentForOffset(String topic, long offset) {
        ConcurrentSkipListMap<Long, LogSegment> segments = topicSegments.get(topic);
        if (segments != null) {
            Map.Entry<Long, LogSegment> entry = segments.floorEntry(offset);
            if (entry != null) {
                return entry.getValue();
            }
        }
        return null;
    }

    public Map<String, ConcurrentSkipListMap<Long, LogSegment>> getAllSegments() {
        return topicSegments;
    }

    private void validateTopic(String topic) {
        if (topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException(
                "Invalid topic name: '" + topic + "'. Topic names cannot be '.' or '..'");
        }
        if (topic.equals("raft") || topic.equals("__consumer_offsets")) {
            throw new IllegalArgumentException(
                "Invalid topic name: '" + topic + "'. Reserved system directory name.");
        }
        if (!TOPIC_NAME_PATTERN.matcher(topic).matches()) {
            throw new IllegalArgumentException(
                "Invalid topic name: '" + topic + "'. Topic names must match pattern: [A-Za-z0-9._-]+");
        }
    }

    /**
     * Recovery: Scan the data directory and load existing segments.
     * This populates the internal maps and returns them.
     */
    public Map<String, List<Path>> discoverSegments() throws IOException {
        Map<String, List<Path>> discovered = new ConcurrentHashMap<>();
        if (!Files.exists(dataDir)) return discovered;

        try (Stream<Path> topicDirs = Files.list(dataDir)) {
            topicDirs.filter(Files::isDirectory).forEach(topicDir -> {
                String topic = topicDir.getFileName().toString();
                if (topic.equals("raft") || topic.equals("__consumer_offsets")) {
                    return;
                }
                List<Path> segmentPaths = new ArrayList<>();
                try (Stream<Path> files = Files.list(topicDir)) {
                    files.filter(p -> p.toString().endsWith(LOG_FILE_SUFFIX))
                         .forEach(segmentPaths::add);
                } catch (IOException e) {
                    logger.error("Error listing segments for topic {}", topic, e);
                }
                
                if (!segmentPaths.isEmpty()) {
                    segmentPaths.sort((p1, p2) -> p1.getFileName().compareTo(p2.getFileName()));
                    discovered.put(topic, segmentPaths);
                    
                    ConcurrentSkipListMap<Long, LogSegment> segments = topicSegments.computeIfAbsent(topic, k -> new ConcurrentSkipListMap<>());
                    for (Path logPath : segmentPaths) {
                        try {
                            LogSegment segment = new LogSegment(logPath);
                            segments.put(segment.getBaseOffset(), segment);
                        } catch (IOException e) {
                            logger.error("Failed to load segment: {}", logPath, e);
                        }
                    }
                }
            });
        }
        return discovered;
    }

    public int getOpenSegmentCount() {
        return topicSegments.values().stream().mapToInt(ConcurrentSkipListMap::size).sum();
    }

    @Override
    public void close() throws IOException {
        IOException primaryException = null;
        
        // Attempt to close all segments, collecting exceptions
        for (ConcurrentSkipListMap<Long, LogSegment> segments : topicSegments.values()) {
            for (LogSegment segment : segments.values()) {
                try {
                    segment.close();
                } catch (IOException e) {
                    logger.error("Error closing segment for topic: {}", e.getMessage(), e);
                    if (primaryException == null) {
                        primaryException = e;
                    } else {
                        primaryException.addSuppressed(e);
                    }
                }
            }
        }
        
        topicSegments.clear();
        
        // Rethrow primary exception if any close failed
        if (primaryException != null) {
            throw primaryException;
        }
    }
}
