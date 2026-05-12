package com.drmq.broker.persistence;

import com.drmq.broker.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<String, LogSegment> topicSegments = new ConcurrentHashMap<>();

    public LogManager(String dataDirStr) throws IOException {
        this.dataDir = Paths.get(dataDirStr != null ? dataDirStr : DEFAULT_DATA_DIR);
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        logger.info("LogManager initialized with data directory: {}", dataDir.toAbsolutePath());
    }

    public LogManager() throws IOException {
        this(DEFAULT_DATA_DIR);
    }

    /**
     * Get or create a log segment for a topic.
     */
    public LogSegment getOrCreateSegment(String topic) throws IOException {
        if (topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException(
                "Invalid topic name: '" + topic + "'. Topic names cannot be '.' or '..'");
        }
        
        if (!TOPIC_NAME_PATTERN.matcher(topic).matches()) {
            throw new IllegalArgumentException(
                "Invalid topic name: '" + topic + "'. Topic names must match pattern: [A-Za-z0-9._-]+");
        }
        
        // Check if segment already exists
        LogSegment existing = topicSegments.get(topic);
        if (existing != null) {
            return existing;
        }
        
        synchronized (this) {
            existing = topicSegments.get(topic);
            if (existing != null) {
                return existing;
            }
            
            Path topicDir = dataDir.resolve(topic);
            if (!Files.exists(topicDir)) {
                Files.createDirectories(topicDir);
            }
            // For Phase 2, we use a single segment "00000000.log" per topic
            Path logPath = topicDir.resolve("00000000" + LOG_FILE_SUFFIX);
            LogSegment segment = new LogSegment(logPath);
            topicSegments.put(topic, segment);
            BrokerMetrics.get().registerLogSegment(topic, segment);
            return segment;
        }
    }

    /**
     * Recovery: Scan the data directory and load existing segments.
     * This allows rebuild of memory index by the caller.
     */
    public Map<String, Path> discoverSegments() throws IOException {
        Map<String, Path> segments = new ConcurrentHashMap<>();
        if (!Files.exists(dataDir)) return segments;

        try (Stream<Path> topicDirs = Files.list(dataDir)) {
            topicDirs.filter(Files::isDirectory).forEach(topicDir -> {
                String topic = topicDir.getFileName().toString();
                Path logPath = topicDir.resolve("00000000" + LOG_FILE_SUFFIX);
                if (Files.exists(logPath)) {
                    segments.put(topic, logPath);
                }
            });
        }
        return segments;
    }

    public int getOpenSegmentCount() {
        return topicSegments.size();
    }

    @Override
    public void close() throws IOException {
        IOException primaryException = null;
        
        // Attempt to close all segments, collecting exceptions
        for (Map.Entry<String, LogSegment> entry : topicSegments.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                logger.error("Error closing segment for topic {}: {}", entry.getKey(), e.getMessage(), e);
                if (primaryException == null) {
                    primaryException = e;
                } else {
                    primaryException.addSuppressed(e);
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
