package com.drmq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer group offsets with persistence to disk.
 *
 * Offsets are stored in:
 *   <dataDir>/__consumer_offsets/offsets.properties
 *
 * Key format:   <consumer_group>/<topic>
 * Value format: <offset> (long)
 */
public class OffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);
    private static final String OFFSETS_DIR  = "__consumer_offsets";
    private static final String OFFSETS_FILE = "offsets.properties";

    private final Path offsetsFile;

    // group/topic -> committed offset
    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();

    public OffsetManager(String dataDir) throws IOException {
        Path dir = Paths.get(dataDir, OFFSETS_DIR);
        Files.createDirectories(dir);
        this.offsetsFile = dir.resolve(OFFSETS_FILE);
        load();
    }


    /**
     * Commit (persist) a consumer group's offset for a topic.
     *
     * @param consumerGroup the consumer group identifier
     * @param topic         the topic name
     * @param offset        the NEXT offset to read (i.e. last processed offset + 1)
     */
    public synchronized void commit(String consumerGroup, String topic, long offset) throws IOException {
        String key = key(consumerGroup, topic);
        offsets.put(key, offset);
        persist();
        logger.debug("Committed offset: group={}, topic={}, offset={}", consumerGroup, topic, offset);
    }

    /**
     * Fetch the committed offset for a consumer group / topic pair.
     *
     * @return the committed offset, or -1 if no offset has been committed yet
     */
    public long fetch(String consumerGroup, String topic) {
        return offsets.getOrDefault(key(consumerGroup, topic), -1L);
    }

    /** Load all offsets from disk on startup. */
    private void load() throws IOException {
        if (!Files.exists(offsetsFile)) {
            logger.info("No existing offset file found at {}. Starting fresh.", offsetsFile);
            return;
        }

        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(offsetsFile)) {
            props.load(in);
        }

        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            try {
                offsets.put((String) entry.getKey(), Long.parseLong((String) entry.getValue()));
            } catch (NumberFormatException e) {
                logger.warn("Skipping malformed offset entry: {}={}", entry.getKey(), entry.getValue());
            }
        }

        logger.info("Loaded {} consumer offset(s) from {}", offsets.size(), offsetsFile);
    }

    /** Flush all offsets to disk atomically (write to tmp then rename). */
    private void persist() throws IOException {
        Properties props = new Properties();
        offsets.forEach((k, v) -> props.setProperty(k, String.valueOf(v)));

        // Write to a temp file first, then atomically rename to avoid corruption
        Path tmp = offsetsFile.resolveSibling(OFFSETS_FILE + ".tmp");
        try (OutputStream out = Files.newOutputStream(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            props.store(out, "DRMQ Consumer Offsets");
        }
        Files.move(tmp, offsetsFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }


    private static String key(String consumerGroup, String topic) {
        return consumerGroup + "/" + topic;
    }
}
