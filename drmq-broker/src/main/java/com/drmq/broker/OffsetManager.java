package com.drmq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages consumer group offsets with persistence to disk.
 *
 * Offsets are stored in:
 *   <dataDir>/__consumer_offsets/offsets.properties
 *
 * Key format:   <consumer_group>/<topic>
 * Value format: <offset> (long)
 */
public class OffsetManager implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);
    private static final String OFFSETS_DIR  = "__consumer_offsets";
    private static final String OFFSETS_FILE = "offsets.properties";

    private final Path offsetsFile;

    // group/topic -> committed offset
    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();
    
    private final AtomicBoolean isDirty = new AtomicBoolean(false);
    private final ReentrantLock persistLock = new ReentrantLock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public OffsetManager(String dataDir) throws IOException {
        Path dir = Paths.get(dataDir, OFFSETS_DIR);
        Files.createDirectories(dir);
        this.offsetsFile = dir.resolve(OFFSETS_FILE);
        load();
        
        scheduler.scheduleWithFixedDelay(this::backgroundPersist, 1000, 1000, TimeUnit.MILLISECONDS);
    }


    /**
     * Commit (persist) a consumer group's offset for a topic.
     *
     * @param consumerGroup the consumer group identifier
     * @param topic         the topic name
     * @param offset        the NEXT offset to read (i.e. last processed offset + 1)
     */
    public void commit(String consumerGroup, String topic, long offset) {
        String key = key(consumerGroup, topic);
        offsets.put(key, offset);
        isDirty.lazySet(true);
        logger.debug("Committed offset in memory: group={}, topic={}, offset={}", consumerGroup, topic, offset);
    }

    /**
     * Fetch the committed offset for a consumer group / topic pair.
     *
     * @return the committed offset, or -1 if no offset has been committed yet
     */
    public long fetch(String consumerGroup, String topic) {
        return offsets.getOrDefault(key(consumerGroup, topic), -1L);
    }

    public int getOffsetEntryCount() {
        return offsets.size();
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

    private void backgroundPersist() {
        if (!isDirty.getAndSet(false)) return;

        persistLock.lock();
        try {
            long startNanos = System.nanoTime();
            try {
                persist();
                BrokerMetrics.get().recordOffsetPersist(System.nanoTime() - startNanos, true);
            } catch (IOException e) {
                logger.error("Failed to background persist offsets", e);
                BrokerMetrics.get().recordOffsetPersist(System.nanoTime() - startNanos, false);
            }
        } finally {
            persistLock.unlock();
        }
    }

    private void persist() throws IOException {
        Properties props = new Properties();
        offsets.forEach((k, v) -> props.setProperty(k, String.valueOf(v)));

        Path tmp = offsetsFile.resolveSibling(OFFSETS_FILE + ".tmp");
        try (OutputStream out = Files.newOutputStream(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            props.store(out, "DRMQ Consumer Offsets");
        }
        Files.move(tmp, offsetsFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        if (isDirty.getAndSet(false)) {
            persistLock.lock();
            try {
                persist();
            } finally {
                persistLock.unlock();
            }
        }
    }

    private static String key(String consumerGroup, String topic) {
        return consumerGroup + "/" + topic;
    }
}
