package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.persistence.LogSegment;
import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Message storage for the broker.
 */
public class MessageStore implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MessageStore.class);

    private final AtomicLong globalOffset = new AtomicLong(0);
    private final LogManager logManager;
    private final BrokerConfig config;
    private final ScheduledExecutorService cleanerScheduler = Executors.newSingleThreadScheduledExecutor();

    // Topic -> Offset -> Byte Position in log file (Sparse Index)
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> topicIndex = new ConcurrentHashMap<>();
    
    // Topic -> Total number of messages
    private final ConcurrentHashMap<String, AtomicLong> topicMessageCounts = new ConcurrentHashMap<>();
    
    // In-memory cache for recent messages (Topic -> BoundedMessageCache)
    private final ConcurrentHashMap<String, BoundedMessageCache> messageCache = new ConcurrentHashMap<>();
    
    // Per-topic write locks to make segment-check + rollover + append atomic
    private final ConcurrentHashMap<String, Object> topicWriteLocks = new ConcurrentHashMap<>();
    
    private static final int MAX_CACHE_SIZE_PER_TOPIC = 1000;
    private static final int INDEX_INTERVAL = 1000;
    private static final int MAX_INDEX_ENTRIES = 10000;

    // Global lock to pause appends during snapshot generation
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();

    private final Object messageMonitor = new Object();
    private final AtomicLong messageSignal = new AtomicLong(0);

    public MessageStore(LogManager logManager, BrokerConfig config) {
        this.logManager = logManager;
        this.config = config;
        
        long retentionMs = config.getLogRetentionMs();
        if (retentionMs > 0) {
            cleanerScheduler.scheduleWithFixedDelay(this::cleanupOldSegments, 1, 1, TimeUnit.MINUTES);
        }
    }

    /**
     * Recovery: Rebuild the index from log files on disk.
     */
    public void recover() throws IOException {
        globalLock.writeLock().lock();
        try {
            recoverInternal();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    private void recoverInternal() throws IOException {
        logger.info("Starting message store recovery...");
        // Discover segments triggers loading them into LogManager
        Map<String, List<Path>> segments = logManager.discoverSegments();
        long maxOffset = -1;

        Map<String, ConcurrentSkipListMap<Long, LogSegment>> allSegments = logManager.getAllSegments();

        for (Map.Entry<String, ConcurrentSkipListMap<Long, LogSegment>> entry : allSegments.entrySet()) {
            String topic = entry.getKey();
            
            for (LogSegment segment : entry.getValue().values()) {
                long position = 0;
                try {
                    long segmentSize = segment.getSize();
                    while (position < segmentSize) {
                        StoredMessage message = segment.read(position);
                        long offset = message.getOffset();
                        
                        indexMessage(topic, offset, position);
                        addToCache(topic, message);
                        topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
                        
                        if (offset > maxOffset) {
                            maxOffset = offset;
                        }
                        
                        position += 4 + message.getSerializedSize();
                    }
                } catch (IOException ioe) {
                    logger.warn("Corruption detected in topic {} segment {} at position {}. Truncating: {}", topic, segment.getFilePath(), position, ioe.getMessage());
                    try {
                        segment.truncate(position);
                    } catch (IOException truncateEx) {
                        logger.error("Failed to truncate segment {}", segment.getFilePath(), truncateEx);
                        throw truncateEx;
                    }
                }
            }
        }

        globalOffset.set(maxOffset + 1);
        logger.info("Recovery complete. Global offset set to {}", globalOffset.get());
    }

    /**
     * Completely clear in-memory state and close file handles, then rebuild from disk.
     * Used after installing a Raft snapshot.
     */
    public void reload() throws IOException {
        globalLock.writeLock().lock();
        try {
            logger.info("Reloading MessageStore state from disk...");
            topicIndex.clear();
            topicMessageCounts.clear();
            messageCache.clear();
            topicWriteLocks.clear();
            
            logManager.close();
            
            recoverInternal();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    private void indexMessage(String topic, long offset, long position) {
        if (offset % INDEX_INTERVAL == 0) {
            ConcurrentSkipListMap<Long, Long> index = topicIndex.computeIfAbsent(topic, k -> new ConcurrentSkipListMap<>());
            index.put(offset, position);
            while (index.size() > MAX_INDEX_ENTRIES) {
                index.pollFirstEntry();
            }
        }
    }

    private void addToCache(String topic, StoredMessage message) {
        messageCache.computeIfAbsent(topic, k -> new BoundedMessageCache(MAX_CACHE_SIZE_PER_TOPIC))
                .add(message);
    }

    /**
     * Append a message to the specified topic.
     */
    public long append(String topic, byte[] payload, String key, long clientTimestamp) {
        long offset = globalOffset.getAndIncrement();
        long storedAt = System.currentTimeMillis();

        StoredMessage.Builder builder = StoredMessage.newBuilder()
                .setOffset(offset)
                .setTopic(topic)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setTimestamp(clientTimestamp)
                .setStoredAt(storedAt);

        if (key != null && !key.isEmpty()) {
            builder.setKey(key);
        }

        StoredMessage message = builder.build();

        globalLock.readLock().lock();
        try {
            Object topicLock = topicWriteLocks.computeIfAbsent(topic, k -> new Object());
            long position;
            LogSegment segment;
            synchronized (topicLock) {
                segment = logManager.getOrCreateActiveSegment(topic);
                
                // Check if we need to roll over
                if (segment.getSize() >= config.getLogSegmentBytes()) {
                    segment = logManager.rollNewSegment(topic, offset);
                }
                
                position = segment.append(message);
            }

            indexMessage(topic, offset, position);

            addToCache(topic, message);
            topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();

            logger.debug("Persisted and indexed message: topic={}, offset={}, position={}, segment={}", 
                    topic, offset, position, segment.getFilePath().getFileName());

        } catch (IOException e) {
            logger.error("Failed to persist message for topic {}", topic, e);
            throw new RuntimeException("Failed to persist message", e);
        } finally {
            globalLock.readLock().unlock();
        }

        // 4. Wake any long-polling consumers waiting for new messages
        synchronized (messageMonitor) {
            messageSignal.incrementAndGet();
            messageMonitor.notifyAll();
        }

        return offset;
    }

    /**
     * Lock the store exclusively to safely take a snapshot of the log segments.
     */
    public void lockForSnapshot(Runnable task) {
        globalLock.writeLock().lock();
        try {
            task.run();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Get a message by topic and offset.
     */
    public StoredMessage getMessage(String topic, long offset) {
        BoundedMessageCache cache = messageCache.get(topic);
        if (cache != null) {
            StoredMessage msg = cache.get(offset);
            if (msg != null) return msg;
        }

        ConcurrentSkipListMap<Long, Long> index = topicIndex.get(topic);
        LogSegment segment = logManager.getSegmentForOffset(topic, offset);
        
        if (segment != null) {
            try {
                long startPosition = 0;
                if (index != null) {
                    java.util.Map.Entry<Long, Long> floorEntry = index.floorEntry(offset);
                    if (floorEntry != null && floorEntry.getKey() >= segment.getBaseOffset()) {
                        startPosition = floorEntry.getValue();
                    }
                }
                
                long segmentSize = segment.getSize();
                long position = startPosition;

                while (position < segmentSize) {
                    StoredMessage message = segment.read(position);
                    if (message.getOffset() == offset) {
                        return message;
                    }
                    if (message.getOffset() > offset) {
                        break;
                    }
                    position += 4 + message.getSerializedSize();
                }
            } catch (IOException e) {
                logger.error("Error reading message from disk: topic={}, offset={}", topic, offset, e);
            }
        }

        return null;
    }

    /**
     * Get messages from a topic starting at the given offset.
     * First tries the in-memory cache, then falls back to disk if needed.
     */
    public List<StoredMessage> getMessages(String topic, long fromOffset, int maxCount) {
        if (maxCount <= 0) {
            return Collections.emptyList();
        }
        
        BoundedMessageCache cache = messageCache.get(topic);
        if (cache != null) {
            List<StoredMessage> cachedMessages = cache.getMessagesFrom(fromOffset, maxCount);
            if (cachedMessages.size() >= maxCount) {
                return cachedMessages;
            }
        }
        
        ConcurrentSkipListMap<Long, Long> index = topicIndex.get(topic);
        
        List<StoredMessage> result = new ArrayList<>();
        long currentOffset = fromOffset;
        
        while (result.size() < maxCount) {
            LogSegment segment = logManager.getSegmentForOffset(topic, currentOffset);
            if (segment == null) {
                // Try to find the next available segment if there is a gap
                ConcurrentSkipListMap<Long, LogSegment> allTopicSegments = logManager.getAllSegments().get(topic);
                if (allTopicSegments != null) {
                    java.util.Map.Entry<Long, LogSegment> higherEntry = allTopicSegments.higherEntry(currentOffset);
                    if (higherEntry != null) {
                        segment = higherEntry.getValue();
                        currentOffset = segment.getBaseOffset();
                    } else {
                        break; // No more segments
                    }
                } else {
                    break;
                }
            }
            
            long startPosition = 0;
            if (index != null) {
                java.util.Map.Entry<Long, Long> floorEntry = index.floorEntry(currentOffset);
                if (floorEntry != null && floorEntry.getKey() >= segment.getBaseOffset()) {
                    startPosition = floorEntry.getValue();
                }
            }
            
            try {
                long segmentSize = segment.getSize();
                long position = startPosition;
                
                while (position < segmentSize && result.size() < maxCount) {
                    StoredMessage message = segment.read(position);
                    if (message.getOffset() >= currentOffset) {
                        result.add(message);
                        currentOffset = message.getOffset() + 1;
                    }
                    position += 4 + message.getSerializedSize();
                }
                
                if (position >= segmentSize) {
                    // Reached end of segment, will loop and fetch next segment
                    ConcurrentSkipListMap<Long, LogSegment> allTopicSegments = logManager.getAllSegments().get(topic);
                    if (allTopicSegments != null) {
                        java.util.Map.Entry<Long, LogSegment> higherEntry = allTopicSegments.higherEntry(segment.getBaseOffset());
                        if (higherEntry != null) {
                            currentOffset = higherEntry.getKey();
                        } else {
                            break; // No more segments
                        }
                    } else {
                        break;
                    }
                }
            } catch (IOException e) {
                logger.warn("Error reading messages from disk for topic {} segment {}: {}", topic, segment.getFilePath(), e.getMessage());
                break;
            }
        }
        
        return result;
    }

    /**
     * Wait for messages to arrive on a topic, blocking until messages are available
     * or the timeout expires. 
     * @param topic       the topic to wait for messages on
     * @param fromOffset  starting offset
     * @param maxCount    maximum messages to return
     * @param timeoutMs   maximum time to wait (0 = return immediately)
     * @return messages found
     */
    public List<StoredMessage> waitForMessages(String topic, long fromOffset, int maxCount, long timeoutMs) {
        if (maxCount <= 0) {
            return Collections.emptyList();
        }
        
        List<StoredMessage> messages = getMessages(topic, fromOffset, maxCount);
        if (!messages.isEmpty() || timeoutMs <= 0) {
            return messages;
        }

        long deadline = System.currentTimeMillis() + timeoutMs;
        long observedSignal = messageSignal.get();
        synchronized (messageMonitor) {
            while (messages.isEmpty()) {
                long currentSignal = messageSignal.get();
                if (currentSignal != observedSignal) {
                    observedSignal = currentSignal;
                    messages = getMessages(topic, fromOffset, maxCount);
                    continue;
                }

                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                try {
                    messageMonitor.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                messages = getMessages(topic, fromOffset, maxCount);
            }
        }
        return messages;
    }

    public long getCurrentOffset() {
        return globalOffset.get();
    }

    public int getMessageCount(String topic) {
        AtomicLong count = topicMessageCounts.get(topic);
        return count == null ? 0 : count.intValue();
    }

    public List<String> getTopics() {
        return new ArrayList<>(topicMessageCounts.keySet());
    }

    public int getTopicCount() {
        return topicMessageCounts.size();
    }

    public long getCachedMessageCount() {
        long total = 0;
        for (BoundedMessageCache cache : messageCache.values()) {
            total += cache.size();
        }
        return total;
    }

    public void clear() {
        topicIndex.clear();
        topicMessageCounts.clear();
        messageCache.clear();
        globalOffset.set(0);
        logger.info("Message store memory state cleared");
    }

    @Override
    public void close() throws IOException {
        if (cleanerScheduler != null) {
            cleanerScheduler.shutdown();
            try {
                if (!cleanerScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanerScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanerScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    void cleanupOldSegments() {
        long retentionMs = config.getLogRetentionMs();
        if (retentionMs <= 0) return;

        long cutoffTime = System.currentTimeMillis() - retentionMs;
        Map<String, ConcurrentSkipListMap<Long, LogSegment>> allSegments = logManager.getAllSegments();

        for (Map.Entry<String, ConcurrentSkipListMap<Long, LogSegment>> entry : allSegments.entrySet()) {
            String topic = entry.getKey();
            ConcurrentSkipListMap<Long, LogSegment> segments = entry.getValue();
            
            // Never delete the currently active (last) segment
            if (segments.size() <= 1) continue;
            
            Long activeBaseOffset = segments.lastKey();
            
            List<Long> toDelete = new ArrayList<>();
            for (Map.Entry<Long, LogSegment> segEntry : segments.entrySet()) {
                long baseOffset = segEntry.getKey();
                if (baseOffset == activeBaseOffset) continue; // Skip active
                
                LogSegment segment = segEntry.getValue();
                try {
                    if (segment.getLastModified() < cutoffTime) {
                        toDelete.add(baseOffset);
                    }
                } catch (IOException e) {
                    logger.warn("Failed to check last modified time for segment {}", segment.getFilePath(), e);
                }
            }
            
            for (Long baseOffset : toDelete) {
                LogSegment segment = segments.remove(baseOffset);
                if (segment != null) {
                    try {
                        segment.delete();
                        // Also cleanup the index
                        ConcurrentSkipListMap<Long, Long> index = topicIndex.get(topic);
                        if (index != null) {
                            // Find next segment's base offset to know how far to delete
                            Long nextBaseOffset = segments.higherKey(baseOffset);
                            long endOffset = (nextBaseOffset != null) ? nextBaseOffset : Long.MAX_VALUE;
                            index.subMap(baseOffset, endOffset).clear();
                        }
                    } catch (IOException e) {
                        logger.error("Failed to delete old log segment {}", segment.getFilePath(), e);
                        // Put it back so we try again later
                        segments.put(baseOffset, segment);
                    }
                }
            }
        }
    }


     // Bounded cache for messages with FIFO eviction.
        private static class BoundedMessageCache {
        private final int maxSize;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final LinkedHashMap<Long, StoredMessage> cache;

        public BoundedMessageCache(int maxSize) {
            this.maxSize = maxSize;
            this.cache = new LinkedHashMap<Long, StoredMessage>(maxSize, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, StoredMessage> eldest) {
                    return size() > maxSize;
                }
            };
        }

        public void add(StoredMessage message) {
            lock.writeLock().lock();
            try {
                long offset = message.getOffset();
                cache.put(offset, message);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public StoredMessage get(long offset) {
            lock.readLock().lock();
            try {
                return cache.get(offset);
            } finally {
                lock.readLock().unlock();
            }
        }

        public List<StoredMessage> getMessagesFrom(long fromOffset, int maxCount) {
            lock.readLock().lock();
            try {
                List<StoredMessage> result = new ArrayList<>();
                for (Map.Entry<Long, StoredMessage> entry : cache.entrySet()) {
                    if (entry.getKey() >= fromOffset) {
                        result.add(entry.getValue());
                        if (result.size() >= maxCount) {
                            break;
                        }
                    }
                }
                return result;
            } finally {
                lock.readLock().unlock();
            }
        }

        public int size() {
            lock.readLock().lock();
            try {
                return cache.size();
            } finally {
                lock.readLock().unlock();
            }
        }
    }

}

