package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.persistence.LogSegment;
import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message storage for the broker.
 */
public class MessageStore {
    private static final Logger logger = LoggerFactory.getLogger(MessageStore.class);

    private final AtomicLong globalOffset = new AtomicLong(0);
    private final LogManager logManager;

    // Topic -> Offset -> Byte Position in log file
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> topicIndex = new ConcurrentHashMap<>();
    
    // In-memory cache for recent messages (Topic -> BoundedMessageCache)
    private final ConcurrentHashMap<String, BoundedMessageCache> messageCache = new ConcurrentHashMap<>();
    
    // Maximum number of messages to keep in memory per topic
    private static final int MAX_CACHE_SIZE_PER_TOPIC = 1000;

    // Monitor for long-poll notification — consumers wait() on this, append() calls notifyAll()
    private final Object messageMonitor = new Object();

    public MessageStore(LogManager logManager) {
        this.logManager = logManager;
    }

    /**
     * Recovery: Rebuild the index from log files on disk.
     */
    public void recover() throws IOException {
        logger.info("Starting message store recovery...");
        Map<String, Path> segments = logManager.discoverSegments();
        long maxOffset = -1;

        for (Map.Entry<String, Path> entry : segments.entrySet()) {
            String topic = entry.getKey();
            Path logPath = entry.getValue();
            
            try (LogSegment segment = new LogSegment(logPath)) {
                long position = 0;
                long segmentSize = segment.getSize();
                while (position < segmentSize) {
                    StoredMessage message = segment.read(position);
                    long offset = message.getOffset();
                    
                    indexMessage(topic, offset, position);
                    addToCache(topic, message);
                    
                    if (offset > maxOffset) {
                        maxOffset = offset;
                    }
                    
                    // Move to next message: 4 bytes (length) + message size
                    position += 4 + message.getSerializedSize();
                }
            } catch (IOException ioe) {
                logger.error("Error recovering topic {}: {}", topic, ioe.getMessage(), ioe);
                throw ioe;
            }
        }

        globalOffset.set(maxOffset + 1);
        logger.info("Recovery complete. Global offset set to {}", globalOffset.get());
    }

    private void indexMessage(String topic, long offset, long position) {
        topicIndex.computeIfAbsent(topic, k -> new ConcurrentSkipListMap<>())
                .put(offset, position);
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

        try {
            // 1. Persist to WAL
            LogSegment segment = logManager.getOrCreateSegment(topic);
            long position = segment.append(message);

            // 2. Update Index
            indexMessage(topic, offset, position);

            // 3. Update Cache
            addToCache(topic, message);

            logger.debug("Persisted and indexed message: topic={}, offset={}, position={}", 
                    topic, offset, position);

        } catch (IOException e) {
            logger.error("Failed to persist message for topic {}: {}", topic, e.getMessage());
            throw new RuntimeException("Persistence failure", e);
        }

        // 4. Wake any long-polling consumers waiting for new messages
        synchronized (messageMonitor) {
            messageMonitor.notifyAll();
        }

        return offset;
    }

    /**
     * Get a message by topic and offset.
     */
    public StoredMessage getMessage(String topic, long offset) {
        // First try cache
        BoundedMessageCache cache = messageCache.get(topic);
        if (cache != null) {
            StoredMessage msg = cache.get(offset);
            if (msg != null) return msg;
        }

        // Then try index + disk
        Map<Long, Long> index = topicIndex.get(topic);
        if (index != null && index.containsKey(offset)) {
            try {
                long position = index.get(offset);
                LogSegment segment = logManager.getOrCreateSegment(topic);
                return segment.read(position);
            } catch (IOException e) {
                logger.error("Error reading message from disk: topic={}, offset={}", topic, offset, e);
            }
        }

        return null;
    }

    /**
     * Get messages from a topic starting at the given offset.
     * First tries the in-memory cache, then falls back to disk if needed.
     * 
     * Note: Offsets are global across all topics, so a topic's offsets may be sparse.
     * We iterate actual index entries rather than probing consecutive offsets.
     */
    public List<StoredMessage> getMessages(String topic, long fromOffset, int maxCount) {
        BoundedMessageCache cache = messageCache.get(topic);
        
        // Try cache first
        if (cache != null) {
            List<StoredMessage> cachedMessages = cache.getMessagesFrom(fromOffset, maxCount);
            // If we got enough messages from cache, return them
            if (cachedMessages.size() >= maxCount) {
                return cachedMessages;
            }
        }
        
        // Cache miss or partial hit - need to read from disk
        ConcurrentSkipListMap<Long, Long> index = topicIndex.get(topic);
        if (index == null) {
            return Collections.emptyList();
        }
        
        List<StoredMessage> result = new ArrayList<>();
        
        // Get segment once outside the loop for efficiency
        LogSegment segment;
        try {
            segment = logManager.getOrCreateSegment(topic);
        } catch (IOException e) {
            logger.error("Failed to get segment for topic {}: {}", topic, e.getMessage());
            return Collections.emptyList();
        }
        
        // Iterate actual index entries (sparse offsets) starting from fromOffset
        // tailMap(fromOffset) returns all entries with key >= fromOffset, already sorted
        for (Map.Entry<Long, Long> entry : index.tailMap(fromOffset).entrySet()) {
            if (result.size() >= maxCount) {
                break;  // Got enough messages
            }
            
            long offset = entry.getKey();
            long position = entry.getValue();
            
            try {
                StoredMessage message = segment.read(position);
                result.add(message);
            } catch (IOException e) {
                logger.warn("Error reading message at offset {} from disk: {}", offset, e.getMessage());
                // Continue to next message
            }
        }
        
        return result;
    }

    /**
     * Wait for messages to arrive on a topic, blocking until messages are available
     * or the timeout expires. Uses Object.wait()/notifyAll() for efficient blocking
     * instead of polling with Thread.sleep().
     *
     * This solves the long-poll thread exhaustion problem: waiting threads release
     * their CPU time slice and are woken immediately when a message is appended.
     *
     * @param topic       the topic to wait for messages on
     * @param fromOffset  starting offset (inclusive)
     * @param maxCount    maximum messages to return
     * @param timeoutMs   maximum time to wait (0 = return immediately)
     * @return messages found (may be empty if timeout expires)
     */
    public List<StoredMessage> waitForMessages(String topic, long fromOffset, int maxCount, long timeoutMs) {
        // Fast path: check if messages are already available
        List<StoredMessage> messages = getMessages(topic, fromOffset, maxCount);
        if (!messages.isEmpty() || timeoutMs <= 0) {
            return messages;
        }

        // Slow path: wait for notification from append()
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (messageMonitor) {
            while (messages.isEmpty()) {
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
        Map<Long, Long> index = topicIndex.get(topic);
        return index == null ? 0 : index.size();
    }

    public List<String> getTopics() {
        return new ArrayList<>(topicIndex.keySet());
    }

    public void clear() {
        topicIndex.clear();
        messageCache.clear();
        globalOffset.set(0);
        // Note: This doesn't delete files from disk for safety, but clears memory view.
        logger.info("Message store memory state cleared");
    }

    /**
     * Bounded cache for messages with LRU eviction.
     * Thread-safe implementation using ArrayDeque with fixed capacity.
     */
    private static class BoundedMessageCache {
        private final int maxSize;
        private final Map<Long, StoredMessage> messageMap = new ConcurrentHashMap<>();
        private final java.util.Deque<Long> offsetQueue = new java.util.ArrayDeque<>();

        public BoundedMessageCache(int maxSize) {
            this.maxSize = maxSize;
        }

        public synchronized void add(StoredMessage message) {
            long offset = message.getOffset();
            
            // If already exists, update it (shouldn't happen in normal operation)
            if (messageMap.containsKey(offset)) {
                return;
            }
            
            // Evict oldest if at capacity
            if (offsetQueue.size() >= maxSize) {
                Long oldestOffset = offsetQueue.pollFirst();
                if (oldestOffset != null) {
                    messageMap.remove(oldestOffset);
                }
            }
            
            // Add new message
            messageMap.put(offset, message);
            offsetQueue.addLast(offset);
        }

        public synchronized StoredMessage get(long offset) {
            return messageMap.get(offset);
        }

        public synchronized List<StoredMessage> getMessagesFrom(long fromOffset, int maxCount) {
            List<StoredMessage> result = new ArrayList<>();
            for (Long offset : offsetQueue) {
                if (offset >= fromOffset) {
                    StoredMessage msg = messageMap.get(offset);
                    if (msg != null) {
                        result.add(msg);
                        if (result.size() >= maxCount) {
                            break;
                        }
                    }
                }
            }
            return result;
        }
    }
}

