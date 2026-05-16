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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Message storage for the broker.
 */
public class MessageStore {
    private static final Logger logger = LoggerFactory.getLogger(MessageStore.class);

    private final AtomicLong globalOffset = new AtomicLong(0);
    private final LogManager logManager;

    // Topic -> Offset -> Byte Position in log file (Sparse Index)
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> topicIndex = new ConcurrentHashMap<>();
    
    // Topic -> Total number of messages
    private final ConcurrentHashMap<String, AtomicLong> topicMessageCounts = new ConcurrentHashMap<>();
    
    // In-memory cache for recent messages (Topic -> BoundedMessageCache)
    private final ConcurrentHashMap<String, BoundedMessageCache> messageCache = new ConcurrentHashMap<>();
    
    private static final int MAX_CACHE_SIZE_PER_TOPIC = 1000;
    private static final int INDEX_INTERVAL = 1000;
    private static final int MAX_INDEX_ENTRIES = 10000;

    private final Object messageMonitor = new Object();
    private final AtomicLong messageSignal = new AtomicLong(0);

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
                    topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
                    
                    if (offset > maxOffset) {
                        maxOffset = offset;
                    }
                    
                   
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

        try {
            LogSegment segment = logManager.getOrCreateSegment(topic);
            long position = segment.append(message);

            indexMessage(topic, offset, position);

            addToCache(topic, message);
            topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();

            logger.debug("Persisted and indexed message: topic={}, offset={}, position={}", 
                    topic, offset, position);

        } catch (IOException e) {
            logger.error("Failed to persist message for topic {}: {}", topic, e.getMessage());
            throw new RuntimeException("Persistence failure", e);
        }

        // 4. Wake any long-polling consumers waiting for new messages
        synchronized (messageMonitor) {
            messageSignal.incrementAndGet();
            messageMonitor.notifyAll();
        }

        return offset;
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
        if (index != null) {
            try {
                Map.Entry<Long, Long> floorEntry = index.floorEntry(offset);
                long startPosition = floorEntry != null ? floorEntry.getValue() : 0;
                
                LogSegment segment = logManager.getOrCreateSegment(topic);
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
        long startPosition = 0;
        if (index != null) {
            Map.Entry<Long, Long> floorEntry = index.floorEntry(fromOffset);
            if (floorEntry != null) {
                startPosition = floorEntry.getValue();
            }
        }
        
        List<StoredMessage> result = new ArrayList<>();
        try {
            LogSegment segment = logManager.getOrCreateSegment(topic);
            long segmentSize = segment.getSize();
            long position = startPosition;
            
            while (position < segmentSize && result.size() < maxCount) {
                StoredMessage message = segment.read(position);
                if (message.getOffset() >= fromOffset) {
                    result.add(message);
                }
                position += 4 + message.getSerializedSize();
            }
        } catch (IOException e) {
            logger.warn("Error reading messages from disk for topic {}: {}", topic, e.getMessage());
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

