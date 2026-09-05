package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.persistence.LogSegment;
import com.drmq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.net.URI;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.drmq.broker.persistence.CorruptRecordException;
import com.drmq.broker.persistence.InfinityLogResolver;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.regions.Region;

/**
 * Message storage for the broker.
 */
public class MessageStore implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MessageStore.class);

    private final AtomicLong globalOffset = new AtomicLong(0);
    private final AtomicLong lastAppliedRaftIndex = new AtomicLong(-1);
    private final LogManager logManager;
    private final BrokerConfig config;
    private final ScheduledExecutorService cleanerScheduler = Executors.newSingleThreadScheduledExecutor();
    private S3Client s3Client;
    private InfinityLogResolver infinityLogResolver;

    // Topic -> Offset -> Byte Position in log file (Sparse Index)
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> topicIndex = new ConcurrentHashMap<>();
    
    // Topic -> Total number of messages
    private final ConcurrentHashMap<String, AtomicLong> topicMessageCounts = new ConcurrentHashMap<>();

    // Topic -> Highest offset appended
    private final ConcurrentHashMap<String, AtomicLong> topicHeadOffsets = new ConcurrentHashMap<>();
    
    // In-memory cache for recent messages (Topic -> BoundedMessageCache)
    private final ConcurrentHashMap<String, BoundedMessageCache> messageCache = new ConcurrentHashMap<>();
    
    // Per-topic locks for append synchronization
    private final ConcurrentHashMap<String, java.util.concurrent.locks.ReentrantLock> topicLocks = new ConcurrentHashMap<>();
    
    private static final int MAX_CACHE_SIZE_PER_TOPIC = 1000;
    private static final int INDEX_INTERVAL = 1000;
    private static final int MAX_INDEX_ENTRIES = 10000;


    private final Object messageMonitor = new Object();
    private final AtomicLong messageSignal = new AtomicLong(0);

    public MessageStore(LogManager logManager, BrokerConfig config) {
        this.logManager = logManager;
        this.config = config;
        
        if (config.getS3ArchiveBucket() != null && !config.getS3ArchiveBucket().isEmpty()) {
            S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(config.getS3ArchiveRegion()));
            if (config.getS3ArchiveEndpoint() != null && !config.getS3ArchiveEndpoint().isEmpty()) {
                builder.endpointOverride(URI.create(config.getS3ArchiveEndpoint()))
                       .forcePathStyle(true);
            }
            this.s3Client = builder.build();
            this.infinityLogResolver = new InfinityLogResolver(this.s3Client, config, logManager);
            logger.info("S3 Archiving enabled for bucket: {} (Endpoint: {})", 
                config.getS3ArchiveBucket(), 
                config.getS3ArchiveEndpoint() != null ? config.getS3ArchiveEndpoint() : "default");
        }

        long retentionMs = config.getLogRetentionMs();
        if (retentionMs > 0) {
            cleanerScheduler.scheduleWithFixedDelay(this::cleanupOldSegments, 5, 10, TimeUnit.SECONDS);
        }
    }

    public boolean isFsyncEnabled() {
        return config != null && config.isLogSegmentFsync();
    }

    /**
     * Recovery: Rebuild the index from log files on disk.
     */
    public void recover() throws IOException {
        recoverInternal();
    }

    private void recoverInternal() throws IOException {
        logger.info("Starting message store recovery...");
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
                        
                        AtomicLong head = topicHeadOffsets.computeIfAbsent(topic, k -> new AtomicLong(-1));
                        if (offset > head.get()) {
                            head.set(offset);
                        }
                        
                        if (offset > maxOffset) {
                            maxOffset = offset;
                        }
                        
                        if (message.getRaftIndex() > lastAppliedRaftIndex.get()) {
                            lastAppliedRaftIndex.set(message.getRaftIndex());
                        }
                        
                        position += 4 + message.getSerializedSize();
                    }
                } catch (CorruptRecordException cre) {
                    logger.warn("Corruption detected in topic {} segment {} at position {}. Truncating: {}", topic, segment.getFilePath(), position, cre.getMessage());
                    try {
                        segment.truncate(position);
                    } catch (IOException truncateEx) {
                        logger.error("Failed to truncate segment {}", segment.getFilePath(), truncateEx);
                        throw truncateEx;
                    }
                }
            }
        }

        Path intentFile = Paths.get(config.getDataDir()).resolve(".atomic-intent");
        if (java.nio.file.Files.exists(intentFile)) {
            logger.info("Found .atomic-intent file. Recovering partial atomic batch...");
            try (DataInputStream dis = new DataInputStream(new FileInputStream(intentFile.toFile()))) {
                int numTopics = dis.readInt();
                for (int i = 0; i < numTopics; i++) {
                    String topic = dis.readUTF();
                    int numMsgs = dis.readInt();
                    List<StoredMessage> msgs = new ArrayList<>(numMsgs);
                    for (int j = 0; j < numMsgs; j++) {
                        int len = dis.readInt();
                        byte[] msgBytes = new byte[len];
                        dis.readFully(msgBytes);
                        msgs.add(StoredMessage.parseFrom(msgBytes));
                    }

                    long currentHeadOffset = topicHeadOffsets.containsKey(topic) ? topicHeadOffsets.get(topic).get() : -1;
                    List<StoredMessage> missingMsgs = new ArrayList<>();
                    for (StoredMessage m : msgs) {
                        if (m.getOffset() > currentHeadOffset) {
                            missingMsgs.add(m);
                        }
                    }

                    if (!missingMsgs.isEmpty()) {
                        logger.info("Recovering {} missing messages for topic {}", missingMsgs.size(), topic);
                        LogSegment segment = logManager.getOrCreateActiveSegment(topic);
                        if (segment.getSize() >= config.getLogSegmentBytes()) {
                            segment = logManager.rollNewSegment(topic, missingMsgs.get(0).getOffset());
                        }
                        List<Long> positions = segment.appendBatch(missingMsgs);
                        
                        AtomicLong counter = topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong());
                        AtomicLong head = topicHeadOffsets.computeIfAbsent(topic, k -> new AtomicLong(-1));
                        
                        for (int j = 0; j < missingMsgs.size(); j++) {
                            StoredMessage m = missingMsgs.get(j);
                            indexMessage(topic, m.getOffset(), positions.get(j));
                            addToCache(topic, m);
                            counter.incrementAndGet();
                            if (m.getOffset() > head.get()) {
                                head.set(m.getOffset());
                            }
                            if (m.getOffset() > maxOffset) {
                                maxOffset = m.getOffset();
                            }
                            if (m.getRaftIndex() > lastAppliedRaftIndex.get()) {
                                lastAppliedRaftIndex.set(m.getRaftIndex());
                            }
                        }
                    }
                }
  
                try {
                  Files.deleteIfExists(intentFile);
                } catch (IOException e) {
                    logger.warn("Failed to delete atomic intent file after successful recovery", e);
                }
            } catch (Exception e) {
                logger.error("FATAL: Failed to recover atomic intent file. Panicking to prevent dataloss!", e);
                if (System.getProperty("drmq.test.mode") != null) {
                    throw new RuntimeException("Simulated panic during atomic intent recovery", e);
                } else {
                    System.exit(1);
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
        lockForSnapshot(() -> {
            logger.info("Reloading MessageStore state from disk...");
            topicIndex.clear();
            topicMessageCounts.clear();
            topicHeadOffsets.clear();
            messageCache.clear();
            topicLocks.clear();
            
            try {
                logManager.close();
                recoverInternal();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void reconcileWithManifest(Map<String, Long> fileManifest) throws IOException {
        lockForSnapshot(() -> {
            try {
                logManager.reconcileWithManifest(fileManifest);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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

    public long getLastAppliedRaftIndex() {
        return lastAppliedRaftIndex.get();
    }
    
    public long getNextOffset() {
        return globalOffset.get();
    }
    
    public long reserveOffsets(int count) {
        return globalOffset.getAndAdd(count);
    }

    public void updateGlobalOffset(long target) {
        long current;
        while ((current = globalOffset.get()) < target) {
            globalOffset.compareAndSet(current, target);
        }
    }

    public long append(String topic, byte[] payload, String key, long clientTimestamp, long raftIndex) {
        return append(topic, com.google.protobuf.ByteString.copyFrom(payload), key, clientTimestamp, raftIndex, -1L);
    }

    public long append(String topic, com.google.protobuf.ByteString payload, String key, long clientTimestamp, long raftIndex) {
        return append(topic, payload, key, clientTimestamp, raftIndex, -1L);
    }

    /**
     * Append a message to the specified topic.
     */
    public long append(String topic, com.google.protobuf.ByteString payload, String key, long clientTimestamp, long raftIndex, long predefinedBaseOffset) {
        long offset = predefinedBaseOffset >= 0 ? predefinedBaseOffset : globalOffset.getAndIncrement();
        if (predefinedBaseOffset >= 0) {
            updateGlobalOffset(predefinedBaseOffset + 1);
        }
        long storedAt = System.currentTimeMillis();

        StoredMessage.Builder builder = StoredMessage.newBuilder()
                .setOffset(offset)
                .setTopic(topic)
                .setPayload(payload)
                .setTimestamp(clientTimestamp)
                .setStoredAt(storedAt)
                .setRaftIndex(raftIndex);

        if (key != null && !key.isEmpty()) {
            builder.setKey(key);
        }

        StoredMessage message = builder.build();

        java.util.concurrent.locks.ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new java.util.concurrent.locks.ReentrantLock());
        lock.lock();
        try {
            long position;
            LogSegment segment = logManager.getOrCreateActiveSegment(topic);
            if (segment.getSize() >= config.getLogSegmentBytes()) {
                segment = logManager.rollNewSegment(topic, offset);
            }
            
            position = segment.append(message);

            indexMessage(topic, offset, position);

            addToCache(topic, message);
            topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
            
            AtomicLong head = topicHeadOffsets.computeIfAbsent(topic, k -> new AtomicLong(-1));
            if (offset > head.get()) {
                head.set(offset);
            }

            logger.debug("Persisted and indexed message: topic={}, offset={}, position={}, segment={}", 
                    topic, offset, position, segment.getFilePath().getFileName());

        } catch (IOException e) {
            logger.error("Failed to persist message for topic {}", topic, e);
            throw new RuntimeException("Failed to persist message", e);
        } finally {
            lock.unlock();
        }
        synchronized (messageMonitor) {
            messageSignal.incrementAndGet();
            messageMonitor.notifyAll();
        }
        
        if (raftIndex > lastAppliedRaftIndex.get()) {
            lastAppliedRaftIndex.set(raftIndex);
        }

        return offset;
    }

    /**
     * Append a batch of messages to the specified topic with True Group Commit.
     * Reserves a contiguous block of offsets atomically, writes all messages in one
     * disk operation with a single fsync, then updates RAM indexes and caches.
     *
     * @param topic     The target topic
     * @param entries   The batch entries (payload, key, timestamp)
     * @return The base offset (offset of the first message in the batch)
     */
    public long appendBatch(String topic, List<ProduceBatchRequest.BatchEntry> entries, long raftIndex) {
        return appendBatch(topic, entries, raftIndex, -1L);
    }

    public long appendBatch(String topic, List<ProduceBatchRequest.BatchEntry> entries, long raftIndex, long predefinedBaseOffset) {
        int batchSize = entries.size();
        if (batchSize == 0) {
            throw new IllegalArgumentException("Batch must contain at least one message");
        }

        long baseOffset = predefinedBaseOffset >= 0 ? predefinedBaseOffset : globalOffset.getAndAdd(batchSize);
        if (predefinedBaseOffset >= 0) {
            updateGlobalOffset(predefinedBaseOffset + batchSize);
        }
        long storedAt = System.currentTimeMillis();

        List<StoredMessage> messages = new ArrayList<>(batchSize);
        long currentOffset = baseOffset;
        for (var entry : entries) {
            StoredMessage.Builder builder = StoredMessage.newBuilder()
                    .setOffset(currentOffset++)
                    .setTopic(topic)
                    .setPayload(entry.getPayload())
                    .setTimestamp(entry.getClientTimestamp())
                    .setStoredAt(storedAt)
                    .setRaftIndex(raftIndex);

            if (entry.hasKey()) {
                builder.setKey(entry.getKey());
            }

            messages.add(builder.build());
        }

        ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
        lock.lock();
        try {
            List<Long> positions;
            LogSegment segment = logManager.getOrCreateActiveSegment(topic);

            if (segment.getSize() >= config.getLogSegmentBytes()) {
                segment = logManager.rollNewSegment(topic, baseOffset);
            }

            positions = segment.appendBatch(messages);

            AtomicLong counter = topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong());
            AtomicLong head = topicHeadOffsets.computeIfAbsent(topic, k -> new AtomicLong(-1));
            for (int i = 0; i < messages.size(); i++) {
                StoredMessage msg = messages.get(i);
                indexMessage(topic, msg.getOffset(), positions.get(i));
                addToCache(topic, msg);
                counter.incrementAndGet();
                if (msg.getOffset() > head.get()) {
                    head.set(msg.getOffset());
                }
            }

            logger.debug("Batch persisted: topic={}, baseOffset={}, count={}", topic, baseOffset, batchSize);

        } catch (IOException e) {
            logger.error("Failed to persist batch for topic {}", topic, e);
            throw new RuntimeException("Failed to persist batch", e);
        } finally {
            lock.unlock();
        }

        synchronized (messageMonitor) {
            messageSignal.incrementAndGet();
            messageMonitor.notifyAll();
        }
        
        if (raftIndex > lastAppliedRaftIndex.get()) {
            lastAppliedRaftIndex.set(raftIndex);
        }

        return baseOffset;
    }

    /**
     * Atomically appends messages to multiple topics in a single operation.
     * Either all topic writes succeed, or none are visible.
     * Returns a map of topic -> base offset for each slice.
     */
    public Map<String, Long> appendAtomicBatch(List<AtomicBatchTopicSlice> slices, long raftIndex) {
        return appendAtomicBatch(slices, raftIndex, -1L);
    }

    public Map<String, Long> appendAtomicBatch(List<AtomicBatchTopicSlice> slices, long raftIndex, long predefinedBaseOffset) {
        if (slices.isEmpty()) {
            return Collections.emptyMap();
        }

        int totalMessages = slices.stream().mapToInt(AtomicBatchTopicSlice::getEntriesCount).sum();
        if (totalMessages == 0) {
            return Collections.emptyMap();
        }

        long baseOffset = predefinedBaseOffset >= 0 ? predefinedBaseOffset : globalOffset.getAndAdd(totalMessages);
        if (predefinedBaseOffset >= 0) {
            updateGlobalOffset(predefinedBaseOffset + totalMessages);
        }
        long storedAt = System.currentTimeMillis();

        Map<String, Long> topicBaseOffsets = new LinkedHashMap<>();
        Map<String, List<StoredMessage>> topicMessages = new LinkedHashMap<>();

        long currentOffset = baseOffset;
        for (var slice : slices) {
            String topic = slice.getTopic();
            long topicBase = currentOffset;
            topicBaseOffsets.putIfAbsent(topic, topicBase);

            List<StoredMessage> messages = topicMessages.computeIfAbsent(topic, k -> new ArrayList<>());
            for (var entry : slice.getEntriesList()) {
                StoredMessage.Builder builder = StoredMessage.newBuilder()
                        .setOffset(currentOffset++)
                        .setTopic(topic)
                        .setPayload(entry.getPayload())
                        .setTimestamp(entry.getClientTimestamp())
                        .setStoredAt(storedAt)
                        .setRaftIndex(raftIndex);

                if (entry.hasKey()) {
                    builder.setKey(entry.getKey());
                }
                messages.add(builder.build());
            }
        }

        Path intentFile = Paths.get(config.getDataDir()).resolve(".atomic-intent");
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(intentFile.toFile());
             java.io.DataOutputStream dos = new java.io.DataOutputStream(fos)) {
            dos.writeInt(topicMessages.size());
            for (var entry : topicMessages.entrySet()) {
                dos.writeUTF(entry.getKey());
                List<StoredMessage> msgs = entry.getValue();
                dos.writeInt(msgs.size());
                for (StoredMessage msg : msgs) {
                    byte[] msgBytes = msg.toByteArray();
                    dos.writeInt(msgBytes.length);
                    dos.write(msgBytes);
                }
            }
            dos.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            logger.error("Failed to write atomic intent file", e);
            throw new RuntimeException("Failed to write atomic intent file", e);
        }
        List<String> sortedTopics = new ArrayList<>(topicMessages.keySet());
        Collections.sort(sortedTopics);
        List<java.util.concurrent.locks.ReentrantLock> acquiredLocks = new ArrayList<>();
        for (String t : sortedTopics) {
            java.util.concurrent.locks.ReentrantLock tLock = topicLocks.computeIfAbsent(t, k -> new java.util.concurrent.locks.ReentrantLock());
            tLock.lock();
            acquiredLocks.add(tLock);
        }

        try {
            for (var entry : topicMessages.entrySet()) {
                String topic = entry.getKey();
                List<StoredMessage> messages = entry.getValue();
                
                List<Long> positions;
                LogSegment segment = logManager.getOrCreateActiveSegment(topic);
                if (segment.getSize() >= config.getLogSegmentBytes()) {
                    segment = logManager.rollNewSegment(topic, topicBaseOffsets.get(topic));
                }
                positions = segment.appendBatch(messages);

                AtomicLong counter = topicMessageCounts.computeIfAbsent(topic, k -> new AtomicLong());
                AtomicLong head = topicHeadOffsets.computeIfAbsent(topic, k -> new AtomicLong(-1));
                for (int i = 0; i < messages.size(); i++) {
                    StoredMessage msg = messages.get(i);
                    indexMessage(topic, msg.getOffset(), positions.get(i));
                    addToCache(topic, msg);
                    counter.incrementAndGet();
                    if (msg.getOffset() > head.get()) {
                        head.set(msg.getOffset());
                    }
                }
                logger.debug("Atomic batch slice persisted: topic={}, baseOffset={}, count={}", topic, topicBaseOffsets.get(topic), messages.size());
            }
        } catch (IOException e) {
            logger.error("Failed to persist atomic batch", e);
            throw new RuntimeException("Failed to persist atomic batch", e);
        } finally {
            for (int i = acquiredLocks.size() - 1; i >= 0; i--) {
                acquiredLocks.get(i).unlock();
            }
        }
        try {
            Files.deleteIfExists(intentFile);
        } catch (IOException e) {
            logger.warn("Failed to delete atomic intent file", e);
        }

        synchronized (messageMonitor) {
            messageSignal.incrementAndGet();
            messageMonitor.notifyAll();
        }
        
        if (raftIndex > lastAppliedRaftIndex.get()) {
            lastAppliedRaftIndex.set(raftIndex);
        }

        return topicBaseOffsets;
    }

    /**
     * Lock the store exclusively to safely take a snapshot of the log segments.
     */
    public void lockForSnapshot(Runnable task) {
        List<String> sortedTopics = new ArrayList<>(topicLocks.keySet());
        Collections.sort(sortedTopics);
        
        List<java.util.concurrent.locks.ReentrantLock> acquired = new ArrayList<>();
        for (String t : sortedTopics) {
            java.util.concurrent.locks.ReentrantLock lock = topicLocks.get(t);
            if (lock != null) {
                lock.lock();
                acquired.add(lock);
            }
        }
        
        try {
            task.run();
        } finally {
            for (int i = acquired.size() - 1; i >= 0; i--) {
                acquired.get(i).unlock();
            }
        }
    }

    /**
     * Get the first offset for a topic whose timestamp is >= targetTimestamp.
     */
    public long findOffsetByTimestamp(String topic, long targetTimestamp) {
        try {
            return logManager.findOffsetByTimestamp(topic, targetTimestamp);
        } catch (IOException e) {
            logger.error("Error searching for timestamp {} in topic {}", targetTimestamp, topic, e);
            return -1;
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
        
        List<StoredMessage> cachedMessages = Collections.emptyList();
        BoundedMessageCache cache = messageCache.get(topic);
        if (cache != null) {
            cachedMessages = cache.getMessagesFrom(fromOffset, maxCount);
            if (cachedMessages.size() >= maxCount) {
                return cachedMessages;
            }
        }
        
        ConcurrentSkipListMap<Long, Long> index = topicIndex.get(topic);
        
        List<StoredMessage> diskResult = new ArrayList<>();
        long currentOffset = fromOffset;
        LogSegment lastSegment = null;
        
        while (diskResult.size() < maxCount) {
            LogSegment segment = logManager.getSegmentForOffset(topic, currentOffset);
            
            if (segment != null && segment == lastSegment) {
                segment = null;
            }
            
            if (segment == null) {
                if (infinityLogResolver != null) {
                    segment = infinityLogResolver.resolveMissingSegment(topic, currentOffset);
                }
                
                if (segment == null) {
                    ConcurrentSkipListMap<Long, LogSegment> allTopicSegments = logManager.getAllSegments().get(topic);
                    if (allTopicSegments != null) {
                        java.util.Map.Entry<Long, LogSegment> higherEntry = allTopicSegments.higherEntry(currentOffset);
                        if (higherEntry != null) {
                            segment = higherEntry.getValue();
                            currentOffset = segment.getBaseOffset();
                        } else {
                            break; 
                        }
                    } else {
                        break;
                    }
                }
            }
            
            long startPosition = 0;
            if (index != null) {
                Map.Entry<Long, Long> floorEntry = index.floorEntry(currentOffset);
                if (floorEntry != null && floorEntry.getKey() >= segment.getBaseOffset()) {
                    startPosition = floorEntry.getValue();
                }
            }
            
            try {
                long segmentSize = segment.getSize();
                long position = startPosition;
                
                while (position < segmentSize && diskResult.size() < maxCount) {
                    StoredMessage message = segment.read(position);
                    if (message.getOffset() >= currentOffset) {
                        diskResult.add(message);
                        currentOffset = message.getOffset() + 1;
                    }
                    position += 4 + message.getSerializedSize();
                }
                
                lastSegment = segment;
            } catch (IOException e) {
                logger.warn("Error reading messages from disk for topic {} segment {}: {}", topic, segment.getFilePath(), e.getMessage());
                break;
            }
        }
        
        if (cachedMessages.isEmpty()) {
            return diskResult;
        }
        
        Map<Long, StoredMessage> merged = new java.util.TreeMap<>();
        for (StoredMessage msg : diskResult) {
            merged.put(msg.getOffset(), msg);
        }
        for (StoredMessage msg : cachedMessages) {
            merged.put(msg.getOffset(), msg);
        }
        
        List<StoredMessage> result = new ArrayList<>(merged.values());
        if (result.size() > maxCount) {
            return result.subList(0, maxCount);
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

    public long getHeadOffset(String topic) {
        AtomicLong head = topicHeadOffsets.get(topic);
        return head == null ? -1 : head.get();
    }

    public List<String> getTopics() {
        return new ArrayList<>(topicMessageCounts.keySet());
    }

    public int getTopicCount() {
        return topicMessageCounts.size();
    }

    /**
     * Get the highest offset for each topic.
     * Used for Tier 2 Incremental Sync to report follower state to the leader.
     * @return Map of topic name to its max offset
     */
    public Map<String, Long> getTopicMaxOffsets() {
        Map<String, Long> maxOffsets = new java.util.HashMap<>();
        for (Map.Entry<String, AtomicLong> entry : topicHeadOffsets.entrySet()) {
            maxOffsets.put(entry.getKey(), entry.getValue().get());
        }
        return maxOffsets;
    }

    /**
     * Get paths of segments that contain offsets greater than the provided offset.
     * Used for Tier 2 Incremental Sync.
     */
    public List<Path> getSegmentsForSync(String topic, long followerOffset) {
        ConcurrentSkipListMap<Long, LogSegment> segments = logManager.getAllSegments().get(topic);
        if (segments == null) return Collections.emptyList();

        List<Path> paths = new ArrayList<>();
        Map.Entry<Long, LogSegment> floorEntry = segments.floorEntry(followerOffset);
        if (floorEntry != null) {
            paths.add(floorEntry.getValue().getFilePath());
            for (LogSegment seg : segments.tailMap(floorEntry.getKey(), false).values()) {
                paths.add(seg.getFilePath());
            }
        } else {
            // followerOffset is before the oldest segment we have, so send all
            for (LogSegment seg : segments.values()) {
                paths.add(seg.getFilePath());
            }
        }
        return paths;
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
        topicHeadOffsets.clear();
        messageCache.clear();
        globalOffset.set(0);
        logger.info("Message store memory state cleared");
    }

    public void forceFlush() throws IOException {
        logManager.forceFlush();
    }

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
        if (s3Client != null) {
            s3Client.close();
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
                        if (s3Client != null) {
                            String key = "archive/shared/" + topic + "/" + segment.getFilePath().getFileName().toString();
                            logger.info("Uploading segment {} to S3 bucket {}", segment.getFilePath(), config.getS3ArchiveBucket());
                            s3Client.putObject(
                                PutObjectRequest.builder()
                                    .bucket(config.getS3ArchiveBucket())
                                    .key(key)
                                    .build(),
                                segment.getFilePath()
                            );
                            logger.info("Successfully uploaded segment to S3: {}", key);
                        }
                        
                        segment.delete();
                        ConcurrentSkipListMap<Long, Long> index = topicIndex.get(topic);
                        if (index != null) {
                            Long nextBaseOffset = segments.higherKey(baseOffset);
                            long endOffset = (nextBaseOffset != null) ? nextBaseOffset : Long.MAX_VALUE;
                            index.subMap(baseOffset, endOffset).clear();
                            
                            BoundedMessageCache cache = messageCache.get(topic);
                            if (cache != null) {
                                cache.removeRange(baseOffset, endOffset);
                            }
                        }
                    } catch (IOException e) {
                        logger.error("Failed to delete old log segment {}", segment.getFilePath(), e);
                        segments.put(baseOffset, segment);
                    }
                }
            }
        }
    }
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

        public void removeRange(long fromOffset, long toOffset) {
            lock.writeLock().lock();
            try {
                cache.keySet().removeIf(k -> k >= fromOffset && k < toOffset);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public List<StoredMessage> getMessagesFrom(long fromOffset, int maxCount) {
            lock.readLock().lock();
            try {
                if (!cache.containsKey(fromOffset)) {
                    return Collections.emptyList();
                }
                List<StoredMessage> result = new ArrayList<>();
                boolean found = false;
                for (Map.Entry<Long, StoredMessage> entry : cache.entrySet()) {
                    if (entry.getKey() == fromOffset) {
                        found = true;
                    }
                    if (found) {
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
