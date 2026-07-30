package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.protocol.DRMQProtocol.AtomicBatchTopicSlice;
import com.drmq.protocol.DRMQProtocol.ProduceBatchRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MessageStore.
 */
class MessageStoreTest {

    @TempDir
    Path tempDir;

    private LogManager logManager;
    private MessageStore store;

    @BeforeEach
    void setUp() throws IOException {
        logManager = new LogManager(tempDir.toString());
        store = new MessageStore(logManager, new BrokerConfig(9092, tempDir.toString()));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logManager != null) {
            logManager.close();
        }
    }

    @Test
    void appendReturnsMonotonicallyIncreasingOffsets() {
        long offset1 = store.append("test-topic", "message1".getBytes(), null, System.currentTimeMillis());
        long offset2 = store.append("test-topic", "message2".getBytes(), null, System.currentTimeMillis());
        long offset3 = store.append("test-topic", "message3".getBytes(), null, System.currentTimeMillis());

        assertEquals(0, offset1);
        assertEquals(1, offset2);
        assertEquals(2, offset3);
    }

    @Test
    void offsetsAreGlobalAcrossTopics() {
        long offset1 = store.append("topic-a", "msg1".getBytes(), null, System.currentTimeMillis());
        long offset2 = store.append("topic-b", "msg2".getBytes(), null, System.currentTimeMillis());
        long offset3 = store.append("topic-a", "msg3".getBytes(), null, System.currentTimeMillis());

        assertEquals(0, offset1);
        assertEquals(1, offset2);
        assertEquals(2, offset3);
    }

    @Test
    void getMessageReturnsStoredMessage() {
        byte[] payload = "hello world".getBytes();
        long offset = store.append("test", payload, "key1", 12345L);

        var message = store.getMessage("test", offset);

        assertNotNull(message);
        assertEquals(offset, message.getOffset());
        assertEquals("test", message.getTopic());
        assertArrayEquals(payload, message.getPayload().toByteArray());
        assertEquals("key1", message.getKey());
        assertEquals(12345L, message.getTimestamp());
    }

    @Test
    void getMessageReturnsNullForNonexistentOffset() {
        store.append("test", "msg".getBytes(), null, System.currentTimeMillis());

        assertNull(store.getMessage("test", 999));
        assertNull(store.getMessage("nonexistent", 0));
    }

    @Test
    void getMessagesReturnsRangeFromOffset() {
        for (int i = 0; i < 10; i++) {
            store.append("test", ("msg" + i).getBytes(), null, System.currentTimeMillis());
        }

        var messages = store.getMessages("test", 5, 3);

        assertEquals(3, messages.size());
        assertEquals(5, messages.get(0).getOffset());
        assertEquals(6, messages.get(1).getOffset());
        assertEquals(7, messages.get(2).getOffset());
    }

    @Test
    void waitForMessagesReturnsAfterAppend() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = executor.submit(() -> store.waitForMessages("test", 0, 1, 1000));

            Thread.sleep(50);
            store.append("test", "msg".getBytes(), null, System.currentTimeMillis());

            @SuppressWarnings("unchecked")
            var messages = (java.util.List<com.drmq.protocol.DRMQProtocol.StoredMessage>) future.get(2, TimeUnit.SECONDS);

            assertEquals(1, messages.size());
            assertEquals(0, messages.get(0).getOffset());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentAppendsProduceUniqueOffsets() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger totalMessages = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadNum = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < messagesPerThread; i++) {
                        store.append("topic-" + threadNum, 
                                ("message-" + i).getBytes(), null, System.currentTimeMillis());
                        totalMessages.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify all messages were stored with unique offsets
        assertEquals(threadCount * messagesPerThread, store.getCurrentOffset());
        assertEquals(threadCount * messagesPerThread, totalMessages.get());
    }

    @Test
    void recoverRebuildsIndexFromDisk() throws IOException {
        String topic = "persistence-test";
        store.append(topic, "msg1".getBytes(), null, 1000L);
        store.append(topic, "msg2".getBytes(), "key2", 2000L);
        
        long lastOffset = store.getCurrentOffset();
        
        // Close and recreate store
        logManager.close();
        
        try (LogManager newLogManager = new LogManager(tempDir.toString())) {
            MessageStore newStore = new MessageStore(newLogManager, new BrokerConfig(9092, tempDir.toString()));
            
            newStore.recover();
            
            assertEquals(lastOffset, newStore.getCurrentOffset());
            assertEquals(2, newStore.getMessageCount(topic));
            
            var msg1 = newStore.getMessage(topic, 0);
            assertNotNull(msg1);
            assertEquals("msg1", new String(msg1.getPayload().toByteArray()));
            
            var msg2 = newStore.getMessage(topic, 1);
            assertNotNull(msg2);
            assertEquals("msg2", new String(msg2.getPayload().toByteArray()));
            assertEquals("key2", msg2.getKey());
        }
    }

    @Test
    void clearResetsStore() {
        store.append("test", "msg".getBytes(), null, System.currentTimeMillis());
        assertEquals(1, store.getCurrentOffset());
        assertEquals(1, store.getMessageCount("test"));

        store.clear();

        assertEquals(0, store.getCurrentOffset());
        assertEquals(0, store.getMessageCount("test"));
    }

    @Test
    void getTopicsReturnsAllTopicNames() {
        store.append("alpha", "msg".getBytes(), null, System.currentTimeMillis());
        store.append("beta", "msg".getBytes(), null, System.currentTimeMillis());
        store.append("gamma", "msg".getBytes(), null, System.currentTimeMillis());

        var topics = store.getTopics();

        assertEquals(3, topics.size());
        assertTrue(topics.contains("alpha"));
        assertTrue(topics.contains("beta"));
        assertTrue(topics.contains("gamma"));
    }
    @Test
    void testLogRolling() throws Exception {
        BrokerConfig config = new BrokerConfig(9092, tempDir.toString());
        config.setLogSegmentBytes(100); 
        
        try (LogManager lm = new LogManager(config)) {
            MessageStore testStore = new MessageStore(lm, config);
            
            testStore.append("roll-topic", new byte[40], null, System.currentTimeMillis());
            testStore.append("roll-topic", new byte[40], null, System.currentTimeMillis());
            testStore.append("roll-topic", new byte[40], null, System.currentTimeMillis());
            
            var segments = lm.getAllSegments().get("roll-topic");
            assertTrue(segments.size() > 1, "Log should have rolled into multiple segments");
        }
    }

    @Test
    void testRetentionPolicy() throws Exception {
        BrokerConfig config = new BrokerConfig(9092, tempDir.toString());
        config.setLogSegmentBytes(100);
        config.setLogRetentionMs(500); 
        
        try (LogManager lm = new LogManager(config)) {
            MessageStore testStore = new MessageStore(lm, config);
            
            testStore.append("retention-topic", new byte[40], null, System.currentTimeMillis());
            testStore.append("retention-topic", new byte[40], null, System.currentTimeMillis());
            testStore.append("retention-topic", new byte[40], null, System.currentTimeMillis());
            
            var segmentsBefore = lm.getAllSegments().get("retention-topic");
            int numSegmentsBefore = segmentsBefore.size();
            assertTrue(numSegmentsBefore > 1, "Log should have rolled");
            
            Thread.sleep(1000); 
            
            testStore.append("retention-topic", new byte[10], null, System.currentTimeMillis());
            
            testStore.cleanupOldSegments();
            
            var segmentsAfter = lm.getAllSegments().get("retention-topic");
            assertTrue(segmentsAfter.size() < numSegmentsBefore, "Old segments should be deleted");
            assertEquals(1, segmentsAfter.size(), "Only the active segment should remain");
        }
    }

    @Test
    void appendAtomicBatchStoresMessagesAcrossTopics() {
        AtomicBatchTopicSlice slice1 = AtomicBatchTopicSlice.newBuilder()
                .setTopic("topic-a")
                .addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("msgA1"))
                        .setClientTimestamp(100L).build())
                .addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("msgA2"))
                        .setClientTimestamp(200L).build())
                .build();

        AtomicBatchTopicSlice slice2 = AtomicBatchTopicSlice.newBuilder()
                .setTopic("topic-b")
                .addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("msgB1"))
                        .setClientTimestamp(300L).build())
                .build();

        Map<String, Long> baseOffsets = store.appendAtomicBatch(Arrays.asList(slice1, slice2));

        assertEquals(2, baseOffsets.size());
        assertEquals(0L, baseOffsets.get("topic-a"));
        assertEquals(2L, baseOffsets.get("topic-b"));

        assertEquals(3L, store.getCurrentOffset());
        assertEquals(2L, store.getMessageCount("topic-a"));
        assertEquals(1L, store.getMessageCount("topic-b"));

        var msgA1 = store.getMessage("topic-a", 0);
        assertEquals("msgA1", msgA1.getPayload().toStringUtf8());
        
        var msgA2 = store.getMessage("topic-a", 1);
        assertEquals("msgA2", msgA2.getPayload().toStringUtf8());

        var msgB1 = store.getMessage("topic-b", 2);
        assertEquals("msgB1", msgB1.getPayload().toStringUtf8());
    }

    @Test
    void appendAtomicBatchWithEmptyListReturnsEmptyMap() {
        Map<String, Long> offsets = store.appendAtomicBatch(Collections.emptyList());
        assertTrue(offsets.isEmpty());
        assertEquals(0, store.getCurrentOffset());
    }
}

