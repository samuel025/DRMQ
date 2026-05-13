package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
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
        store = new MessageStore(logManager);
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
            MessageStore newStore = new MessageStore(newLogManager);
            
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
}

