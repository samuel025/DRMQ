package com.drmq.integration;

import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.drmq.protocol.StoredMessage;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Producer -> Broker communication.
 */
class ProducerBrokerIntegrationTest {

    @TempDir
    Path tempDir;

    private static final int TEST_PORT = 19092;
    private BrokerServer broker;

    @BeforeEach
    void setUp() throws Exception {
        broker = new BrokerServer(TEST_PORT, 5, tempDir.toString());
        broker.startAsync();
        
        // Wait for broker to be ready
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.shutdown();
        }
    }

    @Test
    void producerCanSendSingleMessage() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();

            var result = producer.send("test-topic", "Hello, DRMQ!".getBytes()).join();

            assertTrue(result.isSuccess());
            assertEquals(0, result.getOffset());
        }
    }

    @Test
    void producerCanSendMultipleMessages() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();

            for (int i = 0; i < 100; i++) {
                var result = producer.send("test-topic", ("Message " + i).getBytes()).join();
                assertTrue(result.isSuccess());
                assertEquals(i, result.getOffset());
            }
        }

        // Verify broker received all messages
        assertEquals(100, broker.getMessageStore().getMessageCount("test-topic"));
    }

    @Test
    void producerCanSendToMultipleTopics() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();

            producer.send("orders", "order-1".getBytes()).join();
            producer.send("payments", "payment-1".getBytes()).join();
            producer.send("orders", "order-2".getBytes()).join();
            producer.send("notifications", "notif-1".getBytes()).join();
        }

        assertEquals(2, broker.getMessageStore().getMessageCount("orders"));
        assertEquals(1, broker.getMessageStore().getMessageCount("payments"));
        assertEquals(1, broker.getMessageStore().getMessageCount("notifications"));
    }

    @Test
    void producerAutoConnectsOnFirstSend() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            // Don't call connect() explicitly
            assertFalse(producer.isConnected());

            var result = producer.send("test-topic", "auto-connect test".getBytes()).join();

            assertTrue(producer.isConnected());
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void producerCanSendWithKey() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();

            var result = producer.send("keyed-topic", "payload".getBytes(), "user-123").join();

            assertTrue(result.isSuccess());
            
            var stored = broker.getMessageStore().getMessage("keyed-topic", result.getOffset());
            assertEquals("user-123", stored.getKey());
        }
    }

    @Test
    void multipleProducersCanSendConcurrently() throws Exception {
        int producerCount = 5;
        int messagesPerProducer = 20;
        Thread[] threads = new Thread[producerCount];

        for (int p = 0; p < producerCount; p++) {
            final int producerId = p;
            threads[p] = new Thread(() -> {
                try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
                    producer.connect();
                    for (int m = 0; m < messagesPerProducer; m++) {
                        producer.send("concurrent-topic", 
                                ("Producer " + producerId + " - Message " + m).getBytes()).join();
                    }
                } catch (Exception e) {
                    fail("Producer " + producerId + " failed: " + e.getMessage());
                }
            });
            threads[p].start();
        }

        for (Thread t : threads) {
            t.join(10000);
        }

        // All messages should have been received
        assertEquals(producerCount * messagesPerProducer, 
                broker.getMessageStore().getMessageCount("concurrent-topic"));
        assertEquals(producerCount * messagesPerProducer, 
                broker.getMessageStore().getCurrentOffset());
    }

    @Test
    void producerStringMessageConvenience() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            var result = producer.send("string-topic", "This is a string message").join();

            assertTrue(result.isSuccess());
            
            var stored = broker.getMessageStore().getMessage("string-topic", result.getOffset());
            assertEquals("This is a string message", 
                    new String(stored.getPayload().toByteArray()));
        }
    }

    @Test
    void atomicSendToMultipleTopicsSucceeds() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();

            Map<String, byte[]> batch = new HashMap<>();
            batch.put("orders", "order-atomic-1".getBytes());
            batch.put("payments", "payment-atomic-1".getBytes());

            Map<String, Long> offsets = producer.sendAtomic(batch).join();

            assertNotNull(offsets);
            assertTrue(offsets.containsKey("orders"));
            assertTrue(offsets.containsKey("payments"));
            //
            var sortedOffsets = offsets.values().stream().sorted().toList();
            assertEquals(2, sortedOffsets.size());
            assertEquals(0L, sortedOffsets.get(0).longValue());
            assertEquals(1L, sortedOffsets.get(1).longValue());
        }

        assertEquals(1, broker.getMessageStore().getMessageCount("orders"));
        assertEquals(1, broker.getMessageStore().getMessageCount("payments"));
    }

    @Test
    void atomicSendOffsetsIncreaseByTotalMessagesPerBatch() throws Exception {
        int batches = 100;
        int topicsPerBatch = 3;
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();

            Map<String, byte[]> batch = new HashMap<>();
            batch.put("off-a", "a".getBytes());
            batch.put("off-b", "b".getBytes());
            batch.put("off-c", "c".getBytes());

            long prevBatchBase = -1;
            // Without client batching, each sendAtomic becomes its own request.
            // appendAtomicBatch sees 3 messages (1 per topic) → baseOffset += 3.
            for (int i = 0; i < batches; i++) {
                Map<String, Long> offsets = producer.sendAtomic(batch).join();
                assertEquals(topicsPerBatch, offsets.size());

                var sorted = offsets.values().stream().sorted().toList();
                assertEquals(1L, sorted.get(1) - sorted.get(0)); // contiguous within batch
                assertEquals(1L, sorted.get(2) - sorted.get(1));

                if (i > 0) {
                    // Each batch consumes topicsPerBatch global offsets
                    assertEquals(topicsPerBatch, sorted.get(0) - prevBatchBase);
                }
                prevBatchBase = sorted.get(0);
            }
        }
        assertEquals(batches, broker.getMessageStore().getMessageCount("off-a"));
        assertEquals(batches, broker.getMessageStore().getMessageCount("off-b"));
        assertEquals(batches, broker.getMessageStore().getMessageCount("off-c"));
    }

    @Test
    void atomicSendWithClientBatchingProducesContiguousOffsets() throws Exception {
        int numRequests = 50;
        int topicsPerBatch = 2;
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.setLingerMs(50);
            producer.connect();

            List<java.util.concurrent.CompletableFuture<Map<String, Long>>> futures = new ArrayList<>();

            for (int i = 0; i < numRequests; i++) {
                Map<String, byte[]> batch = new HashMap<>();
                batch.put("batched-a", ("a-" + i).getBytes());
                batch.put("batched-b", ("b-" + i).getBytes());
                futures.add(producer.sendAtomic(batch));
            }

            java.util.concurrent.CompletableFuture.allOf(
                    futures.toArray(new java.util.concurrent.CompletableFuture[0])
            ).get(5, java.util.concurrent.TimeUnit.SECONDS);

            // Client-side batching merges all 50 requests into one AtomicProduceRequest.
            // Topic slices are concatenated per-topic: all A entries then all B entries.
            // appendAtomicBatch assigns offsets [0..49] to A, [50..99] to B.
            // Each constituent i gets {A: i, B: numRequests + i}.
            // The union of ALL offsets spans 0..99 with no gaps.
            java.util.Set<Long> allOffsets = new java.util.HashSet<>();
            for (int i = 0; i < numRequests; i++) {
                Map<String, Long> offsets = futures.get(i).get();
                assertEquals(topicsPerBatch, offsets.size());
                allOffsets.addAll(offsets.values());
            }
            assertEquals(numRequests * topicsPerBatch, allOffsets.size());
            for (long o = 0; o < numRequests * topicsPerBatch; o++) {
                assertTrue(allOffsets.contains(o), "Missing offset " + o);
            }
        }
        assertEquals(numRequests, broker.getMessageStore().getMessageCount("batched-a"));
        assertEquals(numRequests, broker.getMessageStore().getMessageCount("batched-b"));
    }

    @Test
    void atomicSendRequiresAtLeastTwoTopics() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();

            Map<String, byte[]> single = new HashMap<>();
            single.put("only-topic", "data".getBytes());

            assertThrows(Exception.class, () -> producer.sendAtomic(single).join());
        }
    }
}
