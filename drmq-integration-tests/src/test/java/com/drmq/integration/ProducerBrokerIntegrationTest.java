package com.drmq.integration;

import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

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
}
