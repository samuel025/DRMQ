package com.drmq.integration;

import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQConsumer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Consumer functionality.
 */
class ConsumerIntegrationTest {

    @TempDir
    Path tempDir;

    private static final int TEST_PORT = 19093;
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
    void consumerCanReadProducedMessages() throws Exception {
        // Produce some messages
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            producer.send("test-topic", "Message 1".getBytes());
            producer.send("test-topic", "Message 2".getBytes());
            producer.send("test-topic", "Message 3".getBytes());
        }

        // Consume the messages
        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT)) {
            consumer.connect();
            consumer.subscribe("test-topic");

            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(10);

            assertEquals(3, messages.size());
            assertEquals("Message 1", messages.get(0).payloadAsString());
            assertEquals("Message 2", messages.get(1).payloadAsString());
            assertEquals("Message 3", messages.get(2).payloadAsString());
            assertEquals(0, messages.get(0).offset());
            assertEquals(1, messages.get(1).offset());
            assertEquals(2, messages.get(2).offset());
        }
    }

    @Test
    void consumerCanReadFromSpecificOffset() throws Exception {
        // Produce messages
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            for (int i = 0; i < 10; i++) {
                producer.send("offset-test", ("Message " + i).getBytes());
            }
        }

        // Consume starting from offset 5
        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT)) {
            consumer.connect();
            consumer.subscribe("offset-test", 5);

            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(20);

            assertEquals(5, messages.size());
            assertEquals("Message 5", messages.get(0).payloadAsString());
            assertEquals("Message 9", messages.get(4).payloadAsString());
            assertEquals(5, messages.get(0).offset());
            assertEquals(9, messages.get(4).offset());
        }
    }

    @Test
    void multipleConsumersCanReadSameTopic() throws Exception {
        // Produce messages
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            producer.send("shared-topic", "Shared Message 1".getBytes());
            producer.send("shared-topic", "Shared Message 2".getBytes());
        }

        // Consumer 1 (group-a) reads all messages
        try (DRMQConsumer consumer1 = new DRMQConsumer("localhost", TEST_PORT, "group-a")) {
            consumer1.connect();
            consumer1.subscribe("shared-topic");
            List<DRMQConsumer.ConsumedMessage> messages1 = consumer1.poll();
            assertEquals(2, messages1.size());
        }

        // Consumer 2 (group-b) also reads all messages independently
        // (different group = independent offset tracking)
        try (DRMQConsumer consumer2 = new DRMQConsumer("localhost", TEST_PORT, "group-b")) {
            consumer2.connect();
            consumer2.subscribe("shared-topic");
            List<DRMQConsumer.ConsumedMessage> messages2 = consumer2.poll();
            assertEquals(2, messages2.size());
        }
    }

    @Test
    void sameGroupDoesNotReReadMessages() throws Exception {
        // Produce messages
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            producer.send("group-topic", "Message 1".getBytes());
            producer.send("group-topic", "Message 2".getBytes());
        }

        // Consumer reads and commits offset
        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT, "my-service")) {
            consumer.connect();
            consumer.subscribe("group-topic");
            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll();
            assertEquals(2, messages.size());
        } // offset 2 committed to broker on close

        // Same group restarted — should NOT re-read already consumed messages
        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT, "my-service")) {
            consumer.connect();
            consumer.subscribe("group-topic"); // broker returns offset 2
            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll();
            assertEquals(0, messages.size(), "Same group should not re-read committed messages");
        }
    }

    @Test
    void consumerReturnsEmptyListForEmptyTopic() throws Exception {
        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT)) {
            consumer.connect();
            consumer.subscribe("empty-topic");

            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll();

            assertTrue(messages.isEmpty());
        }
    }

    @Test
    void consumerCanPollMultipleTimes() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            producer.send("multi-poll", "First".getBytes());
        }

        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT)) {
            consumer.connect();
            consumer.subscribe("multi-poll");

            // First poll gets the message
            List<DRMQConsumer.ConsumedMessage> messages1 = consumer.poll();
            assertEquals(1, messages1.size());
            assertEquals("First", messages1.get(0).payloadAsString());

            // Produce another message
            try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
                producer.connect();
                producer.send("multi-poll", "Second".getBytes());
            }

            // Second poll gets the new message
            List<DRMQConsumer.ConsumedMessage> messages2 = consumer.poll();
            assertEquals(1, messages2.size());
            assertEquals("Second", messages2.get(0).payloadAsString());
        }
    }

    @Test
    void consumerTracksOffsetCorrectly() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            producer.send("offset-tracking", "A".getBytes());
            producer.send("offset-tracking", "B".getBytes());
            producer.send("offset-tracking", "C".getBytes());
        }

        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT)) {
            consumer.connect();
            consumer.subscribe("offset-tracking");

            assertEquals(0, consumer.getCurrentOffset("offset-tracking"));

            consumer.poll(1); // Read first message
            assertEquals(1, consumer.getCurrentOffset("offset-tracking"));

            consumer.poll(1); // Read second message
            assertEquals(2, consumer.getCurrentOffset("offset-tracking"));

            consumer.poll(10); // Read remaining
            assertEquals(3, consumer.getCurrentOffset("offset-tracking"));
        }
    }

    @Test
    void consumerHandlesMessageWithKey() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            producer.send("keyed-topic", "Value".getBytes(), "my-key");
        }

        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT)) {
            consumer.connect();
            consumer.subscribe("keyed-topic");

            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll();

            assertEquals(1, messages.size());
            assertEquals("my-key", messages.get(0).key());
            assertEquals("Value", messages.get(0).payloadAsString());
        }
    }
}
