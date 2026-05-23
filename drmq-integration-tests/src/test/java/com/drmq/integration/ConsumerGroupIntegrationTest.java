package com.drmq.integration;

import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQConsumer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for multi-consumer group support.
 *
 * Validates that multiple consumers in the same group receive
 * disjoint subsets of messages (load-balanced), while consumers
 * in different groups each receive all messages (fan-out).
 */
class ConsumerGroupIntegrationTest {

    @TempDir
    Path tempDir;

    private static final int TEST_PORT = 19094;
    private BrokerServer broker;

    @BeforeEach
    void setUp() throws Exception {
        broker = new BrokerServer(TEST_PORT, 5, tempDir.toString());
        broker.startAsync();
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.shutdown();
        }
    }

    @Test
    void twoConsumersSameGroupSplitMessages() throws Exception {
        final String topic = "split-test";
        final String group = "split-group";
        final int messageCount = 20;

        // Produce messages
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            for (int i = 0; i < messageCount; i++) {
                producer.send(topic, ("msg-" + i).getBytes());
            }
        }

        // Two consumers in the same group poll concurrently
        Set<Long> consumer1Offsets = ConcurrentHashMap.newKeySet();
        Set<Long> consumer2Offsets = ConcurrentHashMap.newKeySet();

        try (DRMQConsumer c1 = new DRMQConsumer("localhost", TEST_PORT, group);
             DRMQConsumer c2 = new DRMQConsumer("localhost", TEST_PORT, group)) {

            c1.setAutoCommit(true);
            c2.setAutoCommit(true);
            c1.connect();
            c2.connect();
            c1.subscribe(topic);
            c2.subscribe(topic);

            // Poll several times to drain all messages
            for (int round = 0; round < 10; round++) {
                var msgs1 = c1.poll(10, 200);
                for (var m : msgs1) consumer1Offsets.add(m.offset());

                var msgs2 = c2.poll(10, 200);
                for (var m : msgs2) consumer2Offsets.add(m.offset());

                if (consumer1Offsets.size() + consumer2Offsets.size() >= messageCount) break;
            }
        }

        // Verify disjoint: no offset appears in both consumers
        Set<Long> overlap = new HashSet<>(consumer1Offsets);
        overlap.retainAll(consumer2Offsets);
        assertTrue(overlap.isEmpty(),
                "Consumers in the same group should NOT receive overlapping offsets. Overlap: " + overlap);

        // Verify completeness: together they received all messages
        Set<Long> allOffsets = new HashSet<>(consumer1Offsets);
        allOffsets.addAll(consumer2Offsets);
        assertEquals(messageCount, allOffsets.size(),
                "Together, both consumers should have received all " + messageCount +
                " messages. Got: " + allOffsets.size());

        // Verify both consumers did some work (load was distributed)
        assertFalse(consumer1Offsets.isEmpty(), "Consumer 1 should have received some messages");
        assertFalse(consumer2Offsets.isEmpty(), "Consumer 2 should have received some messages");
    }

    @Test
    void differentGroupsReceiveAllMessages() throws Exception {
        final String topic = "fanout-test";
        final int messageCount = 5;

        // Produce messages
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            for (int i = 0; i < messageCount; i++) {
                producer.send(topic, ("fanout-" + i).getBytes());
            }
        }

        // Group A consumer
        List<DRMQConsumer.ConsumedMessage> groupAMessages;
        try (DRMQConsumer cA = new DRMQConsumer("localhost", TEST_PORT, "group-A")) {
            cA.connect();
            cA.subscribe(topic);
            groupAMessages = cA.poll(messageCount, 1000);
        }

        // Group B consumer
        List<DRMQConsumer.ConsumedMessage> groupBMessages;
        try (DRMQConsumer cB = new DRMQConsumer("localhost", TEST_PORT, "group-B")) {
            cB.connect();
            cB.subscribe(topic);
            groupBMessages = cB.poll(messageCount, 1000);
        }

        // Both groups should receive ALL messages
        assertEquals(messageCount, groupAMessages.size(),
                "Group A should receive all messages");
        assertEquals(messageCount, groupBMessages.size(),
                "Group B should receive all messages");

        // Same offsets
        Set<Long> aOffsets = groupAMessages.stream().map(DRMQConsumer.ConsumedMessage::offset).collect(Collectors.toSet());
        Set<Long> bOffsets = groupBMessages.stream().map(DRMQConsumer.ConsumedMessage::offset).collect(Collectors.toSet());
        assertEquals(aOffsets, bOffsets, "Both groups should see the same set of offsets");
    }

    @Test
    void singleConsumerInGroupGetsAllMessages() throws Exception {
        final String topic = "solo-group-test";
        final int messageCount = 10;

        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            for (int i = 0; i < messageCount; i++) {
                producer.send(topic, ("solo-" + i).getBytes());
            }
        }

        try (DRMQConsumer consumer = new DRMQConsumer("localhost", TEST_PORT, "solo-group")) {
            consumer.setAutoCommit(true);
            consumer.connect();
            consumer.subscribe(topic);

            List<DRMQConsumer.ConsumedMessage> all = new ArrayList<>();
            for (int round = 0; round < 5; round++) {
                var msgs = consumer.poll(messageCount, 200);
                all.addAll(msgs);
                if (all.size() >= messageCount) break;
            }

            assertEquals(messageCount, all.size(), "Single consumer in group should receive all messages");
        }
    }

    @Test
    void groupModeAutoCommitPreventsRedelivery() throws Exception {
        final String topic = "commit-test";
        final String group = "commit-group";

        // Produce messages
        try (DRMQProducer producer = new DRMQProducer("localhost", TEST_PORT)) {
            producer.connect();
            producer.send(topic, "A".getBytes());
            producer.send(topic, "B".getBytes());
        }

        // First consumer reads and auto-commits
        try (DRMQConsumer c1 = new DRMQConsumer("localhost", TEST_PORT, group)) {
            c1.setAutoCommit(true);
            c1.connect();
            c1.subscribe(topic);
            var msgs = c1.poll(10, 500);
            assertEquals(2, msgs.size());
        }

        // Second consumer in the same group should NOT re-read
        try (DRMQConsumer c2 = new DRMQConsumer("localhost", TEST_PORT, group)) {
            c2.setAutoCommit(true);
            c2.connect();
            c2.subscribe(topic);
            var msgs = c2.poll(10, 500);
            assertEquals(0, msgs.size(), "After commit, same group should not re-read messages");
        }
    }

    @Test
    void consumerIdIsUniquePerInstance() throws Exception {
        try (DRMQConsumer c1 = new DRMQConsumer("localhost", TEST_PORT, "any-group");
             DRMQConsumer c2 = new DRMQConsumer("localhost", TEST_PORT, "any-group")) {

            assertNotNull(c1.getConsumerId());
            assertNotNull(c2.getConsumerId());
            assertNotEquals(c1.getConsumerId(), c2.getConsumerId(),
                    "Each consumer instance should have a unique consumer ID");
        }
    }
}
