package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Dead-Letter Queue (DLQ) feature:
 *  - Delivery counting on lease expiry
 *  - Explicit NACK with redelivery and DLQ routing
 *  - DLQ topic naming and message content preservation
 *  - Offset advancement after DLQ routing
 *  - Configuration (maxDeliveries, dlqTopicPrefix)
 */
class DeadLetterQueueTest {

    @TempDir
    Path tempDir;

    private LogManager logManager;
    private MessageStore messageStore;
    private OffsetManager offsetManager;
    private ConsumerGroupCoordinator coordinator;

    private static final String GROUP = "dlq-group";
    private static final String TOPIC = "dlq-topic";

    /** Default max deliveries for most tests. */
    private static final int MAX_DELIVERIES = 3;

    @BeforeEach
    void setUp() throws IOException {
        logManager = new LogManager(tempDir.toString());
        BrokerConfig config = new BrokerConfig(9092, tempDir.toString());
        messageStore = new MessageStore(logManager, config);
        offsetManager = new OffsetManager(tempDir.toString());
        // Short lease timeout (50ms), maxDeliveries=3, default prefix "dlq."
        coordinator = new ConsumerGroupCoordinator(
                messageStore, offsetManager, null, 50, MAX_DELIVERIES, "dlq.");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (coordinator != null) coordinator.close();
        if (offsetManager != null) offsetManager.close();
        if (logManager != null) logManager.close();
    }

    private void produceMessages(int count) {
        for (int i = 0; i < count; i++) {
            messageStore.append(TOPIC, ("msg-" + i).getBytes(), null, System.currentTimeMillis());
        }
    }

    private void produceMessages(String topic, int count) {
        for (int i = 0; i < count; i++) {
            messageStore.append(topic, ("msg-" + i).getBytes(), null, System.currentTimeMillis());
        }
    }

    private List<StoredMessage> getDlqMessagesWithWait(String dlqTopic, int expectedSize) {
        for (int i = 0; i < 50; i++) {
            List<StoredMessage> msgs = messageStore.getMessages(dlqTopic, 0, 10);
            if (msgs.size() >= expectedSize) {
                return msgs;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return messageStore.getMessages(dlqTopic, 0, 10);
    }

    // =========================================================================
    // 1. Lease expiry delivery counting — redelivery before threshold
    // =========================================================================

    @Test
    void leaseExpiryIncrementsDeliveryCountAndRedelivers() throws Exception {
        produceMessages(1);

        // First delivery attempt
        List<StoredMessage> batch1 = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        assertEquals(1, batch1.size());
        assertEquals(0, batch1.get(0).getOffset());

        // Let lease expire (attempt 1)
        Thread.sleep(200);
        coordinator.expireLeases();

        // Message should be redelivered (not yet at threshold of 3)
        List<StoredMessage> batch2 = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        assertEquals(1, batch2.size());
        assertEquals(0, batch2.get(0).getOffset(), "Should redeliver offset 0 after first expiry");
    }

    // =========================================================================
    // 2. Lease expiry routes to DLQ after max deliveries
    // =========================================================================

    @Test
    void leaseExpiryRoutesToDlqAfterMaxDeliveries() throws Exception {
        produceMessages(1);

        String dlqTopic = coordinator.getDlqTopicName(GROUP, TOPIC);

        // Exhaust all delivery attempts via lease expiry
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            assertEquals(1, batch.size(), "Attempt " + (i + 1) + " should get the message");

            Thread.sleep(200);
            coordinator.expireLeases();
        }

        // The message should now be in the DLQ topic
        List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 1);
        assertEquals(1, dlqMessages.size(), "Exactly one message should be in the DLQ");

        // Verify the DLQ message payload matches the original
        assertEquals("msg-0", new String(dlqMessages.get(0).getPayload().toByteArray()));

        // The consumer group should have advanced past the poison message
        List<StoredMessage> nextBatch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        assertTrue(nextBatch.isEmpty(), "No more messages should be available after DLQ routing");
    }

    // =========================================================================
    // 3. Explicit NACK — redelivery before threshold
    // =========================================================================

    @Test
    void nackRequeuesMessageBeforeMaxDeliveries() {
        produceMessages(1);

        // Acquire the message
        List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        assertEquals(1, batch.size());

        // NACK it (attempt 1 of 3) — should requeue, not route to DLQ
        boolean routedToDlq = coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);
        assertFalse(routedToDlq, "First NACK should requeue, not route to DLQ");

        // Message should be available for redelivery
        List<StoredMessage> redelivered = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 1, 0);
        assertEquals(1, redelivered.size());
        assertEquals(0, redelivered.get(0).getOffset());
    }

    // =========================================================================
    // 4. Explicit NACK — routes to DLQ after max deliveries
    // =========================================================================

    @Test
    void nackRoutesToDlqAfterMaxDeliveries() {
        produceMessages(1);

        String dlqTopic = coordinator.getDlqTopicName(GROUP, TOPIC);

        // Exhaust all delivery attempts via NACK
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            assertEquals(1, batch.size(), "Attempt " + (i + 1) + " should get the message");

            boolean routedToDlq = coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);

            if (i < MAX_DELIVERIES - 1) {
                assertFalse(routedToDlq, "Should not route to DLQ on attempt " + (i + 1));
            } else {
                assertTrue(routedToDlq, "Should route to DLQ on final attempt");
            }
        }

        // Verify DLQ topic has the message
        List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 1);
        assertEquals(1, dlqMessages.size());
        assertEquals("msg-0", new String(dlqMessages.get(0).getPayload().toByteArray()));
    }

    // =========================================================================
    // 5. DLQ preserves message key and payload
    // =========================================================================

    @Test
    void dlqPreservesMessageKeyAndPayload() {
        // Produce a message WITH a key
        messageStore.append(TOPIC, "important-payload".getBytes(), "routing-key-42", System.currentTimeMillis());

        String dlqTopic = coordinator.getDlqTopicName(GROUP, TOPIC);

        // NACK it maxDeliveries times to trigger DLQ
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);
        }

        List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 1);
        assertEquals(1, dlqMessages.size());

        StoredMessage dlqMsg = dlqMessages.get(0);
        assertEquals("important-payload", new String(dlqMsg.getPayload().toByteArray()));
        assertEquals("routing-key-42", dlqMsg.getKey());
    }

    // =========================================================================
    // 6. Offset advances past DLQ'd message — subsequent messages are delivered
    // =========================================================================

    @Test
    void offsetAdvancesPastDlqMessageAndDeliversSubsequent() {
        produceMessages(3); // offsets 0, 1, 2

        // NACK offset 0 to exhaustion
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);
        }

        // Consumer should now get offset 1 (offset 0 was DLQ'd and skipped)
        List<StoredMessage> next = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 10, 0);
        assertFalse(next.isEmpty(), "Should get remaining messages after DLQ routing");
        assertEquals(1, next.get(0).getOffset(), "First message should be offset 1 (0 was DLQ'd)");
        assertEquals(2, next.size(), "Should get offsets 1 and 2");
    }

    // =========================================================================
    // 7. Mixed NACK and lease expiry share the delivery counter
    // =========================================================================

    @Test
    void nackAndLeaseExpiryShareDeliveryCounter() throws Exception {
        produceMessages(1);

        String dlqTopic = coordinator.getDlqTopicName(GROUP, TOPIC);

        // Attempt 1: NACK
        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        boolean r1 = coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);
        assertFalse(r1);

        // Attempt 2: lease expiry
        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        Thread.sleep(200);
        coordinator.expireLeases();

        // Attempt 3 (= MAX_DELIVERIES): NACK should trigger DLQ
        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        boolean r3 = coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);
        assertTrue(r3, "Third attempt should route to DLQ (mixed NACK + expiry)");

        // Verify the message landed in the DLQ
        List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 1);
        assertEquals(1, dlqMessages.size());
    }

    // =========================================================================
    // 8. NACK on unknown group returns false, doesn't crash
    // =========================================================================

    @Test
    void nackOnUnknownGroupReturnsFalse() {
        boolean result = coordinator.nackOffset("nonexistent-group", TOPIC, "consumer-A", 0);
        assertFalse(result, "NACK on unknown group should return false gracefully");
    }

    // =========================================================================
    // 9. DLQ topic naming convention
    // =========================================================================

    @Test
    void dlqTopicNameFollowsConvention() {
        assertEquals("dlq.my-group.my-topic",
                coordinator.getDlqTopicName("my-group", "my-topic"));
    }

    @Test
    void customDlqTopicPrefix() throws IOException {
        // Create coordinator with custom prefix
        ConsumerGroupCoordinator custom = new ConsumerGroupCoordinator(
                messageStore, offsetManager, null, 50, 3, "dead.");

        assertEquals("dead.my-group.my-topic",
                custom.getDlqTopicName("my-group", "my-topic"));

        custom.close();
    }

    // =========================================================================
    // 10. Committed message is NOT counted as a failed delivery
    // =========================================================================

    @Test
    void successfulCommitDoesNotIncrementDeliveryCount() {
        produceMessages(2);

        String dlqTopic = coordinator.getDlqTopicName(GROUP, TOPIC);

        // Acquire and successfully commit offset 0
        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        coordinator.commitOffset(GROUP, TOPIC, "consumer-A", 1);

        // Acquire offset 1, NACK it maxDeliveries times
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 1);
        }

        // Only offset 1 should be in the DLQ, not offset 0
        List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 1);
        assertEquals(1, dlqMessages.size(), "Only the NACKed message should be in DLQ");
        assertEquals("msg-1", new String(dlqMessages.get(0).getPayload().toByteArray()));
    }

    // =========================================================================
    // 11. Multiple poison messages are handled independently
    // =========================================================================

    @Test
    void multiplePoisonMessagesRoutedToDlqIndependently() {
        produceMessages(5); // offsets 0-4

        String dlqTopic = coordinator.getDlqTopicName(GROUP, TOPIC);

        // DLQ offset 0
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);
        }

        // Successfully consume offset 1
        List<StoredMessage> batch1 = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        assertEquals(1, batch1.get(0).getOffset());
        coordinator.commitOffset(GROUP, TOPIC, "consumer-A", 2);

        // DLQ offset 2
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 2);
        }

        // Two messages should be in the DLQ
        List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 2);
        assertEquals(2, dlqMessages.size(), "Two poison messages should be in DLQ");
        assertEquals("msg-0", new String(dlqMessages.get(0).getPayload().toByteArray()));
        assertEquals("msg-2", new String(dlqMessages.get(1).getPayload().toByteArray()));

        // Remaining offsets 3, 4 should still be deliverable
        List<StoredMessage> remaining = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 10, 0);
        assertEquals(2, remaining.size());
        assertEquals(3, remaining.get(0).getOffset());
        assertEquals(4, remaining.get(1).getOffset());
    }

    // =========================================================================
    // 12. maxDeliveries = 1 routes to DLQ on first failure
    // =========================================================================

    @Test
    void maxDeliveriesOneRouteImmediately() throws IOException {
        ConsumerGroupCoordinator strict = new ConsumerGroupCoordinator(
                messageStore, offsetManager, null, 50, 1, "dlq.");

        try {
            produceMessages(1);
            String dlqTopic = strict.getDlqTopicName(GROUP, TOPIC);

            // First (and only) attempt — NACK should immediately route to DLQ
            strict.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            boolean routedToDlq = strict.nackOffset(GROUP, TOPIC, "consumer-A", 0);
            assertTrue(routedToDlq, "With maxDeliveries=1, first NACK should route to DLQ");

            List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 1);
            assertEquals(1, dlqMessages.size());
        } finally {
            strict.close();
        }
    }

    // =========================================================================
    // 13. DLQ routing does not block subsequent consumer progress
    // =========================================================================

    @Test
    void dlqRoutingDoesNotBlockOtherConsumersInGroup() {
        produceMessages(6); // offsets 0-5

        // Consumer A gets [0, 3) 
        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 3, 0);
        // Consumer B gets [3, 6)
        List<StoredMessage> batchB = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 3, 0);
        assertEquals(3, batchB.size());
        assertEquals(3, batchB.get(0).getOffset());

        // Consumer B successfully commits
        coordinator.commitOffset(GROUP, TOPIC, "consumer-B", 6);

        // Consumer A NACKs offset 0 to exhaustion (DLQ it)
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            coordinator.nackOffset(GROUP, TOPIC, "consumer-A", 0);
            if (i < MAX_DELIVERIES - 1) {
                coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            }
        }

        // After DLQ routing offset 0, dispatch rewinds to 1.
        // Consumer A picks up the remaining messages from offset 1 onward.
        List<StoredMessage> remaining = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 10, 0);
        assertFalse(remaining.isEmpty(), "Consumer A should get remaining messages");
        assertEquals(1, remaining.get(0).getOffset(), "First message should be offset 1");
    }

    // =========================================================================
    // 14. BrokerConfig DLQ defaults
    // =========================================================================

    @Test
    void brokerConfigDefaultDlqValues() {
        BrokerConfig config = new BrokerConfig(9092, tempDir.toString());
        assertEquals(5, config.getMaxDeliveries());
        assertEquals("dlq.", config.getDlqTopicPrefix());
    }

    @Test
    void brokerConfigParsesCliDlqArgs() {
        BrokerConfig config = BrokerConfig.fromArgs(new String[]{
                "--port", "9092",
                "--data-dir", tempDir.toString(),
                "--max-deliveries", "10",
                "--dlq-topic-prefix", "dead-letters."
        });
        assertEquals(10, config.getMaxDeliveries());
        assertEquals("dead-letters.", config.getDlqTopicPrefix());
    }

    // =========================================================================
    // 15. Lease expiry with multiple messages — only first offset is tracked
    // =========================================================================

    @Test
    void leaseExpiryTracksFromOffsetOfLease() throws Exception {
        // Use single-message batches to precisely control which offset the lease covers
        produceMessages(3);

        String dlqTopic = coordinator.getDlqTopicName(GROUP, TOPIC);

        // Expire the lease MAX_DELIVERIES times — the fromOffset (0) should be DLQ'd
        for (int i = 0; i < MAX_DELIVERIES; i++) {
            List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
            assertEquals(1, batch.size());
            assertEquals(0, batch.get(0).getOffset());

            Thread.sleep(200);
            coordinator.expireLeases();
        }

        // Only offset 0 (the lease's fromOffset) should be DLQ'd
        List<StoredMessage> dlqMessages = getDlqMessagesWithWait(dlqTopic, 1);
        assertEquals(1, dlqMessages.size());
        assertEquals("msg-0", new String(dlqMessages.get(0).getPayload().toByteArray()));

        // Remaining offsets 1-2 should be available
        List<StoredMessage> remaining = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 10, 0);
        assertEquals(2, remaining.size());
        assertEquals(1, remaining.get(0).getOffset());
    }
}
