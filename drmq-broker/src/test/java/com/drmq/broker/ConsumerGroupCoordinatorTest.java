package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerGroupCoordinatorTest {

    @TempDir
    Path tempDir;

    private LogManager logManager;
    private MessageStore messageStore;
    private OffsetManager offsetManager;
    private ConsumerGroupCoordinator coordinator;

    private static final String GROUP = "test-group";
    private static final String TOPIC = "test-topic";

    @BeforeEach
    void setUp() throws IOException {
        logManager = new LogManager(tempDir.toString());
        messageStore = new MessageStore(logManager);
        offsetManager = new OffsetManager(tempDir.toString());
        // Use a very short lease timeout (100ms) so expiry tests don't need long sleeps
        coordinator = new ConsumerGroupCoordinator(messageStore, offsetManager, 100);
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


    @Test
    void expiredLeaseRewindsDispatchOffset() throws Exception {
        produceMessages(5);

        // Consumer A acquires messages 0-4
        List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 5, 0);
        assertEquals(5, batch.size());
        assertEquals(1, coordinator.getActiveLeasesCount());

        // Wait for the lease to expire (100ms timeout)
        Thread.sleep(150);

        // Trigger expiry manually (deterministic, no scheduler dependency)
        coordinator.expireLeases();

        assertEquals(0, coordinator.getActiveLeasesCount());

        // Consumer B should now receive the same messages that A failed to commit
        List<StoredMessage> redelivered = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 5, 0);
        assertEquals(5, redelivered.size());
        assertEquals(0, redelivered.get(0).getOffset());
        assertEquals(4, redelivered.get(4).getOffset());
    }

    @Test
    void committedLeaseDoesNotGetRedelivered() throws Exception {
        produceMessages(3);

        List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 3, 0);
        assertEquals(3, batch.size());

        // Commit before the lease expires
        coordinator.commitOffset(GROUP, TOPIC, "consumer-A", 3);

        // Wait past the lease timeout
        Thread.sleep(150);
        coordinator.expireLeases();

        // No active leases and nothing to re-deliver
        assertEquals(0, coordinator.getActiveLeasesCount());

        // Next consumer should get nothing (no new messages produced)
        List<StoredMessage> next = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 10, 0);
        assertTrue(next.isEmpty());
    }


    @Test
    void outOfOrderCommitsCoalesceCorrectly() {
        produceMessages(10);

        // Consumer A gets offsets [0, 5)
        List<StoredMessage> batchA = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 5, 0);
        assertEquals(5, batchA.size());
        assertEquals(0, batchA.get(0).getOffset());

        // Consumer B gets offsets [5, 10)
        List<StoredMessage> batchB = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 5, 0);
        assertEquals(5, batchB.size());
        assertEquals(5, batchB.get(0).getOffset());

        // B commits FIRST (out of order). Committed offset should NOT advance past 0
        // because there's a gap: A's [0,5) is still uncommitted.
        // offsetManager.fetch() returns -1 since no offset has been persisted yet
        // (the coordinator only persists when advanceCommittedOffset actually advances).
        coordinator.commitOffset(GROUP, TOPIC, "consumer-B", 10);
        assertEquals(-1, offsetManager.fetch(GROUP, TOPIC),
                "Committed offset must not advance past the gap left by consumer-A");

        // Now A commits. The committed offset should jump all the way to 10
        // (ranges [0,5) and [5,10) merge contiguously from committedOffset=0).
        coordinator.commitOffset(GROUP, TOPIC, "consumer-A", 5);
        assertEquals(10, offsetManager.fetch(GROUP, TOPIC),
                "After both ranges are committed, offset should advance to 10");
    }

    @Test
    void singleConsumerCommitAdvancesImmediately() {
        produceMessages(3);

        List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 3, 0);
        assertEquals(3, batch.size());

        coordinator.commitOffset(GROUP, TOPIC, "consumer-A", 3);
        assertEquals(3, offsetManager.fetch(GROUP, TOPIC));
    }


    @Test
    void concurrentAcquireReturnsEmptyOnContention() {
        produceMessages(5);

        // Consumer A acquires offsets [0, 5) — advances dispatchOffset to 5
        List<StoredMessage> batchA = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 5, 0);
        assertEquals(5, batchA.size());

        // Consumer B tries to acquire — no new messages exist past offset 5
        List<StoredMessage> batchB = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 5, 0);
        assertTrue(batchB.isEmpty(),
                "Consumer B should get nothing because A already took all available messages");
    }


    @Test
    void commitAfterLeaseExpiryDoesNotCrash() throws Exception {
        produceMessages(3);

        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 3, 0);

        // Let the lease expire
        Thread.sleep(150);
        coordinator.expireLeases();

        // Consumer A tries to commit after its lease was already reaped.
        // This should not throw and should not corrupt state.
        assertDoesNotThrow(() ->
                coordinator.commitOffset(GROUP, TOPIC, "consumer-A", 3));

        // The lease was already removed by the reaper, so commitOffset finds
        // lease == null and does NOT record a CommittedRange.
        // Therefore advanceCommittedOffset has nothing to merge and
        // offsetManager is never written to. This is correct behavior:
        // the expired lease was already rewound for re-delivery.
        long committed = offsetManager.fetch(GROUP, TOPIC);
        assertEquals(-1, committed,
                "No offset should be persisted when commit arrives after lease expiry");
    }

    // =========================================================================
    // 5. Commit with no active state (group never coordinated)
    // =========================================================================

    @Test
    void commitWithNoActiveStateFallsThroughToOffsetManager() {
        // Never called acquireMessages — no group state exists
        coordinator.commitOffset("unknown-group", TOPIC, "consumer-X", 42);

        // Should fall through to offsetManager.commit() directly
        assertEquals(42, offsetManager.fetch("unknown-group", TOPIC));
    }

    // =========================================================================
    // 6. isGroupActive correctness
    // =========================================================================

    @Test
    void isGroupActiveReturnsFalseBeforeFirstAcquire() {
        assertFalse(coordinator.isGroupActive(GROUP, TOPIC));
    }

    @Test
    void isGroupActiveReturnsTrueAfterAcquire() {
        produceMessages(1);
        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);
        assertTrue(coordinator.isGroupActive(GROUP, TOPIC));
    }

    @Test
    void isGroupActiveReturnsFalseAfterAllLeasesExpire() throws Exception {
        produceMessages(1);
        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 1, 0);

        Thread.sleep(150);
        coordinator.expireLeases();

        assertFalse(coordinator.isGroupActive(GROUP, TOPIC),
                "Group should not be active after all members' leases expire");
    }

    // =========================================================================
    // 7. Offset bootstrap from OffsetManager
    // =========================================================================

    @Test
    void newGroupStartsFromPersistedOffset() {
        produceMessages(10);

        // Simulate a prior session that committed offset 5
        offsetManager.commit(GROUP, TOPIC, 5);

        // New coordinator bootstraps from persisted offset
        List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 10, 0);

        // Should start from offset 5, not 0
        assertFalse(batch.isEmpty());
        assertEquals(5, batch.get(0).getOffset());
    }

    @Test
    void newGroupWithNoCommittedOffsetStartsFromZero() {
        produceMessages(3);

        // No prior commits — OffsetManager.fetch() returns -1
        assertEquals(-1, offsetManager.fetch(GROUP, TOPIC));

        List<StoredMessage> batch = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 10, 0);
        assertEquals(3, batch.size());
        assertEquals(0, batch.get(0).getOffset());
    }

    // =========================================================================
    // 8. Multiple leases and partial expiry
    // =========================================================================

    @Test
    void onlyExpiredLeasesAreRewound() throws Exception {
        produceMessages(10);

        // Consumer A gets [0, 5)
        List<StoredMessage> batchA = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 5, 0);
        assertEquals(5, batchA.size());

        // Wait almost until expiry, then consumer B gets [5, 10)
        Thread.sleep(60);
        List<StoredMessage> batchB = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 5, 0);
        assertEquals(5, batchB.size());

        // Wait so A's lease expires (~100ms total for A) but B's hasn't (~60ms for B)
        Thread.sleep(60);
        coordinator.expireLeases();

        // A's lease should be gone, B's should still be active
        assertEquals(1, coordinator.getActiveLeasesCount());

        // Dispatch offset should rewind to A's range start (0), not B's
        // Next acquire should get offsets starting from 0
        List<StoredMessage> redelivered = coordinator.acquireMessages(GROUP, TOPIC, "consumer-C", 5, 0);
        assertEquals(5, redelivered.size());
        assertEquals(0, redelivered.get(0).getOffset());
    }

    // =========================================================================
    // 9. Consumer count tracking
    // =========================================================================

    @Test
    void consumerCountTracksActiveMembers() throws Exception {
        produceMessages(10);

        assertEquals(0, coordinator.getConsumerCount(GROUP, TOPIC));

        coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 3, 0);
        assertEquals(1, coordinator.getConsumerCount(GROUP, TOPIC));

        coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 3, 0);
        assertEquals(2, coordinator.getConsumerCount(GROUP, TOPIC));

        // Expire all leases — members are removed
        Thread.sleep(150);
        coordinator.expireLeases();
        assertEquals(0, coordinator.getConsumerCount(GROUP, TOPIC));
    }

    // =========================================================================
    // 10. Disjoint delivery (no duplicates across consumers)
    // =========================================================================

    @Test
    void twoConsumersGetDisjointOffsets() {
        produceMessages(20);

        Set<Long> allOffsets = new HashSet<>();
        int totalMessages = 0;

        List<StoredMessage> a = coordinator.acquireMessages(GROUP, TOPIC, "consumer-A", 10, 0);
        for (StoredMessage m : a) assertTrue(allOffsets.add(m.getOffset()), "Duplicate offset: " + m.getOffset());
        totalMessages += a.size();

        List<StoredMessage> b = coordinator.acquireMessages(GROUP, TOPIC, "consumer-B", 10, 0);
        for (StoredMessage m : b) assertTrue(allOffsets.add(m.getOffset()), "Duplicate offset: " + m.getOffset());
        totalMessages += b.size();

        assertEquals(20, totalMessages);
        assertEquals(20, allOffsets.size());
    }
}
