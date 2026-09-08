package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.protocol.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for atomic batch crash-recovery using completion markers.
 *
 * The recovery mechanism uses post-write .atomic-done-{raftIndex} marker files.
 * After all topic segments in an atomic batch are durably written, a marker file
 * is created. On recovery, if the highest raftIndex in the segments does NOT
 * have a corresponding marker, the apply was partial and must be truncated so
 * Raft re-applies the entry.
 *
 * Two distinct recovery scenarios are tested:
 *
 *  Scenario A — Crashed node (partial apply):
 *    The node crashed after writing some (but not all) topic segments for an
 *    atomic batch. No .atomic-done marker exists because the marker is written
 *    after all segments. Recovery detects the missing marker, truncates the
 *    partially-written messages, and decrements lastAppliedRaftIndex so Raft
 *    will re-apply the entry.
 *
 *  Scenario B — New leader re-applies from Raft log:
 *    The new leader has the committed ATOMIC_BATCH entry in its Raft log but
 *    has never applied it locally. It must be able to apply the entry cleanly
 *    via appendAtomicBatch(), which writes all segments and then the marker.
 */
class AtomicBatchCrashRecoveryTest {

    @TempDir
    Path tempDir;

    private LogManager logManager;

    @AfterEach
    void tearDown() throws IOException {
        if (logManager != null) {
            logManager.close();
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private List<AtomicBatchTopicSlice> buildSlices() {
        AtomicBatchTopicSlice sliceA = AtomicBatchTopicSlice.newBuilder()
                .setTopic("orders")
                .addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("order-1"))
                        .setClientTimestamp(1000L).build())
                .addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("order-2"))
                        .setClientTimestamp(2000L).build())
                .build();

        AtomicBatchTopicSlice sliceB = AtomicBatchTopicSlice.newBuilder()
                .setTopic("payments")
                .addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("payment-1"))
                        .setClientTimestamp(3000L).build())
                .build();

        return Arrays.asList(sliceA, sliceB);
    }

    private MessageStore openStore(Path dir) throws IOException {
        logManager = new LogManager(dir.toString());
        return new MessageStore(logManager, new BrokerConfig(9092, dir.toString()));
    }

    // ---------------------------------------------------------------------------
    // Scenario A: Crashed node — partial apply detected by missing marker
    // ---------------------------------------------------------------------------

    /**
     * Simulates a crash after only one topic was written. The "orders" topic
     * has messages with raftIndex=42, but "payments" does not. No .atomic-done-42
     * marker exists. Recovery must:
     *   1. Detect the missing marker for raftIndex 42
     *   2. Truncate "orders" segments to remove the partial batch messages
     *   3. Set lastAppliedRaftIndex below 42 so Raft re-applies the entry
     */
    @Test
    void scenarioA_crashAfterFirstTopicWrite_truncatesPartialApply() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;
        long raftIndex = 42L;

        // Partially apply: write only "orders" to the topic log, skip "payments".
        // No completion marker is written.
        MessageStore partialStore = openStore(tempDir);
        partialStore.appendBatch("orders",
                List.of(
                        ProduceBatchRequest.BatchEntry.newBuilder()
                                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("order-1"))
                                .setClientTimestamp(1000L).build(),
                        ProduceBatchRequest.BatchEntry.newBuilder()
                                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("order-2"))
                                .setClientTimestamp(2000L).build()
                ),
                raftIndex, baseOffset);

        // Verify the partial state before crash
        assertEquals(2L, partialStore.getMessageCount("orders"));
        assertEquals(0L, partialStore.getMessageCount("payments"));

        // No marker file should exist
        assertFalse(Files.exists(tempDir.resolve(".atomic-done-42")),
                "No completion marker should exist for the partial apply");

        // Close to simulate crash
        logManager.close();
        logManager = null;

        // Re-open and recover — simulates the node restarting.
        MessageStore store = openStore(tempDir);
        store.recover();

        // After recovery, the partial batch should be truncated.
        // lastAppliedRaftIndex should be decremented below 42 so Raft re-applies.
        assertTrue(store.getLastAppliedRaftIndex() < raftIndex,
                "lastAppliedRaftIndex must be decremented below " + raftIndex +
                        " for Raft re-apply, but was " + store.getLastAppliedRaftIndex());

        // "orders" messages from the partial batch should be removed.
        assertEquals(0L, store.getMessageCount("orders"),
                "orders must have 0 messages after truncation of partial batch");
        assertEquals(0L, store.getMessageCount("payments"),
                "payments must still have 0 messages");
    }

    /**
     * If the node crashed after all topic logs were written AND the completion
     * marker was also written, recovery must be a no-op — all data is intact.
     */
    @Test
    void scenarioA_crashAfterAllWritesAndMarker_noopRecovery() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;
        long raftIndex = 42L;

        // Fully apply all slices — this writes segments AND the marker.
        MessageStore preStore = openStore(tempDir);
        preStore.appendAtomicBatch(slices, raftIndex, baseOffset);

        // Verify marker exists
        assertTrue(Files.exists(tempDir.resolve(".atomic-done-42")),
                "Completion marker must exist after successful apply");

        logManager.close();
        logManager = null;

        // Restart and recover.
        MessageStore store = openStore(tempDir);
        store.recover();

        // Exactly the original 3 messages, no duplicates, no truncation.
        assertEquals(3L, store.getCurrentOffset());
        assertEquals(2L, store.getMessageCount("orders"));
        assertEquals(1L, store.getMessageCount("payments"));
        assertEquals("order-1", store.getMessage("orders", 0).getPayload().toStringUtf8());
        assertEquals("order-2", store.getMessage("orders", 1).getPayload().toStringUtf8());
        assertEquals("payment-1", store.getMessage("payments", 2).getPayload().toStringUtf8());
    }

    /**
     * Simulates a crash after all segments were written but before the completion
     * marker was created. Recovery should truncate and Raft will re-apply.
     */
    @Test
    void scenarioA_crashAfterAllWritesButBeforeMarker_truncatesForReapply() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;
        long raftIndex = 42L;

        // Fully apply, then delete the marker to simulate crash before marker write.
        MessageStore preStore = openStore(tempDir);
        preStore.appendAtomicBatch(slices, raftIndex, baseOffset);
        // Delete the marker to simulate the crash happening before marker was written
        Files.deleteIfExists(tempDir.resolve(".atomic-done-42"));

        logManager.close();
        logManager = null;

        // Restart and recover.
        MessageStore store = openStore(tempDir);
        store.recover();

        // Without the marker, recovery must treat this as a partial apply.
        // It truncates all messages with raftIndex=42 so Raft can re-apply cleanly.
        assertTrue(store.getLastAppliedRaftIndex() < raftIndex,
                "lastAppliedRaftIndex must be decremented for Raft re-apply");
        assertEquals(0L, store.getMessageCount("orders"),
                "orders must be truncated");
        assertEquals(0L, store.getMessageCount("payments"),
                "payments must be truncated");
    }

    /**
     * When prior messages exist from earlier Raft entries, only the partial batch
     * at the highest raftIndex should be truncated. Earlier messages must survive.
     */
    @Test
    void scenarioA_partialApplyWithPriorMessages_onlyTruncatesLatestBatch() throws IOException {
        long priorRaftIndex = 40L;
        long atomicRaftIndex = 42L;

        MessageStore store = openStore(tempDir);

        // Write some prior single-topic messages at raftIndex=40
        store.appendBatch("orders",
                List.of(
                        ProduceBatchRequest.BatchEntry.newBuilder()
                                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("prior-order"))
                                .setClientTimestamp(500L).build()
                ),
                priorRaftIndex, 0L);

        // Now simulate a partial atomic batch at raftIndex=42 — only "orders" written
        store.appendBatch("orders",
                List.of(
                        ProduceBatchRequest.BatchEntry.newBuilder()
                                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("atomic-order"))
                                .setClientTimestamp(1000L).build()
                ),
                atomicRaftIndex, 1L);
        // "payments" not written — simulating crash

        logManager.close();
        logManager = null;

        // Restart and recover
        store = openStore(tempDir);
        store.recover();

        // The prior message at raftIndex=40 must survive
        assertEquals(1L, store.getMessageCount("orders"),
                "Prior orders message at raftIndex=40 must survive");
        assertEquals("prior-order", store.getMessage("orders", 0).getPayload().toStringUtf8());

        // lastAppliedRaftIndex should be 40 (the prior entry), not 42
        assertEquals(priorRaftIndex, store.getLastAppliedRaftIndex(),
                "lastAppliedRaftIndex must be decremented to the prior entry");
    }

    // ---------------------------------------------------------------------------
    // Scenario B: New leader re-applies the committed entry from the Raft log
    // ---------------------------------------------------------------------------

    /**
     * The new leader never had any partial state. It holds the committed
     * ATOMIC_BATCH entry in its Raft log and calls appendAtomicBatch()
     * directly, as RaftNode.applyCommitted() does. This must produce the
     * correct result and write a completion marker.
     */
    @Test
    void scenarioB_newLeaderAppliesFromRaftLog_writesMarker() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;
        long raftIndex = 42L;

        // New leader opens its store and applies the committed Raft entry.
        MessageStore store = openStore(tempDir);
        Map<String, Long> offsets = store.appendAtomicBatch(slices, raftIndex, baseOffset);

        // Completion marker must exist after successful apply.
        assertTrue(Files.exists(tempDir.resolve(".atomic-done-42")),
                "Completion marker must be written after successful apply");

        // All messages present with correct offsets.
        assertEquals(2, offsets.size());
        assertEquals(0L, offsets.get("orders"));
        assertEquals(2L, offsets.get("payments"));
        assertEquals(3L, store.getCurrentOffset());
        assertEquals("order-1",   store.getMessage("orders",   0).getPayload().toStringUtf8());
        assertEquals("order-2",   store.getMessage("orders",   1).getPayload().toStringUtf8());
        assertEquals("payment-1", store.getMessage("payments", 2).getPayload().toStringUtf8());
    }

    /**
     * Proves that Scenario A (crashed node with marker) and Scenario B (new leader)
     * converge on the exact same durable state for the same Raft-committed operation.
     */
    @Test
    void scenarioAandB_produceIdenticalDurableState() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;
        long raftIndex = 42L;

        // --- Scenario A: fully applied with marker, then restart ---
        Path dirA = tempDir.resolve("node-with-marker");
        Files.createDirectories(dirA);
        LogManager lmA = new LogManager(dirA.toString());
        MessageStore storeA = new MessageStore(lmA, new BrokerConfig(9092, dirA.toString()));
        storeA.appendAtomicBatch(slices, raftIndex, baseOffset);
        lmA.close();

        // --- Scenario B: new leader path ---
        Path dirB = tempDir.resolve("node-new-leader");
        Files.createDirectories(dirB);
        LogManager lmB = new LogManager(dirB.toString());
        MessageStore storeB = new MessageStore(lmB, new BrokerConfig(9092, dirB.toString()));
        storeB.appendAtomicBatch(slices, raftIndex, baseOffset);
        lmB.close();

        // Re-open each and recover to compare durable on-disk state.
        LogManager lmA2 = new LogManager(dirA.toString());
        MessageStore recoveredA = new MessageStore(lmA2, new BrokerConfig(9092, dirA.toString()));
        recoveredA.recover();

        LogManager lmB2 = new LogManager(dirB.toString());
        MessageStore recoveredB = new MessageStore(lmB2, new BrokerConfig(9092, dirB.toString()));
        recoveredB.recover();

        try {
            assertEquals(recoveredA.getCurrentOffset(), recoveredB.getCurrentOffset(),
                    "Both nodes must end up at the same global offset");
            assertEquals(recoveredA.getMessageCount("orders"),  recoveredB.getMessageCount("orders"));
            assertEquals(recoveredA.getMessageCount("payments"), recoveredB.getMessageCount("payments"));

            String[] topics = {"orders", "orders", "payments"};
            long[] offsets  = {0, 1, 2};

            for (int i = 0; i < offsets.length; i++) {
                StoredMessage mA = recoveredA.getMessage(topics[i], offsets[i]);
                StoredMessage mB = recoveredB.getMessage(topics[i], offsets[i]);
                assertNotNull(mA, "Node A must have message at offset " + offsets[i]);
                assertNotNull(mB, "Node B must have message at offset " + offsets[i]);
                assertEquals(mA.getPayload(), mB.getPayload(),
                        "Payload at offset " + offsets[i] + " must be identical on both nodes");
                assertEquals(mA.getOffset(), mB.getOffset(),
                        "Offset must match on both nodes");
            }
        } finally {
            lmA2.close();
            lmB2.close();
        }
    }

    /**
     * Verifies that old completion markers are cleaned up after a new atomic
     * batch is applied, preventing marker file accumulation.
     */
    @Test
    void markerCleanup_oldMarkersRemovedAfterNewBatch() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();

        MessageStore store = openStore(tempDir);

        // Apply first atomic batch at raftIndex=10
        store.appendAtomicBatch(slices, 10L, 0L);
        assertTrue(Files.exists(tempDir.resolve(".atomic-done-10")),
                "Marker for raftIndex 10 must exist");

        // Apply second atomic batch at raftIndex=20
        store.appendAtomicBatch(slices, 20L, 3L);
        assertTrue(Files.exists(tempDir.resolve(".atomic-done-20")),
                "Marker for raftIndex 20 must exist");
        assertFalse(Files.exists(tempDir.resolve(".atomic-done-10")),
                "Old marker for raftIndex 10 must be cleaned up");
    }

    /**
     * In no-raft mode (raftIndex = -1), no completion markers should be written.
     */
    @Test
    void noRaftMode_noMarkerWritten() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();

        MessageStore store = openStore(tempDir);
        store.appendAtomicBatch(slices, -1L);

        // No marker files should exist
        try (var files = Files.list(tempDir)) {
            long markerCount = files
                    .filter(p -> p.getFileName().toString().startsWith(".atomic-done-"))
                    .count();
            assertEquals(0L, markerCount, "No markers should be written in no-raft mode");
        }

        // Messages should still be written correctly
        assertEquals(3L, store.getCurrentOffset());
        assertEquals(2L, store.getMessageCount("orders"));
        assertEquals(1L, store.getMessageCount("payments"));
    }
}
