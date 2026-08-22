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
 * Integration tests for atomic batch crash-recovery.
 *
 * Two distinct recovery paths are tested:
 *
 *  Scenario A — Crashed node (e.g. old leader) restarts:
 *    The .atomic-intent file exists on disk because the node crashed after
 *    writing the intent file but before finishing all topic-log writes.
 *    MessageStore.recover() must complete the partial write idempotently.
 *
 *  Scenario B — New leader re-applies from Raft log:
 *    The new leader has the committed ATOMIC_BATCH entry in its Raft log but
 *    has never seen the intent file (it lives only on the crashed node).
 *    It must be able to apply the entry cleanly via appendAtomicBatch(), as
 *    though it is the first time, arriving at the same result as Scenario A.
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

    /**
     * Write a .atomic-intent file for the given slices at baseOffset,
     * mirroring the exact binary format used by appendAtomicBatch().
     * This simulates what is on disk after a crash between writing the intent
     * file and finishing the topic-log writes.
     */
    private void writeIntentFile(Path dataDir, List<AtomicBatchTopicSlice> slices, long baseOffset)
            throws IOException {
        long currentOffset = baseOffset;
        java.util.Map<String, List<StoredMessage>> topicMessages = new java.util.LinkedHashMap<>();
        for (AtomicBatchTopicSlice slice : slices) {
            List<StoredMessage> msgs = topicMessages.computeIfAbsent(slice.getTopic(),
                    k -> new java.util.ArrayList<>());
            for (ProduceBatchRequest.BatchEntry entry : slice.getEntriesList()) {
                msgs.add(StoredMessage.newBuilder()
                        .setOffset(currentOffset++)
                        .setTopic(slice.getTopic())
                        .setPayload(entry.getPayload())
                        .setTimestamp(entry.getClientTimestamp())
                        .setStoredAt(System.currentTimeMillis())
                        .setRaftIndex(42L)
                        .build());
            }
        }

        Path intentFile = dataDir.resolve(".atomic-intent");
        try (FileOutputStream fos = new FileOutputStream(intentFile.toFile());
             DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeInt(topicMessages.size());
            for (var entry : topicMessages.entrySet()) {
                dos.writeUTF(entry.getKey());
                List<StoredMessage> msgList = entry.getValue();
                dos.writeInt(msgList.size());
                for (StoredMessage msg : msgList) {
                    byte[] bytes = msg.toByteArray();
                    dos.writeInt(bytes.length);
                    dos.write(bytes);
                }
            }
            dos.flush();
            fos.getFD().sync();
        }
    }

    // ---------------------------------------------------------------------------
    // Scenario A: Crashed node recovers from its own intent file
    // ---------------------------------------------------------------------------

    /**
     * Simulates a crash immediately after the intent file was written but
     * before any topic log was touched. All messages must be recovered.
     */
    @Test
    void scenarioA_crashBeforeAnyTopicWrite_recoversAllMessages() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;

        // Simulate crash: write the intent file but write nothing to the topic logs.
        writeIntentFile(tempDir, slices, baseOffset);
        assertTrue(Files.exists(tempDir.resolve(".atomic-intent")),
                "Pre-condition: intent file must be on disk before recovery");

        // Node restarts — MessageStore.recover() runs.
        MessageStore store = openStore(tempDir);
        store.recover();

        // Intent file must be deleted after successful recovery.
        assertFalse(Files.exists(tempDir.resolve(".atomic-intent")),
                "Intent file must be deleted after successful recovery");

        // All messages must be visible.
        assertEquals(3L, store.getCurrentOffset(), "All 3 messages must have been recovered");
        assertEquals(2L, store.getMessageCount("orders"));
        assertEquals(1L, store.getMessageCount("payments"));

        StoredMessage order1 = store.getMessage("orders", 0);
        assertNotNull(order1);
        assertEquals("order-1", order1.getPayload().toStringUtf8());

        StoredMessage order2 = store.getMessage("orders", 1);
        assertNotNull(order2);
        assertEquals("order-2", order2.getPayload().toStringUtf8());

        StoredMessage payment1 = store.getMessage("payments", 2);
        assertNotNull(payment1);
        assertEquals("payment-1", payment1.getPayload().toStringUtf8());
    }

    /**
     * Simulates a crash mid-write: the first topic ("orders") was fully
     * written to disk but the second topic ("payments") was not started.
     * Recovery must write only the missing messages, not duplicate "orders".
     */
    @Test
    void scenarioA_crashAfterFirstTopicWrite_recoversOnlyMissingMessages() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;

        // Write the intent file first (as appendAtomicBatch does).
        writeIntentFile(tempDir, slices, baseOffset);

        // Partially apply: write "orders" to the topic log, but not "payments".
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
                42L, baseOffset);
        // Close without writing "payments" or deleting the intent file — simulate crash.
        logManager.close();
        logManager = null;

        // Re-open and recover — simulates the node restarting.
        MessageStore store = openStore(tempDir);
        store.recover();

        assertFalse(Files.exists(tempDir.resolve(".atomic-intent")),
                "Intent file must be deleted after recovery");

        // "orders" must not be duplicated.
        assertEquals(2L, store.getMessageCount("orders"),
                "orders must have exactly 2 messages — no duplicates");

        // "payments" must now be present.
        assertEquals(1L, store.getMessageCount("payments"),
                "payments must have been recovered");

        // Offsets must be consistent.
        assertEquals(3L, store.getCurrentOffset());
        assertEquals("payment-1", store.getMessage("payments", 2).getPayload().toStringUtf8());
    }

    /**
     * If the node crashed after all topic logs were written but before the
     * intent file was deleted, recovery must be a no-op (all offsets already
     * present) and still clean up the intent file.
     */
    @Test
    void scenarioA_crashAfterAllTopicWrites_noopRecoveryDeletesIntentFile() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;

        // Write the intent file.
        writeIntentFile(tempDir, slices, baseOffset);

        // Fully apply all slices to the topic logs.
        MessageStore preStore = openStore(tempDir);
        preStore.appendAtomicBatch(slices, 42L, baseOffset);
        // appendAtomicBatch deletes the intent file on success — re-create it
        // to simulate a crash between the last segment fsync and the delete.
        writeIntentFile(tempDir, slices, baseOffset);
        logManager.close();
        logManager = null;

        // Restart and recover.
        MessageStore store = openStore(tempDir);
        store.recover();

        assertFalse(Files.exists(tempDir.resolve(".atomic-intent")),
                "Intent file must be deleted even when recovery is a no-op");

        // Exactly the original 3 messages, no duplicates.
        assertEquals(3L, store.getCurrentOffset());
        assertEquals(2L, store.getMessageCount("orders"));
        assertEquals(1L, store.getMessageCount("payments"));
    }

    // ---------------------------------------------------------------------------
    // Scenario B: New leader re-applies the committed entry from the Raft log
    // ---------------------------------------------------------------------------

    /**
     * The new leader never had an intent file. It holds the committed
     * ATOMIC_BATCH entry in its own Raft log and calls appendAtomicBatch()
     * directly, as RaftNode.applyCommitted() does. This must produce the
     * same result as Scenario A without requiring the intent file at all.
     */
    @Test
    void scenarioB_newLeaderAppliesFromRaftLog_noIntentFileRequired() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;
        long raftIndex = 42L;

        // No intent file exists on this node — it was never the writer.
        assertFalse(Files.exists(tempDir.resolve(".atomic-intent")),
                "Pre-condition: new leader must not have an intent file");

        // New leader opens its store and applies the committed Raft entry.
        MessageStore store = openStore(tempDir);
        Map<String, Long> offsets = store.appendAtomicBatch(slices, raftIndex, baseOffset);

        // The intent file is created then immediately deleted by appendAtomicBatch on success.
        assertFalse(Files.exists(tempDir.resolve(".atomic-intent")),
                "Intent file must be cleaned up after successful apply");

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
     * Proves that Scenario A (crashed node) and Scenario B (new leader) converge
     * on the exact same durable state for the same Raft-committed operation,
     * even though they took completely different code paths to get there.
     */
    @Test
    void scenarioAandB_produceIdenticalDurableState() throws IOException {
        List<AtomicBatchTopicSlice> slices = buildSlices();
        long baseOffset = 0L;
        long raftIndex = 42L;

        // --- Scenario A: crashed node path ---
        Path dirA = tempDir.resolve("node-crashed");
        Files.createDirectories(dirA);
        writeIntentFile(dirA, slices, baseOffset);
        LogManager lmA = new LogManager(dirA.toString());
        MessageStore storeA = new MessageStore(lmA, new BrokerConfig(9092, dirA.toString()));
        storeA.recover();
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

            long[][] topicOffsets = {{0, 1}, {2}};   // orders: 0,1  payments: 2
            String[] topics = {"orders", "orders", "payments"};
            long[] offsets  = {0, 1, 2};

            for (int i = 0; i < offsets.length; i++) {
                StoredMessage mA = recoveredA.getMessage(topics[i], offsets[i]);
                StoredMessage mB = recoveredB.getMessage(topics[i], offsets[i]);
                assertNotNull(mA, "Crashed node must have message at offset " + offsets[i]);
                assertNotNull(mB, "New leader must have message at offset " + offsets[i]);
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
}
