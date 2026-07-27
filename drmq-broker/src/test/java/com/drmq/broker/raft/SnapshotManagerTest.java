package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.broker.persistence.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.drmq.protocol.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotManagerTest {

    @TempDir
    Path tempDir;

    private LogManager logManager;
    private MessageStore messageStore;
    private OffsetManager offsetManager;
    private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() throws IOException {
        logManager = new LogManager(tempDir.toString());
        messageStore = new MessageStore(logManager, new BrokerConfig(9092, tempDir.toString()));
        offsetManager = new OffsetManager(tempDir.toString());
        
        snapshotManager = new SnapshotManager(tempDir, messageStore, offsetManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logManager != null) {
            logManager.close();
        }
    }

    @Test
    void testStreamIncrementalSegments() throws IOException {
        // 1. Create some dummy state to stream
        messageStore.append("test-topic", "dummy-message-data".getBytes(), null, System.currentTimeMillis());
        Path topicDir = tempDir.resolve("test-topic");

        Path offsetsDir = tempDir.resolve("__consumer_offsets");
        Files.createDirectories(offsetsDir);
        Files.writeString(offsetsDir.resolve("offsets.properties"), "mygroup-mytopic-0=100");
        
        java.util.Map<String, Long> followerOffsets = new java.util.HashMap<>();
        followerOffsets.put("test-topic", 0L); // Follower is at offset 0
        
        BrokerConfig.PeerAddress dummyPeer = new BrokerConfig.PeerAddress("node2", "localhost", 9093);
        
        java.util.concurrent.atomic.AtomicInteger chunkCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicBoolean doneCalled = new java.util.concurrent.atomic.AtomicBoolean(false);

        java.util.function.Function<IncrementalSnapshotChunk, IncrementalSnapshotChunkResponse> chunkHandler = req -> {
            chunkCount.incrementAndGet();
            assertEquals("test-topic", req.getTopic());
            assertTrue(req.getData().size() > 0);
            return IncrementalSnapshotChunkResponse.newBuilder().setSuccess(true).build();
        };

        java.util.function.Function<IncrementalSnapshotDoneRequest, IncrementalSnapshotDoneResponse> doneHandler = req -> {
            doneCalled.set(true);
            assertEquals(42L, req.getLastIncludedIndex());
            return IncrementalSnapshotDoneResponse.newBuilder().setSuccess(true).build();
        };

        // Stream the segments
        snapshotManager.streamIncrementalSegments(
                followerOffsets,
                42L,
                1L,
                "node1",
                dummyPeer,
                chunkHandler,
                doneHandler
        );

        assertTrue(chunkCount.get() > 0, "Should have streamed at least one chunk");
        assertTrue(doneCalled.get(), "Done handler should have been called");
    }
}
