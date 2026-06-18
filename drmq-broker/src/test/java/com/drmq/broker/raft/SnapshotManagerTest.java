package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.broker.persistence.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    void testCreateAndRestoreSnapshot() throws IOException {
        // 1. Create some dummy state to snapshot
        Path storeDir = tempDir.resolve("store");
        Files.createDirectories(storeDir);
        Files.writeString(storeDir.resolve("test-topic.log"), "dummy-message-data");

        Path offsetsDir = tempDir.resolve("__consumer_offsets");
        Files.createDirectories(offsetsDir);
        Files.writeString(offsetsDir.resolve("offsets.properties"), "mygroup-mytopic-0=100");

        // 2. Create the snapshot
        long raftIndex = 42;
        Path zipFile = snapshotManager.createSnapshot(raftIndex);

        assertTrue(Files.exists(zipFile));
        assertTrue(zipFile.getFileName().toString().contains("42"));

        // 3. Wipe the original state manually to simulate follower before restoration
        Files.delete(storeDir.resolve("test-topic.log"));
        Files.delete(offsetsDir.resolve("offsets.properties"));
        assertFalse(Files.exists(storeDir.resolve("test-topic.log")));
        assertFalse(Files.exists(offsetsDir.resolve("offsets.properties")));

        // 4. Restore the snapshot
        snapshotManager.restoreSnapshot(zipFile);

        // 5. Verify the state was fully restored
        assertTrue(Files.exists(storeDir.resolve("test-topic.log")));
        assertEquals("dummy-message-data", Files.readString(storeDir.resolve("test-topic.log")));

        assertTrue(Files.exists(offsetsDir.resolve("offsets.properties")));
        assertEquals("mygroup-mytopic-0=100", Files.readString(offsetsDir.resolve("offsets.properties")));
    }

    @Test
    void testZipSlipPrevention() throws IOException {
        // 1. Create a malicious zip file
        Path zipFile = tempDir.resolve("malicious.zip");
        try (java.io.OutputStream fos = Files.newOutputStream(zipFile);
             java.util.zip.ZipOutputStream zos = new java.util.zip.ZipOutputStream(fos)) {
            
            // Create a malicious entry pointing outside the target directory
            java.util.zip.ZipEntry maliciousEntry = new java.util.zip.ZipEntry("../../../../../tmp/hacked.txt");
            zos.putNextEntry(maliciousEntry);
            zos.write("You've been hacked!".getBytes());
            zos.closeEntry();
        }

        // 2. Attempt to restore it, which should throw an IOException
        IOException exception = assertThrows(IOException.class, () -> {
            snapshotManager.restoreSnapshot(zipFile);
        });

        // 3. Verify the exception message
        assertTrue(exception.getMessage().contains("outside of target dir") || 
                   exception.getMessage().contains("Zip entry is outside"));
    }
}
