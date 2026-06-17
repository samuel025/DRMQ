package com.drmq.broker.raft;

import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.util.zip.ZipInputStream;

/**
 * Handles zipping up the broker's data directory (MessageStore and OffsetManager state)
 * into a single archive for transmission to lagging Raft followers.
 */
public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private final Path dataDir;
    private final MessageStore messageStore;
    private final OffsetManager offsetManager;

    public SnapshotManager(Path dataDir, MessageStore messageStore, OffsetManager offsetManager) {
        this.dataDir = dataDir;
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
    }

    /**
     * Zips up the current state of the node into a snapshot file.
     * Blocks appends to the MessageStore while creating the snapshot to ensure consistency.
     *
     * @param lastIncludedIndex The last Raft index applied to the state machine before this snapshot.
     * @return Path to the generated zip file.
     */
    public Path createSnapshot(long lastIncludedIndex) throws IOException {
        // 1. Force the OffsetManager to dump its in-memory map to disk
        if (offsetManager != null) {
            offsetManager.forcePersist();
        }

        // 2. Prepare the snapshot directory and file
        Path snapshotsDir = dataDir.resolve("raft/snapshots");
        Files.createDirectories(snapshotsDir);
        Path zipFile = snapshotsDir.resolve("snapshot_" + lastIncludedIndex + ".zip");

        logger.info("Starting snapshot generation for index {}...", lastIncludedIndex);
        long startMs = System.currentTimeMillis();

        // 3. Lock MessageStore to prevent concurrent appends during the zip process
        messageStore.lockForSnapshot(() -> {
            try (OutputStream fos = Files.newOutputStream(zipFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                 ZipOutputStream zos = new ZipOutputStream(fos)) {

                // 3a. Zip the topics/messages store directory
                Path storeDir = dataDir.resolve("store");
                zipDirectory(storeDir, "store", zos);

                // 3b. Zip the consumer offsets directory
                Path offsetsDir = dataDir.resolve("__consumer_offsets");
                zipDirectory(offsetsDir, "__consumer_offsets", zos);

            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        logger.info("Created snapshot {} (size: {} bytes, took: {} ms)", 
                zipFile.getFileName(), Files.size(zipFile), System.currentTimeMillis() - startMs);

        cleanupOldSnapshots(snapshotsDir, zipFile);
        
        return zipFile;
    }

    /**
     * Recursively zips all files in a source directory, maintaining relative paths.
     */
    private void zipDirectory(Path sourceDir, String baseName, ZipOutputStream zos) throws IOException {
        if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
            return;
        }

        try (Stream<Path> paths = Files.walk(sourceDir)) {
            paths.filter(path -> !Files.isDirectory(path))
                 .forEach(path -> {
                     try {
                         // Convert Windows paths to Unix-style for zip compatibility
                         String relative = sourceDir.relativize(path).toString().replace('\\', '/');
                         String zipEntryName = baseName + "/" + relative;
                         
                         zos.putNextEntry(new ZipEntry(zipEntryName));
                         Files.copy(path, zos);
                         zos.closeEntry();
                     } catch (IOException e) {
                         throw new UncheckedIOException("Failed to zip file: " + path, e);
                     }
                 });
        }
    }

    /**
     * Keeps only the 2 most recent snapshots, deleting older ones to save disk space.
     */
    private void cleanupOldSnapshots(Path snapshotsDir, Path keepFile) {
        try (Stream<Path> stream = Files.list(snapshotsDir)) {
            stream.filter(p -> p.toString().endsWith(".zip"))
                  .filter(p -> !p.equals(keepFile))
                  .sorted((p1, p2) -> {
                      try {
                          return Files.getLastModifiedTime(p2).compareTo(Files.getLastModifiedTime(p1));
                      } catch (IOException e) {
                          return 0;
                      }
                  })
                  .skip(1) // Keep the most recent older one, just in case
                  .forEach(p -> {
                      try {
                          Files.deleteIfExists(p);
                          logger.debug("Deleted old snapshot: {}", p.getFileName());
                      } catch (IOException e) {
                          logger.warn("Failed to delete old snapshot: {}", p.getFileName());
                      }
                  });
        } catch (IOException e) {
            logger.warn("Failed to cleanup old snapshots", e);
        }
    }

    /**
     * Unzips the snapshot into the data directory, replacing the current state.
     */
    public void restoreSnapshot(Path zipFile) throws IOException {
        logger.info("Restoring state from snapshot zip: {}", zipFile);

        // Delete existing directories first
        Path storeDir = dataDir.resolve("store");
        Path offsetsDir = dataDir.resolve("__consumer_offsets");
        deleteDirectory(storeDir);
        deleteDirectory(offsetsDir);

        // Unzip
        try (java.io.InputStream is = Files.newInputStream(zipFile);
             ZipInputStream zis = new ZipInputStream(is)) {
            
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                Path targetDirAbs = dataDir.toAbsolutePath().normalize();
                Path resolvedPath = dataDir.resolve(entry.getName()).toAbsolutePath().normalize();
                
                // Security check to prevent ZipSlip
                if (!resolvedPath.startsWith(targetDirAbs)) {
                    throw new IOException("Zip entry is outside of target dir: " + entry.getName());
                }

                if (entry.isDirectory()) {
                    Files.createDirectories(resolvedPath);
                } else {
                    Files.createDirectories(resolvedPath.getParent());
                    Files.copy(zis, resolvedPath, StandardCopyOption.REPLACE_EXISTING);
                }
                zis.closeEntry();
            }
        }
    }

    private void deleteDirectory(Path path) throws IOException {
        if (!Files.exists(path)) return;
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.delete(p); } catch (IOException ignored) {}
                });
        }
    }
}
