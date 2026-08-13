package com.drmq.broker.raft;

import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.broker.ClusterEventBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.drmq.protocol.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.*;


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
     * Atomically activates a fully downloaded snapshot. If a crash occurs during activation,
     * this method can be re-run on startup to idempotently complete the process.
     */
    public static void activateSnapshot(Path dataDir) throws IOException {
        Path tempSnapshotDir = dataDir.resolve(".snapshot-tmp");
        Path markerFile = dataDir.resolve(".snapshot-activate");

        // If the marker exists, we must resume a crashed activation even if tempSnapshotDir is gone 
        // (though if temp is gone, we might just be cleaning up .old files).
        // If marker doesn't exist, and tempSnapshotDir doesn't exist, nothing to do.
        if (!Files.exists(markerFile) && !Files.exists(tempSnapshotDir)) {
            return;
        }

        // Write marker if it doesn't exist to indicate we are committed to activating
        if (!Files.exists(markerFile)) {
            Files.writeString(markerFile, "active");
        }

        if (Files.exists(tempSnapshotDir)) {
            try (java.util.stream.Stream<Path> tempDirs = Files.list(tempSnapshotDir)) {
                tempDirs.forEach(topicDir -> {
                    try {
                        String topicName = topicDir.getFileName().toString();
                        Path targetTopicDir = dataDir.resolve(topicName);
                        if (!Files.exists(targetTopicDir)) {
                            Files.createDirectories(targetTopicDir);
                        }

                        // Move individual segment files from .snapshot-tmp/topic into active topic dir
                        try (java.util.stream.Stream<Path> chunkFiles = Files.list(topicDir)) {
                            chunkFiles.forEach(chunkFile -> {
                                try {
                                    Path targetFile = targetTopicDir.resolve(chunkFile.getFileName().toString());
                                    Files.move(chunkFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                        }
                    } catch (IOException e) {
                        logger.error("Error activating snapshot for topic {}", topicDir, e);
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }

        // 3. Cleanup temp dir and marker
        if (Files.exists(tempSnapshotDir)) {
            // Delete now empty topic subdirs
            try (java.util.stream.Stream<Path> tempDirs = Files.list(tempSnapshotDir)) {
                tempDirs.forEach(d -> {
                    try { Files.delete(d); } catch (Exception ignored) {}
                });
            }
            Files.delete(tempSnapshotDir);
        }
        Files.deleteIfExists(markerFile);
    }

    /**
     * Streams the incremental segments directly to the follower instead of zipping the entire MessageStore.
     */
    public static class SnapshotManifest {
        public final long snapshotIndex;
        public final long snapshotTerm;
        public final java.util.Map<String, Long> offsetState;
        public final java.util.Map<String, Long> fileManifest;
        public final java.util.List<Path> allSegmentsToStream;
        public final java.util.Map<Path, String> pathToTopic;

        public SnapshotManifest(long snapshotIndex, long snapshotTerm, java.util.Map<String, Long> offsetState,
                                java.util.Map<String, Long> fileManifest, java.util.List<Path> allSegmentsToStream,
                                java.util.Map<Path, String> pathToTopic) {
            this.snapshotIndex = snapshotIndex;
            this.snapshotTerm = snapshotTerm;
            this.offsetState = offsetState;
            this.fileManifest = fileManifest;
            this.allSegmentsToStream = allSegmentsToStream;
            this.pathToTopic = pathToTopic;
        }
    }

    public SnapshotManifest freezeSnapshot(long snapshotIndex, long snapshotTerm, java.util.Map<String, Long> followerOffsets) {
        java.util.Map<String, Long> offsetState = offsetManager != null ? offsetManager.getAllOffsets() : java.util.Collections.emptyMap();
        java.util.Map<String, Long> fileManifest = new java.util.HashMap<>();
        java.util.List<Path> allSegmentsToStream = new java.util.ArrayList<>();
        java.util.Map<Path, String> pathToTopic = new java.util.HashMap<>();

        messageStore.lockForSnapshot(() -> {
            try {
                for (String topic : messageStore.getTopics()) {
                    long followerOffset = followerOffsets.getOrDefault(topic, -1L);
                    java.util.List<Path> segments = messageStore.getSegmentsForSync(topic, followerOffset);
                    for (Path segmentPath : segments) {
                        allSegmentsToStream.add(segmentPath);
                        pathToTopic.put(segmentPath, topic);
                        fileManifest.put(topic + "/" + segmentPath.getFileName().toString(), Files.size(segmentPath));
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        return new SnapshotManifest(snapshotIndex, snapshotTerm, offsetState, fileManifest, allSegmentsToStream, pathToTopic);
    }

    /**
     * Streams the frozen snapshot segments directly to the follower.
     */
    public void streamIncrementalSegments(
            SnapshotManifest manifest,
            String nodeId,
            com.drmq.broker.BrokerConfig.PeerAddress peer,
            java.util.function.Function<IncrementalSnapshotChunk,IncrementalSnapshotChunkResponse> chunkHandler,
            java.util.function.Function<IncrementalSnapshotDoneRequest, IncrementalSnapshotDoneResponse> doneHandler) {

        logger.info("[{}] Starting Incremental State Sync for peer {} at Raft index {}", nodeId, peer.id(), manifest.snapshotIndex);

        try {

            for (Path segmentPath : manifest.allSegmentsToStream) {
                String topic = manifest.pathToTopic.get(segmentPath);
                long exactLength = manifest.fileManifest.get(topic + "/" + segmentPath.getFileName().toString());
                streamFile(segmentPath, topic, manifest.snapshotTerm, nodeId, exactLength, chunkHandler);
            }

            // After all topic files are streamed, send the Done request with the manifest
            com.drmq.protocol.IncrementalSnapshotDoneRequest doneReq = com.drmq.protocol.IncrementalSnapshotDoneRequest.newBuilder()
                    .setTerm(manifest.snapshotTerm)
                    .setLeaderId(nodeId)
                    .setLastIncludedIndex(manifest.snapshotIndex)
                    .setLastIncludedTerm(manifest.snapshotTerm)
                    .putAllOffsetManagerState(manifest.offsetState)
                    .putAllFileManifest(manifest.fileManifest)
                    .build();

            com.drmq.protocol.IncrementalSnapshotDoneResponse doneResp = doneHandler.apply(doneReq);
            if (doneResp == null || !doneResp.getSuccess()) {
                throw new IOException("Follower " + peer.id() + " rejected IncrementalSnapshotDoneRequest");
            }
            logger.info("[{}] Incremental State Sync completed successfully for peer {}", nodeId, peer.id());

        } catch (Exception e) {
            logger.error("[{}] Incremental State Sync failed for peer {}", nodeId, peer.id(), e);
            throw new RuntimeException(e);
        }
    }

    private void streamFile(Path filePath, String topic, long term, String leaderId, long exactLength,
                            java.util.function.Function<com.drmq.protocol.IncrementalSnapshotChunk, com.drmq.protocol.IncrementalSnapshotChunkResponse> chunkHandler) throws IOException {
        
        if (!Files.exists(filePath) || exactLength <= 0) return;
        
        String fileName = filePath.getFileName().toString();
        try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(filePath, StandardOpenOption.READ)) {
            long totalBytes = exactLength; // Limit streaming to the exact point-in-time boundary length
            long offset = 0;
            long chunkSize = 2 * 1024 * 1024; // 2MB

            while (offset < totalBytes || totalBytes == 0) {
                long remaining = totalBytes - offset;
                long payloadSize = Math.min(chunkSize, remaining);
                boolean isDone = (offset + payloadSize >= totalBytes);

                com.google.protobuf.ByteString data;
                if (payloadSize > 0) {
                    java.nio.MappedByteBuffer mappedBuffer = channel.map(java.nio.channels.FileChannel.MapMode.READ_ONLY, offset, payloadSize);
                    data = com.google.protobuf.ByteString.copyFrom(mappedBuffer);
                } else {
                    data = com.google.protobuf.ByteString.EMPTY;
                }

                com.drmq.protocol.IncrementalSnapshotChunk chunkReq = com.drmq.protocol.IncrementalSnapshotChunk.newBuilder()
                        .setTerm(term)
                        .setLeaderId(leaderId)
                        .setTopic(topic)
                        .setFileName(fileName)
                        .setFileOffset(offset)
                        .setData(data)
                        .setIsLastChunkForFile(isDone)
                        .build();

                com.drmq.protocol.IncrementalSnapshotChunkResponse chunkResp = chunkHandler.apply(chunkReq);
                if (chunkResp == null || !chunkResp.getSuccess()) {
                    throw new IOException("Follower rejected file chunk for " + fileName);
                }

                if (isDone) break;
                offset += payloadSize;
            }
        }
    }
}
