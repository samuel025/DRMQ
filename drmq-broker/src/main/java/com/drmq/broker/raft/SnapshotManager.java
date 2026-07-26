package com.drmq.broker.raft;

import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.broker.ClusterEventBuffer;
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
     * Streams the incremental segments directly to the follower instead of zipping the entire MessageStore.
     */
    public void streamIncrementalSegments(
            java.util.Map<String, Long> followerOffsets,
            long snapshotIndex,
            long snapshotTerm,
            String nodeId,
            com.drmq.broker.BrokerConfig.PeerAddress peer,
            java.util.function.Function<com.drmq.protocol.DRMQProtocol.IncrementalSnapshotChunk, com.drmq.protocol.DRMQProtocol.IncrementalSnapshotChunkResponse> chunkHandler,
            java.util.function.Function<com.drmq.protocol.DRMQProtocol.IncrementalSnapshotDoneRequest, com.drmq.protocol.DRMQProtocol.IncrementalSnapshotDoneResponse> doneHandler) {

        logger.info("[{}] Starting Tier 2 Incremental Sync for peer {} at Raft index {}", nodeId, peer.id(), snapshotIndex);

        try {
            for (String topic : messageStore.getTopics()) {
                long followerOffset = followerOffsets.getOrDefault(topic, -1L);
                java.util.List<Path> segmentsToStream = messageStore.getSegmentsForSync(topic, followerOffset);

                for (Path segmentPath : segmentsToStream) {
                    streamFile(segmentPath, topic, snapshotTerm, nodeId, chunkHandler);
                }
            }

            // After all topic files are streamed, send the Done request
            com.drmq.protocol.DRMQProtocol.IncrementalSnapshotDoneRequest doneReq = com.drmq.protocol.DRMQProtocol.IncrementalSnapshotDoneRequest.newBuilder()
                    .setTerm(snapshotTerm)
                    .setLeaderId(nodeId)
                    .setLastIncludedIndex(snapshotIndex)
                    .setLastIncludedTerm(snapshotTerm)
                    .build();

            com.drmq.protocol.DRMQProtocol.IncrementalSnapshotDoneResponse doneResp = doneHandler.apply(doneReq);
            if (doneResp == null || !doneResp.getSuccess()) {
                throw new IOException("Follower " + peer.id() + " rejected IncrementalSnapshotDoneRequest");
            }
            logger.info("[{}] Tier 2 Incremental Sync completed successfully for peer {}", nodeId, peer.id());

        } catch (Exception e) {
            logger.error("[{}] Tier 2 Incremental Sync failed for peer {}", nodeId, peer.id(), e);
            throw new RuntimeException(e);
        }
    }

    private void streamFile(Path filePath, String topic, long term, String leaderId,
                            java.util.function.Function<com.drmq.protocol.DRMQProtocol.IncrementalSnapshotChunk, com.drmq.protocol.DRMQProtocol.IncrementalSnapshotChunkResponse> chunkHandler) throws IOException {
        
        if (!Files.exists(filePath)) return;
        
        String fileName = filePath.getFileName().toString();
        try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(filePath, StandardOpenOption.READ)) {
            long totalBytes = channel.size();
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

                com.drmq.protocol.DRMQProtocol.IncrementalSnapshotChunk chunkReq = com.drmq.protocol.DRMQProtocol.IncrementalSnapshotChunk.newBuilder()
                        .setTerm(term)
                        .setLeaderId(leaderId)
                        .setTopic(topic)
                        .setFileName(fileName)
                        .setFileOffset(offset)
                        .setData(data)
                        .setIsLastChunkForFile(isDone)
                        .build();

                com.drmq.protocol.DRMQProtocol.IncrementalSnapshotChunkResponse chunkResp = chunkHandler.apply(chunkReq);
                if (chunkResp == null || !chunkResp.getSuccess()) {
                    throw new IOException("Follower rejected file chunk for " + fileName);
                }

                if (isDone) break;
                offset += payloadSize;
            }
        }
    }
}
