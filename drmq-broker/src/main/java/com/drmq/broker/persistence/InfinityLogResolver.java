package com.drmq.broker.persistence;

import com.drmq.broker.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * InfinityLogResolver enables transparent tiered storage for DRMQ.
 * It automatically fetches archived log segments from S3/cloud storage back to the local disk
 * when a consumer requests a historical offset that has been deleted locally.
 */
public class InfinityLogResolver {
    private static final Logger logger = LoggerFactory.getLogger(InfinityLogResolver.class);
    
    private final S3Client s3Client;
    private final BrokerConfig config;
    private final LogManager logManager;

    public InfinityLogResolver(S3Client s3Client, BrokerConfig config, LogManager logManager) {
        this.s3Client = s3Client;
        this.config = config;
        this.logManager = logManager;
    }

    /**
     * Attempts to resolve a missing offset by fetching the appropriate segment from cloud storage.
     * @return The downloaded LogSegment, or null if it could not be found in the InfinityLog.
     */
    public LogSegment resolveMissingSegment(String topic, long targetOffset) {
        if (s3Client == null || config.getS3ArchiveBucket() == null) {
            return null; // InfinityLog not configured
        }

        String prefix = "archive/" + config.getNodeId() + "/" + topic + "/";
        
        try {
            // List all archived segments for this topic
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(config.getS3ArchiveBucket())
                .prefix(prefix)
                .build();
                
            ListObjectsV2Response listRes = s3Client.listObjectsV2(listReq);
            
            long bestBaseOffset = -1;
            String bestKey = null;

            boolean done = false;
            while (!done) {
                ListObjectsV2Response listResp = s3Client.listObjectsV2(listReq);
                
                for (S3Object obj : listResp.contents()) {
                    String key = obj.key();
                    String filename = key.substring(key.lastIndexOf('/') + 1);
                    if (filename.endsWith(".log")) {
                        try {
                            long baseOffset = Long.parseLong(filename.replace(".log", ""));
                            if (baseOffset <= targetOffset && baseOffset > bestBaseOffset) {
                                bestBaseOffset = baseOffset;
                                bestKey = key;
                            } else if (baseOffset > targetOffset) {
                                // Since S3 returns keys in lexicographical order, and we format them with zero-padding
                                // (e.g. 00000000000000000000.log), they are sorted by baseOffset.
                                // We can safely abort early once we surpass targetOffset.
                                done = true;
                                break;
                            }
                        } catch (NumberFormatException ignored) { }
                    }
                }
                
                if (done || !listRes.isTruncated()) {
                    break;
                }
                listReq = listReq.toBuilder().continuationToken(listRes.nextContinuationToken()).build();
            }

            if (bestKey != null) {
                logger.info("[InfinityLog] Found missing offset {} in cloud segment: {}", targetOffset, bestKey);
                
                Path topicDir = Paths.get(config.getDataDir()).resolve(topic);
                Path localPath = topicDir.resolve(String.format("%020d.log", bestBaseOffset));
                
                synchronized ((topic + "-" + bestBaseOffset).intern()) {
                    java.util.concurrent.ConcurrentSkipListMap<Long, LogSegment> topicMap = logManager.getAllSegments()
                            .computeIfAbsent(topic, k -> new java.util.concurrent.ConcurrentSkipListMap<>());
                            
                    if (topicMap.containsKey(bestBaseOffset)) {
                        return topicMap.get(bestBaseOffset);
                    }
                    
                    // If it already exists somehow, don't download it again
                    if (!localPath.toFile().exists()) {
                        if (!topicDir.toFile().exists()) {
                            topicDir.toFile().mkdirs();
                        }
                        Path tmpPath = topicDir.resolve(String.format("%020d.log.tmp", bestBaseOffset));
                        logger.info("[InfinityLog] Downloading segment from cloud to {}", tmpPath);
                        GetObjectRequest getReq = GetObjectRequest.builder()
                            .bucket(config.getS3ArchiveBucket())
                            .key(bestKey)
                            .build();
                            
                        s3Client.getObject(getReq, tmpPath);
                        java.nio.file.Files.move(tmpPath, localPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        logger.info("[InfinityLog] Successfully recovered segment to {}", localPath);
                    }
                    
                    // Load it into LogManager memory
                    LogSegment segment = new LogSegment(localPath, config.isLogSegmentFsync());
                    topicMap.put(bestBaseOffset, segment);
                    
                    return segment;
                }
            } else {
                logger.debug("[InfinityLog] Offset {} not found in any archived segment for topic {}", targetOffset, topic);
            }

        } catch (Exception e) {
            logger.error("[InfinityLog] Failed to resolve segment from cloud", e);
        }
        
        return null;
    }
}
