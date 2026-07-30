package com.drmq.broker.persistence;

import com.drmq.broker.BrokerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InfinityLogResolverTest {
    
    @TempDir
    Path tempDir;

    private BrokerConfig config;
    private LogManager logManager;

    @BeforeEach
    void setUp() throws Exception {
        config = BrokerConfig.fromArgs(new String[]{
            "--id", "node-1",
            "--port", "9092",
            "--data-dir", tempDir.toString(),
            "--s3-archive-bucket", "test-archive-bucket"
        });
        logManager = new LogManager(config.getDataDir());
    }

    @AfterEach
    void tearDown() throws Exception {
        logManager.close();
    }

    @Test
    void testResolveMissingSegment() throws Exception {
        S3Client fakeS3 = new S3Client() {
            @Override
            public String serviceName() {
                return "s3";
            }

            @Override
            public void close() {
            }

            @Override
            public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
                List<S3Object> contents = new ArrayList<>();
                contents.add(S3Object.builder().key("archive/node-1/test-topic/00000000000000000000.log").build());
                contents.add(S3Object.builder().key("archive/node-1/test-topic/00000000000000001000.log").build());
                contents.add(S3Object.builder().key("archive/node-1/test-topic/00000000000000002000.log").build());

                return ListObjectsV2Response.builder()
                        .contents(contents)
                        .isTruncated(false)
                        .build();
            }

            @Override
            public GetObjectResponse getObject(GetObjectRequest getObjectRequest, Path destinationPath) {
                try {
                    Files.writeString(destinationPath, "mock segment content");
                    return GetObjectResponse.builder().build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        InfinityLogResolver resolver = new InfinityLogResolver(fakeS3, config, logManager);

        // Try to resolve offset 1500. It should pick the segment with baseOffset 1000
        LogSegment resolved = resolver.resolveMissingSegment("test-topic", 1500L);
        
        assertNotNull(resolved, "Resolver should successfully return a segment");
        assertEquals(1000L, resolved.getBaseOffset(), "Should fetch the segment that contains the offset 1500 (base = 1000)");
        
        // Ensure it's correctly cached in the LogManager memory
        assertNotNull(logManager.getAllSegments().get("test-topic"), "Topic map should exist");
        assertNotNull(logManager.getAllSegments().get("test-topic").get(1000L), "Segment should be loaded into LogManager");
    }
}
