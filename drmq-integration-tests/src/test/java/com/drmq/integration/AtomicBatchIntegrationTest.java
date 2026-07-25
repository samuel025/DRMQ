package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerServer;

import com.drmq.client.DRMQProducer;
import com.drmq.protocol.DRMQProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class AtomicBatchIntegrationTest {

    @TempDir
    Path tempDir;

    private BrokerServer broker;

    @BeforeEach
    void setUp() throws Exception {
        String dataDir = tempDir.resolve("node-1").toString();
        BrokerConfig config = new BrokerConfig(9092, dataDir);
        broker = new BrokerServer(config);
        
        // Start broker in a separate thread so it doesn't block the test
        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        brokerThread.start();
        
        // Wait for broker to start up and become leader (since it's a standalone node)
        Thread.sleep(2000); 
    }

    @AfterEach
    void tearDown() throws Exception {
        if (broker != null) {
            broker.shutdown();
        }
    }

    @Test
    void testAtomicBatchingAcrossTopics() throws Exception {
        try (DRMQProducer producer = new DRMQProducer("localhost:9092")) {
            producer.setLingerMs(50); // Give it time to batch
            producer.connect();

            // Send multiple atomic batches concurrently to trigger client-side batching
            int numRequests = 100;
            List<CompletableFuture<Map<String, Long>>> futures = new ArrayList<>();

            for (int i = 0; i < numRequests; i++) {
                Map<String, byte[]> atomicBatch = new HashMap<>();
                atomicBatch.put("Topic-A", ("MsgA-" + i).getBytes());
                atomicBatch.put("Topic-B", ("MsgB-" + i).getBytes());
                
                futures.add(producer.sendAtomic(atomicBatch));
            }

            // Wait for all futures to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

            // Verify the offsets are assigned correctly across the batch
            long lastOffsetA = -1;
            long lastOffsetB = -1;

            for (int i = 0; i < numRequests; i++) {
                Map<String, Long> offsets = futures.get(i).get();
                assertNotNull(offsets, "Offsets should not be null");
                assertTrue(offsets.containsKey("Topic-A"), "Should contain Topic-A offset");
                assertTrue(offsets.containsKey("Topic-B"), "Should contain Topic-B offset");

                long offsetA = offsets.get("Topic-A");
                long offsetB = offsets.get("Topic-B");

                if (i == 0) {
                    lastOffsetA = offsetA;
                    lastOffsetB = offsetB;
                } else {
                    // Since it's processed in order, each message should have a strictly increasing offset
                    assertEquals(lastOffsetA + 1, offsetA, "Topic-A offsets should be contiguous");
                    assertEquals(lastOffsetB + 1, offsetB, "Topic-B offsets should be contiguous");
                    lastOffsetA = offsetA;
                    lastOffsetB = offsetB;
                }
            }
        }
    }
}
