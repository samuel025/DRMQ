package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerServer;

import com.drmq.client.DRMQConsumer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    // Ports for the 3-node cluster
    private static final int PORT_1 = 19300;
    private static final int PORT_2 = 19301;
    private static final int PORT_3 = 19302;

    private BrokerServer broker1;
    private BrokerServer broker2;
    private BrokerServer broker3;

    private BrokerConfig clusterConfig(String nodeId, int port, String dataDirName) {
        List<BrokerConfig.PeerAddress> peers = switch (nodeId) {
            case "b1" -> List.of(
                    new BrokerConfig.PeerAddress("b2", "localhost", PORT_2),
                    new BrokerConfig.PeerAddress("b3", "localhost", PORT_3));
            case "b2" -> List.of(
                    new BrokerConfig.PeerAddress("b1", "localhost", PORT_1),
                    new BrokerConfig.PeerAddress("b3", "localhost", PORT_3));
            case "b3" -> List.of(
                    new BrokerConfig.PeerAddress("b1", "localhost", PORT_1),
                    new BrokerConfig.PeerAddress("b2", "localhost", PORT_2));
            default -> throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        };
        return new BrokerConfig(nodeId, port, tempDir.resolve(dataDirName).toString(), peers);
    }

    @BeforeEach
    void setUp() throws Exception {
        broker1 = new BrokerServer(clusterConfig("b1", PORT_1, "node-1"));
        broker2 = new BrokerServer(clusterConfig("b2", PORT_2, "node-2"));
        broker3 = new BrokerServer(clusterConfig("b3", PORT_3, "node-3"));

        broker1.startAsync();
        broker2.startAsync();
        broker3.startAsync();

        waitForLeader(10000);
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leaderCount = 0;
            if (broker1 != null && broker1.getRaftNode() != null && broker1.getRaftNode().isLeader()) leaderCount++;
            if (broker2 != null && broker2.getRaftNode() != null && broker2.getRaftNode().isLeader()) leaderCount++;
            if (broker3 != null && broker3.getRaftNode() != null && broker3.getRaftNode().isLeader()) leaderCount++;
            if (leaderCount == 1) return;
            Thread.sleep(100);
        }
        throw new IllegalStateException("Leader not elected within " + timeoutMs + "ms");
    }

    private BrokerServer findLeader() {
        if (broker1 != null && broker1.getRaftNode() != null && broker1.getRaftNode().isLeader()) return broker1;
        if (broker2 != null && broker2.getRaftNode() != null && broker2.getRaftNode().isLeader()) return broker2;
        if (broker3 != null && broker3.getRaftNode() != null && broker3.getRaftNode().isLeader()) return broker3;
        return null;
    }

    @AfterEach
    void tearDown() throws Exception {
        if (broker1 != null) broker1.shutdown();
        if (broker2 != null) broker2.shutdown();
        if (broker3 != null) broker3.shutdown();
    }

    @Test
    void testAtomicBatchingAcrossTopics() throws Exception {
        BrokerServer leader = findLeader();
        assertNotNull(leader, "Cluster must have an elected leader");

        try (DRMQProducer producer = new DRMQProducer("localhost", leader.getPort())) {
            producer.setLingerMs(50); // Give it time to batch
            producer.connect();

            int numRequests = 100;
            List<CompletableFuture<Map<String, Long>>> futures = new ArrayList<>();

            for (int i = 0; i < numRequests; i++) {
                Map<String, byte[]> atomicBatch = new HashMap<>();
                atomicBatch.put("Topic-A", ("MsgA-" + i).getBytes());
                atomicBatch.put("Topic-B", ("MsgB-" + i).getBytes());
                
                futures.add(producer.sendAtomic(atomicBatch));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(15, TimeUnit.SECONDS);

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
                    assertEquals(lastOffsetA + 1, offsetA, "Topic-A offsets should be contiguous");
                    assertEquals(lastOffsetB + 1, offsetB, "Topic-B offsets should be contiguous");
                    lastOffsetA = offsetA;
                    lastOffsetB = offsetB;
                }
            }

            long deadline = System.currentTimeMillis() + 5000;
            for (BrokerServer b : List.of(broker1, broker2, broker3)) {
                while (System.currentTimeMillis() < deadline) {
                    if (b.getMessageStore().getMessageCount("Topic-A") == numRequests &&
                        b.getMessageStore().getMessageCount("Topic-B") == numRequests) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertEquals(numRequests, b.getMessageStore().getMessageCount("Topic-A"),
                        "Broker on port " + b.getPort() + " should have all Topic-A messages");
                assertEquals(numRequests, b.getMessageStore().getMessageCount("Topic-B"),
                        "Broker on port " + b.getPort() + " should have all Topic-B messages");
            }

            long firstOffsetA = futures.get(0).get().get("Topic-A");
            try (DRMQConsumer consumerA = new DRMQConsumer("localhost", leader.getPort(), "atomic-consumer-group-a")) {
                consumerA.connect();
                consumerA.subscribe("Topic-A");

                List<DRMQConsumer.ConsumedMessage> consumedA = new ArrayList<>();
                long pollDeadline = System.currentTimeMillis() + 10000;
                while (consumedA.size() < numRequests && System.currentTimeMillis() < pollDeadline) {
                    List<DRMQConsumer.ConsumedMessage> batch = consumerA.poll(numRequests - consumedA.size());
                    if (batch != null && !batch.isEmpty()) {
                        consumedA.addAll(batch);
                    }
                }

                assertEquals(numRequests, consumedA.size(), "Should have consumed all Topic-A messages");
                for (int i = 0; i < numRequests; i++) {
                    assertEquals("MsgA-" + i, consumedA.get(i).payloadAsString(),
                            "Topic-A message payload mismatch at index " + i);
                    assertEquals(firstOffsetA + i, consumedA.get(i).offset(),
                            "Topic-A message offset mismatch at index " + i);
                }
            }

            long firstOffsetB = futures.get(0).get().get("Topic-B");
            try (DRMQConsumer consumerB = new DRMQConsumer("localhost", leader.getPort(), "atomic-consumer-group-b")) {
                consumerB.connect();
                consumerB.subscribe("Topic-B");

                List<DRMQConsumer.ConsumedMessage> consumedB = new ArrayList<>();
                long pollDeadline = System.currentTimeMillis() + 10000;
                while (consumedB.size() < numRequests && System.currentTimeMillis() < pollDeadline) {
                    List<DRMQConsumer.ConsumedMessage> batch = consumerB.poll(numRequests - consumedB.size());
                    if (batch != null && !batch.isEmpty()) {
                        consumedB.addAll(batch);
                    }
                }

                assertEquals(numRequests, consumedB.size(), "Should have consumed all Topic-B messages");
                for (int i = 0; i < numRequests; i++) {
                    assertEquals("MsgB-" + i, consumedB.get(i).payloadAsString(),
                            "Topic-B message payload mismatch at index " + i);
                    assertEquals(firstOffsetB + i, consumedB.get(i).offset(),
                            "Topic-B message offset mismatch at index " + i);
                }
            }
        }
    }
}
