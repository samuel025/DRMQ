package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Benchmark for evaluating Cross-Topic Atomicity performance.
 * This test spins up a 3-node cluster and compares the latency of writing to N topics
 * atomically (1 Raft entry) vs sequentially (N Raft entries).
 */
public class AtomicLatencyBenchmarkTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19300;
    private static final int PORT_2 = 19301;
    private static final int PORT_3 = 19302;

    private BrokerServer broker1;
    private BrokerServer broker2;
    private BrokerServer broker3;

    private BrokerConfig clusterConfig(String nodeId, int port, String dataDirName) {
        List<PeerAddress> peers = switch (nodeId) {
            case "b1" -> List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3));
            case "b2" -> List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3));
            case "b3" -> List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2));
            default -> throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        };
        return new BrokerConfig(nodeId, port, tempDir.resolve(dataDirName).toString(), peers);
    }

    @BeforeEach
    void setUp() throws Exception {
        broker1 = new BrokerServer(clusterConfig("b1", PORT_1, "data-1"));
        broker2 = new BrokerServer(clusterConfig("b2", PORT_2, "data-2"));
        broker3 = new BrokerServer(clusterConfig("b3", PORT_3, "data-3"));

        broker1.startAsync();
        broker2.startAsync();
        broker3.startAsync();

        // Wait for leader election
        waitForLeader(3000);
    }

    @AfterEach
    void tearDown() {
        if (broker1 != null) broker1.shutdown();
        if (broker2 != null) broker2.shutdown();
        if (broker3 != null) broker3.shutdown();
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leaderCount = 0;
            if (broker1.getRaftNode() != null && broker1.getRaftNode().isLeader()) leaderCount++;
            if (broker2.getRaftNode() != null && broker2.getRaftNode().isLeader()) leaderCount++;
            if (broker3.getRaftNode() != null && broker3.getRaftNode().isLeader()) leaderCount++;
            if (leaderCount == 1) return;
            Thread.sleep(100);
        }
    }

    private int getLeaderPort() {
        if (broker1.getRaftNode() != null && broker1.getRaftNode().isLeader()) return PORT_1;
        if (broker2.getRaftNode() != null && broker2.getRaftNode().isLeader()) return PORT_2;
        if (broker3.getRaftNode() != null && broker3.getRaftNode().isLeader()) return PORT_3;
        throw new IllegalStateException("No leader found");
    }

    @Test
    public void benchmarkAtomicVsSequentialWrites() throws Exception {
        int leaderPort = getLeaderPort();
        int iterations = 1000;
        int warmup = 200;
        byte[] payload = new byte[256];

        System.out.println("=================================================");
        System.out.println("BENCHMARK: Cross-Topic Atomicity vs Sequential");
        System.out.println("=================================================");

        String bootstrapServers = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        try (DRMQProducer producer = new DRMQProducer(bootstrapServers)) {
            producer.connect();

            // Run for 2 topics, then 4 topics, then 8 topics
            int[] topicCounts = {2, 4, 8};

            for (int numTopics : topicCounts) {
                System.out.println("\nTesting with " + numTopics + " topics:");

                Map<String, byte[]> atomicBatch = new LinkedHashMap<>();
                for (int t = 0; t < numTopics; t++) atomicBatch.put("atomic-topic-" + t, payload);

                // --- ATOMIC BENCHMARK ---
                // Warmup
                for (int i = 0; i < warmup; i++) producer.sendAtomic(atomicBatch).join();

                long atomicTotalTimeNs = 0;
                for (int i = 0; i < iterations; i++) {
                    long start = System.nanoTime();
                    producer.sendAtomic(atomicBatch).join();
                    atomicTotalTimeNs += (System.nanoTime() - start);
                }
                double atomicAvgMs = (atomicTotalTimeNs / (double) iterations) / 1_000_000.0;
                double atomicTps = iterations / (atomicTotalTimeNs / 1_000_000_000.0);

                // --- SEQUENTIAL BENCHMARK ---
                // Warmup
                for (int i = 0; i < warmup; i++) {
                    for (int t = 0; t < numTopics; t++) {
                        producer.send("seq-topic-" + t, payload).join();
                    }
                }

                long seqTotalTimeNs = 0;
                for (int i = 0; i < iterations; i++) {
                    long start = System.nanoTime();
                    for (int t = 0; t < numTopics; t++) {
                        producer.send("seq-topic-" + t, payload).join();
                    }
                    seqTotalTimeNs += (System.nanoTime() - start);
                }
                double seqAvgMs = (seqTotalTimeNs / (double) iterations) / 1_000_000.0;
                double seqTps = iterations / (seqTotalTimeNs / 1_000_000_000.0);

                System.out.printf("Atomic Write (1 Raft roundtrip)  : Avg %.2f ms/op | %.2f ops/sec%n", atomicAvgMs, atomicTps);
                System.out.printf("Sequential Write (%d Raft trips)  : Avg %.2f ms/op | %.2f ops/sec%n", numTopics, seqAvgMs, seqTps);
                System.out.printf("SPEEDUP                          : %.2fx faster%n", seqAvgMs / atomicAvgMs);
            }
        }
        System.out.println("=================================================");
    }
}
