package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import com.drmq.client.DRMQProducer.SendResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-threaded sequential stress test (1 message per round trip, batching disabled).
 */
public class SingleThreadStressTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19850;
    private static final int PORT_2 = 19851;
    private static final int PORT_3 = 19852;

    @Test
    void runSingleThreadStressTest() throws Exception {
        System.out.println("=================================================");
        System.out.println("RUNNING SINGLE-THREAD SEQUENTIAL STRESS TEST");
        System.out.println("(1 Thread • 1 Msg / Round Trip • No Batching)");
        System.out.println("=================================================");

        Path trialDir = tempDir.resolve("single-thread-test");

        BrokerConfig c1 = new BrokerConfig("b1", PORT_1, trialDir.resolve("data-1").toString(),
                List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9150, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c2 = new BrokerConfig("b2", PORT_2, trialDir.resolve("data-2").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9151, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c3 = new BrokerConfig("b3", PORT_3, trialDir.resolve("data-3").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2)),
                true, 9152, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);

        BrokerServer b1 = new BrokerServer(c1);
        BrokerServer b2 = new BrokerServer(c2);
        BrokerServer b3 = new BrokerServer(c3);

        try {
            b1.startAsync();
            b2.startAsync();
            b3.startAsync();

            assertTrue(waitForLeader(b1, b2, b3, 8000), "Raft cluster failed to elect a leader");

            String topic = "single-thread-sync-topic";
            String bootstrap = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
            byte[] payload = new byte[512];
            Arrays.fill(payload, (byte) 'A');

            int recordCount = 200;
            List<Double> latencies = new ArrayList<>();
            int successCount = 0;

            try (DRMQProducer producer = new DRMQProducer(bootstrap)) {
                producer.setBatchSizeBytes(1);
                producer.setLingerMs(0);
                producer.setMaxInflight(1);
                producer.connect();

                long startTime = System.currentTimeMillis();

                for (int i = 0; i < recordCount; i++) {
                    long t0 = System.nanoTime();
                    SendResult result = producer.send(topic, payload).join();
                    long t1 = System.nanoTime();

                    if (result != null && result.isSuccess()) {
                        successCount++;
                        latencies.add((t1 - t0) / 1_000_000.0);
                    }
                }

                long totalTimeMs = System.currentTimeMillis() - startTime;
                double totalSec = totalTimeMs / 1000.0;
                double recordsPerSec = successCount / Math.max(0.001, totalSec);

                latencies.sort(Double::compareTo);
                double p50 = getPercentile(latencies, 50);
                double p95 = getPercentile(latencies, 95);
                double p99 = getPercentile(latencies, 99);
                double avg = latencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

                System.out.println("\n-------------------------------------------------");
                System.out.println("SINGLE THREAD SEQUENTIAL TEST RESULTS:");
                System.out.printf("  Total Sent   : %d / %d records%n", successCount, recordCount);
                System.out.printf("  Elapsed Time : %.3f s%n", totalSec);
                System.out.printf("  Throughput   : %.2f msgs/sec%n", recordsPerSec);
                System.out.printf("  Latency Avg  : %.2f ms%n", avg);
                System.out.printf("  Latency P50  : %.2f ms%n", p50);
                System.out.printf("  Latency P95  : %.2f ms%n", p95);
                System.out.printf("  Latency P99  : %.2f ms%n", p99);
                System.out.println("-------------------------------------------------\n");

                assertEquals(recordCount, successCount, "All messages should be synchronously acked");
            }
        } finally {
            b1.shutdown();
            b2.shutdown();
            b3.shutdown();
        }
    }

    private static double getPercentile(List<Double> sorted, double percentile) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
    }

    private boolean waitForLeader(BrokerServer b1, BrokerServer b2, BrokerServer b3, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leaderCount = 0;
            if (b1 != null && b1.getRaftNode() != null && b1.getRaftNode().isLeader()) leaderCount++;
            if (b2 != null && b2.getRaftNode() != null && b2.getRaftNode().isLeader()) leaderCount++;
            if (b3 != null && b3.getRaftNode() != null && b3.getRaftNode().isLeader()) leaderCount++;
            if (leaderCount == 1) return true;
            Thread.sleep(20);
        }
        return false;
    }
}
