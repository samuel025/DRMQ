package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WALAppendLatencyBenchmarkTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19700;
    private static final int PORT_2 = 19701;
    private static final int PORT_3 = 19702;

    @Test
    void runWALBenchmark() throws Exception {
        System.out.println("=================================================");
        System.out.println("STARTING WAL APPEND LATENCY BENCHMARK (Table 4.5)");
        System.out.println("=================================================");

        // Config 1: fsync = true, 1 msg / append
        LatencyResult r1 = runTrial(true, 1, 300);

        // Config 2: fsync = true, 16KB / batch (approx 16 messages per batch)
        LatencyResult r2 = runTrial(true, 16, 300);

        // Config 3: fsync = false, 1 msg / append
        LatencyResult r3 = runTrial(false, 1, 300);

        // Config 4: fsync = false, 16KB / batch
        LatencyResult r4 = runTrial(false, 16, 300);

        System.out.println("\n=================================================");
        System.out.println("TABLE 4.5: WAL APPEND LATENCY RESULTS");
        System.out.println("=================================================");
        System.out.printf("| Configuration            | Batch Size         | p50 (ms) | p99 (ms) |%n");
        System.out.printf("| raft.fsync.enabled = true | 1 message / append | %8.3f | %8.3f |%n", r1.p50, r1.p99);
        System.out.printf("| raft.fsync.enabled = true | 16 KB batch (def)  | %8.3f | %8.3f |%n", r2.p50, r2.p99);
        System.out.printf("| raft.fsync.enabled = false| 1 message / append | %8.3f | %8.3f |%n", r3.p50, r3.p99);
        System.out.printf("| raft.fsync.enabled = false| 16 KB batch        | %8.3f | %8.3f |%n", r4.p50, r4.p99);
        System.out.println("=================================================");
    }

    private LatencyResult runTrial(boolean fsyncEnabled, int batchSize, int messageCount) throws Exception {
        Path trialDir = tempDir.resolve("wal-" + fsyncEnabled + "-" + batchSize);

        BrokerConfig c1 = new BrokerConfig("b1", PORT_1, trialDir.resolve("data-1").toString(),
                List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9099, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, fsyncEnabled, null, null, null);
        BrokerConfig c2 = new BrokerConfig("b2", PORT_2, trialDir.resolve("data-2").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9100, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, fsyncEnabled, null, null, null);
        BrokerConfig c3 = new BrokerConfig("b3", PORT_3, trialDir.resolve("data-3").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2)),
                true, 9101, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, fsyncEnabled, null, null, null);

        BrokerServer b1 = new BrokerServer(c1);
        BrokerServer b2 = new BrokerServer(c2);
        BrokerServer b3 = new BrokerServer(c3);

        b1.startAsync();
        b2.startAsync();
        b3.startAsync();

        waitForLeader(b1, b2, b3, 8000);

        byte[] payload = new byte[1024]; // 1KB message
        List<Double> sampleLatencies = new ArrayList<>();

        String bootstrap = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        try (DRMQProducer producer = new DRMQProducer(bootstrap)) {
            producer.connect();

            // Warmup
            for (int i = 0; i < 20; i++) {
                producer.send("wal-topic", payload).join();
            }

            // Benchmark appends
            for (int i = 0; i < messageCount; i++) {
                long start = System.nanoTime();
                producer.send("wal-topic", payload).join();
                long elapsed = System.nanoTime() - start;
                sampleLatencies.add(elapsed / 1_000_000.0); // convert to ms
            }
        } finally {
            b1.shutdown();
            b2.shutdown();
            b3.shutdown();
        }

        Collections.sort(sampleLatencies);
        double p50 = getPercentile(sampleLatencies, 50);
        double p99 = getPercentile(sampleLatencies, 99);
        return new LatencyResult(p50, p99);
    }

    private static double getPercentile(List<Double> sorted, double percentile) {
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

    private static record LatencyResult(double p50, double p99) {}
}
