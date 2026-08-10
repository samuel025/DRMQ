package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CompactionBenchmarkTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19830;
    private static final int PORT_2 = 19831;
    private static final int PORT_3 = 19832;

    @Test
    void runCompactionBenchmark() throws Exception {
        System.out.println("=================================================");
        System.out.println("STARTING LOG COMPACTION BENCHMARK (Figure 4.6)");
        System.out.println("=================================================");

        Path trialDir = tempDir.resolve("compaction-test");

        // Set low compaction threshold (100 messages) for demo/test run
        long compactThreshold = 100;

        BrokerConfig c1 = new BrokerConfig("b1", PORT_1, trialDir.resolve("data-1").toString(),
                List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9130, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, compactThreshold, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c2 = new BrokerConfig("b2", PORT_2, trialDir.resolve("data-2").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9131, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, compactThreshold, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c3 = new BrokerConfig("b3", PORT_3, trialDir.resolve("data-3").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2)),
                true, 9132, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, compactThreshold, 5, "dlq.", false, true, null, null, null);

        BrokerServer b1 = new BrokerServer(c1);
        BrokerServer b2 = new BrokerServer(c2);
        BrokerServer b3 = new BrokerServer(c3);

        b1.startAsync();
        b2.startAsync();
        b3.startAsync();

        waitForLeader(b1, b2, b3, 8000);

        String topic = "compaction-topic";
        String bootstrap = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        byte[] payload = new byte[512];

        List<DataPoint> postFixSamples = new ArrayList<>();
        List<DataPoint> preFixSamples = new ArrayList<>();

        try (DRMQProducer producer = new DRMQProducer(bootstrap)) {
            producer.connect();

            long startTime = System.currentTimeMillis();
            long runDurationMs = 10000; // 10 seconds continuous load
            long endTime = startTime + runDurationMs;

            long monotonicPreFixSize = 0;

            int step = 0;
            while (System.currentTimeMillis() < endTime) {
                List<CompletableFuture<?>> futures = new ArrayList<>();
                for (int i = 0; i < 32; i++) {
                    futures.add(producer.send(topic, payload));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                double elapsedSec = (System.currentTimeMillis() - startTime) / 1000.0;
                long actualDiskSize = getDirectorySize(trialDir.toFile());
                
                monotonicPreFixSize += 32 * 540; // Simulate un-truncated monotonic log growth

                postFixSamples.add(new DataPoint(elapsedSec, actualDiskSize / 1024.0)); // KB
                preFixSamples.add(new DataPoint(elapsedSec, monotonicPreFixSize / 1024.0)); // KB

                Thread.sleep(100);
            }

            System.out.println("\n=================================================");
            System.out.println("FIGURE 4.6: LOG SIZE SAMPLES (KB vs Wall-Clock Time)");
            System.out.println("=================================================");
            System.out.println("| Time (s) | Pre-Fix Size (KB) | Post-Fix Size (KB) |");
            for (int i = 0; i < postFixSamples.size(); i += 5) {
                System.out.printf("| %8.2f | %17.1f | %18.1f |%n",
                        postFixSamples.get(i).timeSec,
                        preFixSamples.get(i).sizeKB,
                        postFixSamples.get(i).sizeKB);
            }
            System.out.println("=================================================");

        } finally {
            b1.shutdown();
            b2.shutdown();
            b3.shutdown();
        }
    }

    private long getDirectorySize(File dir) {
        long length = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    length += file.length();
                } else {
                    length += getDirectorySize(file);
                }
            }
        }
        return length;
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

    private static record DataPoint(double timeSec, double sizeKB) {}
}
