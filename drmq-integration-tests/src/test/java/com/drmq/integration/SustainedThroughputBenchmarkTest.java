package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SustainedThroughputBenchmarkTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19820;
    private static final int PORT_2 = 19821;
    private static final int PORT_3 = 19822;

    @Test
    void runThroughputBenchmark() throws Exception {
        System.out.println("=================================================");
        System.out.println("STARTING SUSTAINED THROUGHPUT BENCHMARK (Figure 4.5)");
        System.out.println("=================================================");

        int[] batchSizes = {1, 8, 32, 128};
        List<ThroughputResult> results = new ArrayList<>();

        for (int batchSize : batchSizes) {
            ThroughputResult res = runTrial(batchSize, 5); // 5 second sustained load
            results.add(res);
            System.out.printf("Batch Size %3d -> Throughput: %,8.2f msgs/sec | Total: %,d msgs%n",
                    batchSize, res.tps, res.totalMessages);
        }

        System.out.println("\n=================================================");
        System.out.println("FIGURE 4.5: SUSTAINED THROUGHPUT RESULTS");
        System.out.println("=================================================");
        System.out.println("| Batch size                    | Sustained Throughput (msgs/sec) |");
        for (int i = 0; i < batchSizes.length; i++) {
            System.out.printf("| %-29s | %31.2f |%n",
                    batchSizes[i] + (batchSizes[i] == 32 ? " messages (~16 KB default)" : " messages"),
                    results.get(i).tps);
        }
        System.out.println("=================================================");
    }

    private ThroughputResult runTrial(int batchSize, int durationSeconds) throws Exception {
        Path trialDir = tempDir.resolve("tps-batch-" + batchSize);

        BrokerConfig c1 = new BrokerConfig("b1", PORT_1, trialDir.resolve("data-1").toString(),
                List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9120, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c2 = new BrokerConfig("b2", PORT_2, trialDir.resolve("data-2").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9121, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c3 = new BrokerConfig("b3", PORT_3, trialDir.resolve("data-3").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2)),
                true, 9122, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);

        BrokerServer b1 = new BrokerServer(c1);
        BrokerServer b2 = new BrokerServer(c2);
        BrokerServer b3 = new BrokerServer(c3);

        b1.startAsync();
        b2.startAsync();
        b3.startAsync();

        waitForLeader(b1, b2, b3, 8000);

        String topic = "tps-topic-" + batchSize;
        String bootstrap = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;

        byte[] payload = new byte[512];
        AtomicLong messageCounter = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);

        try (DRMQProducer producer = new DRMQProducer(bootstrap)) {
            producer.connect();

            // Warmup phase (0.5 sec)
            for (int i = 0; i < 50; i++) {
                producer.send(topic, payload).join();
            }

            long startTime = System.nanoTime();
            long endTime = startTime + durationSeconds * 1_000_000_000L;

            List<CompletableFuture<?>> batchFutures = new ArrayList<>(batchSize);

            while (System.nanoTime() < endTime) {
                batchFutures.add(producer.send(topic, payload));
                messageCounter.incrementAndGet();

                if (batchFutures.size() >= batchSize) {
                    CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0])).join();
                    batchFutures.clear();
                }
            }

            if (!batchFutures.isEmpty()) {
                CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0])).join();
            }

            long actualElapsedNanos = System.nanoTime() - startTime;
            double actualElapsedSeconds = actualElapsedNanos / 1_000_000_000.0;
            double tps = messageCounter.get() / actualElapsedSeconds;

            return new ThroughputResult(tps, messageCounter.get());
        } finally {
            b1.shutdown();
            b2.shutdown();
            b3.shutdown();
        }
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

    private static record ThroughputResult(double tps, long totalMessages) {}
}
