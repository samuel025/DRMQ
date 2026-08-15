package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQConsumer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EndToEndLatencyBenchmarkTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19800;
    private static final int PORT_2 = 19801;
    private static final int PORT_3 = 19802;

    @Test
    void runEndToEndBenchmark() throws Exception {
        System.out.println("=================================================");
        System.out.println("STARTING END-TO-END LATENCY BENCHMARK (Table 4.6)");
        System.out.println("=================================================");

        int[] batchSizes = {1, 8, 32, 128};
        List<LatencyResult> results = new ArrayList<>();

        for (int batchSize : batchSizes) {
            LatencyResult res = runTrial(batchSize, 300);
            results.add(res);
            System.out.printf("Batch Size %3d messages -> p50: %6.2f ms | p95: %6.2f ms | p99: %6.2f ms%n",
                    batchSize, res.p50, res.p95, res.p99);
        }

        System.out.println("\n=================================================");
        System.out.println("TABLE 4.6: PRODUCE-TO-CONSUME END-TO-END LATENCY RESULTS");
        System.out.println("=================================================");
        System.out.printf("| Batch size                    | p50 latency (ms) | p95 latency (ms) | p99 latency (ms) |%n");
        System.out.printf("| 1 message                     | %16.2f | %16.2f | %16.2f |%n", results.get(0).p50, results.get(0).p95, results.get(0).p99);
        System.out.printf("| 8 messages                    | %16.2f | %16.2f | %16.2f |%n", results.get(1).p50, results.get(1).p95, results.get(1).p99);
        System.out.printf("| 32 messages (~16 KB default)  | %16.2f | %16.2f | %16.2f |%n", results.get(2).p50, results.get(2).p95, results.get(2).p99);
        System.out.printf("| 128 messages                  | %16.2f | %16.2f | %16.2f |%n", results.get(3).p50, results.get(3).p95, results.get(3).p99);
        System.out.println("=================================================");
    }

    private LatencyResult runTrial(int batchSize, int messageCount) throws Exception {
        Path trialDir = tempDir.resolve("e2e-batch-" + batchSize);

        BrokerConfig c1 = new BrokerConfig("b1", PORT_1, trialDir.resolve("data-1").toString(),
                List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9110, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", true, true, null, null, null);
        BrokerConfig c2 = new BrokerConfig("b2", PORT_2, trialDir.resolve("data-2").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9111, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", true, true, null, null, null);
        BrokerConfig c3 = new BrokerConfig("b3", PORT_3, trialDir.resolve("data-3").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2)),
                true, 9112, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", true, true, null, null, null);

        BrokerServer b1 = new BrokerServer(c1);
        BrokerServer b2 = new BrokerServer(c2);
        BrokerServer b3 = new BrokerServer(c3);

        b1.startAsync();
        b2.startAsync();
        b3.startAsync();

        waitForLeader(b1, b2, b3, 8000);

        String topic = "e2e-topic-" + batchSize;
        String bootstrap = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;

        List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
        byte[] payload = new byte[512];

        try (DRMQProducer producer = new DRMQProducer(bootstrap);
             DRMQConsumer consumer = new DRMQConsumer(bootstrap, "e2e-group-" + batchSize)) {
            
            producer.connect();
            consumer.connect();
            consumer.subscribe(topic);

            // Warmup: 20 messages to prime sockets, JVM JIT, and Raft pipeline
            for (int i = 0; i < 20; i++) {
                producer.send(topic, payload).join();
            }
            long warmupDeadline = System.currentTimeMillis() + 3000;
            int warmupReceived = 0;
            while (warmupReceived < 20 && System.currentTimeMillis() < warmupDeadline) {
                var msgs = consumer.poll(20, 200);
                warmupReceived += msgs.size();
            }

            CountDownLatch allReceivedLatch = new CountDownLatch(messageCount);
            java.util.concurrent.atomic.AtomicBoolean consumerRunning = new java.util.concurrent.atomic.AtomicBoolean(true);

            // Start concurrent consumer thread to poll messages as they arrive
            Thread consumerThread = new Thread(() -> {
                try {
                    while (consumerRunning.get() && latencies.size() < messageCount) {
                        var msgs = consumer.poll(1000, 100);
                        long rcvTime = System.nanoTime();
                        for (var m : msgs) {
                            long sentTime = parseTimestamp(m.payload());
                            if (sentTime > 0) {
                                double elapsedMs = (rcvTime - sentTime) / 1_000_000.0;
                                latencies.add(elapsedMs);
                                allReceivedLatch.countDown();
                            }
                        }
                    }
                } catch (Exception ignored) {
                }
            }, "consumer-thread-" + batchSize);
            consumerThread.start();
            Thread.sleep(50);

            // Send messages with embedded nanoTime timestamp
            List<CompletableFuture<?>> sendFutures = new ArrayList<>();
            for (int i = 0; i < messageCount; i++) {
                long now = System.nanoTime();
                byte[] msgPayload = createPayloadWithTimestamp(now, payload);
                sendFutures.add(producer.send(topic, msgPayload));

                if ((i + 1) % batchSize == 0) {
                    long beforeJoin = System.nanoTime();
                    CompletableFuture.allOf(sendFutures.toArray(new CompletableFuture[0])).join();
                    long afterJoin = System.nanoTime();
                    if (batchSize == 32 && i < 32 * 5) {
                        System.out.println("DEBUG JOIN for batch " + (i/batchSize) + ": " + ((afterJoin - beforeJoin) / 1000000.0) + " ms");
                    }
                    sendFutures.clear();
                }
            }
            if (!sendFutures.isEmpty()) {
                CompletableFuture.allOf(sendFutures.toArray(new CompletableFuture[0])).join();
            }

            allReceivedLatch.await(15, TimeUnit.SECONDS);
            consumerRunning.set(false);
            consumerThread.join(2000);
        } finally {
            b1.shutdown();
            b2.shutdown();
            b3.shutdown();
        }

        Collections.sort(latencies);
        double p50 = getPercentile(latencies, 50);
        double p95 = getPercentile(latencies, 95);
        double p99 = getPercentile(latencies, 99);
        if (batchSize == 32) {
            System.out.println("DEBUG LATENCIES FOR 32: " + latencies);
        }
        return new LatencyResult(p50, p95, p99);
    }

    private byte[] createPayloadWithTimestamp(long timestampNanos, byte[] basePayload) {
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(8 + basePayload.length);
        bb.putLong(timestampNanos);
        bb.put(basePayload);
        return bb.array();
    }

    private long parseTimestamp(byte[] data) {
        if (data.length < 8) return 0;
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(data);
        return bb.getLong();
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
            if (leaderCount == 1) {
                Thread.sleep(150);
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    private static record LatencyResult(double p50, double p95, double p99) {}
}
