package com.drmq.client.commandLineExample;

import com.drmq.client.DRMQProducer;
import com.drmq.client.DRMQProducer.SendResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lightweight Single-Threaded Stress Test App
 * 
 * Enforces strictly 1 thread, 1 message per round trip (synchronous block on ACK),
 * with batching disabled (lingerMs=0, batchSize=1, maxInflight=1).
 *
 * Usage:
 *   SingleThreadStressTestApp [bootstrapServers] [topic] [msgSize] [numRecords]
 *
 * Examples:
 *   SingleThreadStressTestApp localhost:9092 load-test-topic 512 1000
 *   SingleThreadStressTestApp "localhost:9092,localhost:9093,localhost:9094" test-topic 1024 0
 */
public class SingleThreadStressTestApp {

    public static void main(String[] args) throws Exception {
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║    DRMQ Lightweight Single-Threaded Sequential Stress Test     ║");
        System.out.println("║    (1 Thread • 1 Msg / Round Trip • No Batching • Sync ACKs)   ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝\n");

        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092,localhost:9093,localhost:9094";
        String topic            = args.length > 1 ? args[1] : "single-thread-topic";
        int msgSize             = args.length > 2 ? Integer.parseInt(args[2]) : 512;
        long numRecords         = args.length > 3 ? Long.parseLong(args[3]) : 1000L;

        boolean bounded = numRecords > 0;

        System.out.println("Configuration:");
        System.out.println("  Brokers           : " + bootstrapServers);
        System.out.println("  Concurrency       : 1 thread");
        System.out.println("  Pattern           : 1 message per round-trip (Synchronous .join())");
        System.out.println("  Batching          : Disabled (batchSize=1 byte, lingerMs=0)");
        System.out.println("  Max Inflight      : 1");
        System.out.println("  Topic             : " + topic);
        System.out.println("  Payload Size      : " + msgSize + " bytes");
        if (bounded) {
            System.out.printf("  Num Records       : %,d messages%n%n", numRecords);
        } else {
            System.out.println("  Num Records       : ∞ (running until Ctrl+C)\n");
        }

        byte[] payloadBytes = new byte[msgSize];
        Arrays.fill(payloadBytes, (byte) 'x');

        DRMQProducer producer = new DRMQProducer(bootstrapServers);
        producer.setBatchSizeBytes(1);
        producer.setLingerMs(0);
        producer.setMaxInflight(1);

        System.out.print("Connecting to cluster... ");
        try {
            producer.connect();
            System.out.println("Connected!");
        } catch (IOException e) {
            System.err.println("\nFailed to connect: " + e.getMessage());
            producer.close();
            System.exit(1);
            return;
        }

        double[] latencies = bounded ? new double[(int) numRecords] : null;

        AtomicBoolean running = new AtomicBoolean(true);
        long[] counter = new long[]{0, 0}; // [0] = sent, [1] = errors
        long startTime = System.currentTimeMillis();

        // Reporter thread
        Thread reporter = new Thread(() -> {
            long lastSent = 0;
            while (running.get()) {
                try {
                    Thread.sleep(1000);
                    long currentSent = counter[0];
                    long currentErr  = counter[1];
                    long elapsedSec  = (System.currentTimeMillis() - startTime) / 1000;
                    long delta = currentSent - lastSent;
                    System.out.printf("[%3ds] %,6d msgs/sec | Total: %,7d | Errors: %d%n",
                            elapsedSec, delta, currentSent, currentErr);
                    lastSent = currentSent;
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "stress-reporter");
        reporter.setDaemon(true);
        reporter.start();

        System.out.println("🚀 Starting test loop...\n");

        if (bounded) {
            for (int i = 0; i < numRecords; i++) {
                long sendStart = System.nanoTime();
                try {
                    SendResult res = producer.send(topic, payloadBytes).join();
                    long sendEnd = System.nanoTime();
                    double elapsedMs = (sendEnd - sendStart) / 1_000_000.0;
                    if (latencies != null) {
                        latencies[i] = elapsedMs;
                    }
                    if (res != null && res.isSuccess()) {
                        counter[0]++;
                    } else {
                        counter[1]++;
                    }
                } catch (Exception e) {
                    counter[1]++;
                }
            }

            long totalTimeMs = System.currentTimeMillis() - startTime;
            running.set(false);
            reporter.interrupt();
            producer.close();

            printReport(totalTimeMs, numRecords, msgSize, counter[0], counter[1], latencies);
        } else {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                long totalTimeMs = System.currentTimeMillis() - startTime;
                producer.close();
                System.out.printf("%n📊 Final Results:%n");
                System.out.printf("  Total Sent   : %,d messages%n", counter[0]);
                System.out.printf("  Total Errors : %,d%n", counter[1]);
                System.out.printf("  Elapsed      : %.3f s%n", totalTimeMs / 1000.0);
                System.out.printf("  Avg Rate     : %,.2f msgs/sec%n",
                        (counter[0] * 1000.0) / Math.max(1, totalTimeMs));
            }));

            while (running.get()) {
                long sendStart = System.nanoTime();
                try {
                    SendResult res = producer.send(topic, payloadBytes).join();
                    if (res != null && res.isSuccess()) {
                        counter[0]++;
                    } else {
                        counter[1]++;
                    }
                } catch (Exception e) {
                    counter[1]++;
                }
            }
        }
    }

    private static void printReport(long totalTimeMs, long numRecords, int msgSize,
                                    long sent, long errCount, double[] latencies) {
        double totalSec      = totalTimeMs / 1000.0;
        double throughputMsg = sent / Math.max(0.001, totalSec);
        double throughputMB  = (sent * (long) msgSize) / Math.max(0.001, totalSec) / (1024.0 * 1024.0);

        System.out.println("\n" + "─".repeat(64));
        System.out.println("📊  DRMQ Single-Thread Sequential Producer Performance Report");
        System.out.println("─".repeat(64));

        System.out.printf("%,d records sent, %,.1f records/sec (%.3f MB/sec)%n",
                sent, throughputMsg, throughputMB);

        if (latencies != null && sent > 0) {
            double[] filled = Arrays.copyOf(latencies, (int) Math.min(sent, latencies.length));
            Arrays.sort(filled);

            double p50  = percentile(filled, 50);
            double p95  = percentile(filled, 95);
            double p99  = percentile(filled, 99);
            double p999 = percentile(filled, 99.9);
            double max  = filled[filled.length - 1];
            double avg  = Arrays.stream(filled).average().orElse(0);

            System.out.printf("  avg latency : %6.2f ms%n", avg);
            System.out.printf("  p50 latency : %6.2f ms%n", p50);
            System.out.printf("  p95 latency : %6.2f ms%n", p95);
            System.out.printf("  p99 latency : %6.2f ms%n", p99);
            System.out.printf("  p999 latency: %6.2f ms%n", p999);
            System.out.printf("  max latency : %6.2f ms%n", max);
        }

        System.out.println("─".repeat(64));
        System.out.printf("  Elapsed      : %.3f s%n", totalSec);
        System.out.printf("  Sent         : %,d / %,d records%n", sent, numRecords);
        System.out.printf("  Errors       : %,d%n", errCount);
        System.out.printf("  Payload Size : %,d bytes%n", msgSize);
        System.out.printf("  Mode         : 1 Thread, 1 Msg / Round Trip (Sync ACK)%n");
        System.out.println("─".repeat(64));
    }

    private static double percentile(double[] sorted, double pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
