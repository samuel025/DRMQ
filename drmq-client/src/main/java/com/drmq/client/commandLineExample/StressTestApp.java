package com.drmq.client.commandLineExample;

import com.drmq.client.DRMQProducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-threaded Stress Test App
 * Spawns multiple threads within a single JVM to hammer the broker
 * with messages.
 *
 * Two producer modes:
 *   5th arg = "shared" (default)  — single producer, threads fill the accumulator
 *   5th arg = "separate"         — one producer per thread
 *
 * Usage:
 *   StressTestApp [bootstrapServers] [concurrency] [topic] [msgSize] [numRecords] [mode]
 *
 * When numRecords > 0, the test runs in bounded mode: it sends exactly
 * numRecords messages, then prints a Kafka-style performance report
 * (throughput, p50/p95/p99/p999/max latency) and exits.
 *
 * When numRecords is omitted or 0, the test runs forever until Ctrl+C.
 */
public class StressTestApp {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("╔═══════════════════════════════════════════════════╗");
        System.out.println("║        DRMQ Multi-Threaded Stress Test            ║");
        System.out.println("╚═══════════════════════════════════════════════════╝\n");

        // ── Parse arguments ───────────────────────────────────────────────────
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092,localhost:9093,localhost:9094";
        int concurrency;
        int msgSize;
        long numRecords;
        String producerMode;
        try {
            concurrency  = args.length > 1 ? Integer.parseInt(args[1]) : 4;
            msgSize      = args.length > 3 ? Integer.parseInt(args[3]) : 1024;
            numRecords   = args.length > 4 ? Long.parseLong(args[4])   : 0L;
            producerMode = args.length > 5 ? args[5]                    : "shared";
            if (concurrency < 1 || msgSize < 1 || numRecords < 0) {
                throw new NumberFormatException("Values must be positive");
            }
            if (!producerMode.equals("shared") && !producerMode.equals("separate")) {
                throw new NumberFormatException("mode must be 'shared' or 'separate'");
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: StressTestApp [bootstrapServers] [concurrency] [topic] [msgSize] [numRecords] [mode]");
            System.err.println("  mode: 'shared' (default, single producer) or 'separate' (one producer per thread)");
            System.exit(1);
            return;
        }

        String topic = args.length > 2 ? args[2] : "load-test-topic";
        boolean bounded = numRecords > 0;
        boolean separateProducers = producerMode.equals("separate");

        // ── Configuration banner ──────────────────────────────────────────────
        System.out.println("Configuration:");
        System.out.println("  Brokers      : " + bootstrapServers);
        System.out.println("  Concurrency  : " + concurrency + " thread(s)");
        System.out.println("  Producer mode: " + (separateProducers ? "separate (one per thread)" : "shared (single producer)"));
        System.out.println("  Topic        : " + topic);
        System.out.println("  Payload Size : " + msgSize + " bytes");
        System.out.println("  Batch Size   : 1 MiB");
        System.out.println("  Linger       : 10 ms");
        System.out.println("  ACKs         : all (Raft quorum)");
        if (bounded) {
            System.out.printf("  Num Records  : %,d%n%n", numRecords);
        } else {
            System.out.println("  Num Records  : ∞ (run until Ctrl+C)\n");
        }

        // ── Payload ───────────────────────────────────────────────────────────
        final byte[] payloadBytes = new byte[msgSize];
        for (int i = 0; i < msgSize; i++) {
            payloadBytes[i] = (byte) ('a' + (i % 26));
        }

        // ── Latency collection (bounded mode only) ────────────────────────────
        // We store per-message round-trip latency in milliseconds.
        // Pre-allocate an array large enough for all expected records.
        final long[] latencies = bounded ? new long[(int) numRecords] : null;

        // ── Counters ──────────────────────────────────────────────────────────
        AtomicLong messagesSent   = new AtomicLong(0);
        AtomicLong errors         = new AtomicLong(0);
        CountDownLatch doneLatch  = bounded ? new CountDownLatch(1) : null;

        // ── Build producers ───────────────────────────────────────────────────
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        System.out.println("🚀 Starting load test..." + (bounded ? "" : " Press Ctrl+C to stop."));

        // ── Periodic reporter thread ──────────────────────────────────────────
        Thread reporter = new Thread(() -> {
            try {
                long lastSent = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1000);
                    long currentSent   = messagesSent.get();
                    long currentErrors = errors.get();
                    long elapsedSec    = (System.currentTimeMillis() - startTime) / 1000;
                    System.out.printf("[%3ds] %,7d msgs/sec | Total: %,d | Errors: %d%n",
                            elapsedSec, (currentSent - lastSent), currentSent, currentErrors);
                    lastSent = currentSent;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "stress-reporter");
        reporter.setDaemon(true);
        reporter.start();

        // ── Build producer(s) ────────────────────────────────────────────────
        // Two modes:
        //   "shared"   — single DRMQProducer, all threads call send() on it.
        //                The senderLoop batches across threads.
        //   "separate" — each thread gets its own DRMQProducer (serial per producer).
        final DRMQProducer[] producers;
        if (separateProducers) {
            producers = new DRMQProducer[concurrency];
            for (int i = 0; i < concurrency; i++) {
                producers[i] = new DRMQProducer(bootstrapServers);
                producers[i].setBatchSizeBytes(1 * 1024 * 1024);
                producers[i].setLingerMs(10);
                try {
                    producers[i].connect();
                } catch (java.io.IOException e) {
                    System.err.println("Failed to connect producer " + i + ": " + e.getMessage());
                    System.exit(1);
                }
            }
        } else {
            producers = new DRMQProducer[1];
            producers[0] = new DRMQProducer(bootstrapServers);
            producers[0].setBatchSizeBytes(1 * 1024 * 1024);
            producers[0].setLingerMs(10);
            try {
                producers[0].connect();
            } catch (Exception e) {
                System.err.println("Failed to connect producer: " + e.getMessage());
                System.exit(1);
            }
        }

        // ── Per-thread backpressure semaphore ─────────────────────────────────
        // Prevents accumulator overflow when threads call send() faster than the
        // senderLoop can drain it. Each thread gets its own budget so they don't
        // serialize each other (important in separate mode).
        final Semaphore[] threadPermits = new Semaphore[concurrency];
        for (int i = 0; i < concurrency; i++) {
            threadPermits[i] = new Semaphore(separateProducers ? 2000 : 8000 / concurrency);
        }

        // ── Global message index counter for bounded mode ─────────────────────
        // Each thread atomically claims the next index so latencies[] stays race-free.
        AtomicLong globalIndex = new AtomicLong(0);

        // ── Producer threads ──────────────────────────────────────────────────
        for (int i = 0; i < concurrency; i++) {
            final Semaphore myPermit = threadPermits[i];
            final DRMQProducer prod = separateProducers ? producers[i] : producers[0];
            executor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        // Bounded mode: stop when all records have been claimed
                        long idx = -1;
                        if (bounded) {
                            idx = globalIndex.getAndIncrement();
                            if (idx >= numRecords) break;
                        }

                        myPermit.acquire();
                        final long sendTs = System.currentTimeMillis();
                        final long capturedIdx = idx;

                        prod.send(topic, payloadBytes).whenComplete((result, ex) -> {
                            myPermit.release();
                            if (ex == null && result != null && result.isSuccess()) {
                                long sentCount = messagesSent.incrementAndGet();
                                if (bounded && latencies != null && capturedIdx >= 0) {
                                    latencies[(int) capturedIdx] = System.currentTimeMillis() - sendTs;
                                }
                                // Signal completion when the last ack arrives
                                if (bounded && sentCount >= numRecords && doneLatch != null) {
                                    doneLatch.countDown();
                                }
                            } else {
                                errors.incrementAndGet();
                                // Still signal done if errors pushed us past the target
                                if (bounded && (messagesSent.get() + errors.get()) >= numRecords && doneLatch != null) {
                                    doneLatch.countDown();
                                }
                            }
                        });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Producer failed: " + e.getMessage());
                }
            });
        }

        // ── Wait for completion (bounded) or shutdown hook (unbounded) ─────────
        if (bounded) {
            // Wait up to 10 min for all acks; then force a summary
            boolean finished = doneLatch.await(10, TimeUnit.MINUTES);
            if (!finished) {
                System.out.println("\n⚠️  Timed out waiting for all acks.");
            }

            long totalTimeMs = System.currentTimeMillis() - startTime;
            reporter.interrupt();
            executor.shutdownNow();
            for (DRMQProducer p : producers) {
                p.close();
            }

            printReport(totalTimeMs, numRecords, msgSize, messagesSent.get(), errors.get(), latencies);

        } else {
            // Unbounded: install shutdown hook and block forever
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down stress test...");
                for (DRMQProducer p : producers) {
                    p.close();
                }
                executor.shutdownNow();
                reporter.interrupt();
                long totalTimeMs = System.currentTimeMillis() - startTime;
                long sent = messagesSent.get();
                System.out.printf("%n📊 Final Results:%n");
                System.out.printf("  Total Sent   : %,d messages%n", sent);
                System.out.printf("  Total Errors : %,d%n", errors.get());
                System.out.printf("  Elapsed      : %.3f s%n", totalTimeMs / 1000.0);
                System.out.printf("  Avg Rate     : %,.2f msgs/sec%n",
                        (sent * 1000.0) / Math.max(1, totalTimeMs));
                System.out.printf("  Throughput   : %,.2f MB/s%n",
                        (sent * (long) msgSize * 1000.0) / Math.max(1, totalTimeMs) / (1024.0 * 1024.0));
            }));
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        }
    }


    // ─────────────────────────────────────────────────────────────────────────
    // Kafka-style performance report
    // ─────────────────────────────────────────────────────────────────────────
    private static void printReport(long totalTimeMs, long numRecords, int msgSize,
                                    long sent, long errCount, long[] latencies) {
        double totalSec      = totalTimeMs / 1000.0;
        double throughputMsg = sent / totalSec;
        double throughputMB  = (sent * (long) msgSize) / totalSec / (1024.0 * 1024.0);

        System.out.println("\n" + "─".repeat(60));
        System.out.println("📊  DRMQ Producer Performance Report");
        System.out.println("─".repeat(60));

        // ── Throughput line (mirrors kafka-producer-perf-test output) ─────────
        System.out.printf("%,d records sent, %,.1f records/sec (%.2f MB/sec)%n",
                sent, throughputMsg, throughputMB);

        // ── Latency percentiles ───────────────────────────────────────────────
        if (latencies != null && sent > 0) {
            // Only consider the indices that were actually acked
            long[] filled = Arrays.copyOf(latencies, (int) Math.min(sent, latencies.length));
            Arrays.sort(filled);

            long p50  = percentile(filled, 50);
            long p95  = percentile(filled, 95);
            long p99  = percentile(filled, 99);
            long p999 = percentile(filled, 99.9);
            long max  = filled[filled.length - 1];
            double avg = Arrays.stream(filled).average().orElse(0);

            System.out.printf("  avg latency: %.2f ms%n", avg);
            System.out.printf("  max latency: %d ms%n", max);
            System.out.printf("  p50 latency: %d ms%n", p50);
            System.out.printf("  p95 latency: %d ms%n", p95);
            System.out.printf("  p99 latency: %d ms%n", p99);
            System.out.printf("  p999 latency: %d ms%n", p999);
        }

        // ── Summary ───────────────────────────────────────────────────────────
        System.out.println("─".repeat(60));
        System.out.printf("  Elapsed      : %.3f s%n", totalSec);
        System.out.printf("  Sent         : %,d / %,d records%n", sent, numRecords);
        System.out.printf("  Errors       : %,d%n", errCount);
        System.out.printf("  Payload Size : %,d bytes%n", msgSize);
        System.out.printf("  Batch Size   : 1 MiB%n");
        System.out.printf("  Linger       : 10 ms%n");
        System.out.printf("  ACKs         : all (Raft quorum)%n");
        System.out.println("─".repeat(60));
    }

    /** Compute the Nth percentile from a sorted long[] array. */
    private static long percentile(long[] sorted, double pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
