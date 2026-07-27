package com.drmq.client.commandLineExample;

import com.drmq.client.DRMQProducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-threaded Stress Test App
 * Spawns multiple threads within a single JVM to hammer the broker
 * with messages. Each thread shares the same producer pool.
 *
 * Usage:
 *   StressTestApp [bootstrapServers] [concurrency] [topic] [msgSize] [numRecords]
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
        try {
            concurrency  = args.length > 1 ? Integer.parseInt(args[1]) : 4;
            msgSize      = args.length > 3 ? Integer.parseInt(args[3]) : 1024;
            numRecords   = args.length > 4 ? Long.parseLong(args[4])   : 0L;
            if (concurrency < 1 || msgSize < 1 || numRecords < 0) {
                throw new NumberFormatException("Values must be positive");
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: StressTestApp [bootstrapServers] [concurrency] [topic] [msgSize] [numRecords]");
            System.exit(1);
            return;
        }

        String topic = args.length > 2 ? args[2] : "load-test-topic";
        boolean bounded = numRecords > 0;

        // ── Configuration banner ──────────────────────────────────────────────
        System.out.println("Configuration:");
        System.out.println("  Brokers      : " + bootstrapServers);
        System.out.println("  Concurrency  : " + concurrency + " threads");
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

        // ── In-flight semaphore ───────────────────────────────────────────────
        // The senderLoop inside DRMQProducer already serializes network sends:
        // it sends one batch at a time and waits for the ack (via sendLock)
        // before sending the next — exactly like Kafka's max.in.flight=1 at the IO level.
        //
        // The semaphore here only limits how many messages are queued in the
        // accumulator at once (backpressure). Keep it large so all concurrency
        // threads can keep the accumulator full → batches reach 1 MiB before linger fires.
        Semaphore inFlight = new Semaphore(8000);

        // ── ONE shared producer (mirrors Kafka: 1 producer client) ────────────
        // All concurrency threads call .send() on the same instance.
        // Its internal accumulator batches their messages together up to 1 MiB,
        // and the senderLoop flushes one batch at a time — equivalent to
        // Kafka's max.in.flight.requests.per.connection=1 at the network level.
        DRMQProducer sharedProducer = new DRMQProducer(bootstrapServers);
        sharedProducer.setBatchSizeBytes(1 * 1024 * 1024); // 1 MiB — matches Kafka benchmark
        sharedProducer.setLingerMs(10);                    // 10 ms  — matches Kafka benchmark
        try {
            sharedProducer.connect();
        } catch (Exception e) {
            System.err.println("Failed to connect producer: " + e.getMessage());
            System.exit(1);
        }

        // ── Global message index counter for bounded mode ─────────────────────
        // Each thread atomically claims the next index so latencies[] stays race-free.
        AtomicLong globalIndex = new AtomicLong(0);

        // ── Producer threads ──────────────────────────────────────────────────
        // N threads share the one producer — they call send() concurrently,
        // filling the accumulator so the senderLoop always has a full 1 MiB batch ready.
        for (int i = 0; i < concurrency; i++) {
            executor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        // Bounded mode: stop when all records have been claimed
                        long idx = -1;
                        if (bounded) {
                            idx = globalIndex.getAndIncrement();
                            if (idx >= numRecords) break;
                        }

                        inFlight.acquire();
                        final long sendTs = System.currentTimeMillis();
                        final long capturedIdx = idx;

                        sharedProducer.send(topic, payloadBytes).whenComplete((result, ex) -> {
                            inFlight.release();
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
            sharedProducer.close();

            printReport(totalTimeMs, numRecords, msgSize, messagesSent.get(), errors.get(), latencies);

        } else {
            // Unbounded: install shutdown hook and block forever
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down stress test...");
                sharedProducer.close();
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
