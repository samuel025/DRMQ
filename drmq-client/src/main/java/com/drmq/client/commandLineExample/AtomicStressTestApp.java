package com.drmq.client.commandLineExample;

import com.drmq.client.DRMQProducer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DRMQ Atomic Cross-Topic Stress Test
 * ─────────────────────────────────────────────────────────────────────────────
 * Mirrors KafkaTransactionBenchmark exactly:
 *   - 200,000 atomic transactions (bounded)
 *   - Each transaction writes atomically to 2 topics (Topic-A + Topic-B)
 *   - 1 KB payload per topic per transaction  →  2 KB total per transaction
 *   - Concurrency = 1 (serial: one transaction in-flight at a time)
 *     → equivalent to Kafka's serial beginTransaction→commit model
 *   - Semaphore(1): ensures only 1 sendAtomic() is in the atomicAccumulator
 *     at a time, so each becomes its own Raft proposal (no batching advantage)
 *   - Reports TPS + p50 / p95 / p99 / p999 / max latency
 *
 * Usage:
 *   AtomicStressTestApp [bootstrapServers] [concurrency] [numTransactions]
 *
 * When numTransactions > 0: bounded mode with full report.
 * When numTransactions = 0: unbounded (run until Ctrl+C), original behaviour.
 */
public class AtomicStressTestApp {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("╔═══════════════════════════════════════════════════╗");
        System.out.println("║    DRMQ Atomic Cross-Topic Stress Test            ║");
        System.out.println("╚═══════════════════════════════════════════════════╝\n");

        // ── Arguments ─────────────────────────────────────────────────────────
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092,localhost:9093,localhost:9094";
        int    concurrency;
        long   numTransactions;
        int    inFlight;
        try {
            concurrency      = args.length > 1 ? Integer.parseInt(args[1])  : 10;
            numTransactions  = args.length > 2 ? Long.parseLong(args[2])    : 0L;
            inFlight         = args.length > 3 ? Integer.parseInt(args[3])  : 5000;
            if (concurrency < 1 || numTransactions < 0 || inFlight < 1) throw new NumberFormatException("must be positive");
        } catch (NumberFormatException e) {
            System.err.println("Usage: AtomicStressTestApp [bootstrapServers] [concurrency] [numTransactions] [inFlight]");
            System.exit(1);
            return;
        }

        boolean bounded = numTransactions > 0;

        // ── Config banner ──────────────────────────────────────────────────────
        System.out.println("Configuration:");
        System.out.println("  Brokers        : " + bootstrapServers);
        System.out.printf ("  Concurrency    : %d thread(s)%n", concurrency);
        System.out.println("  Topics         : Topic-A  +  Topic-B");
        System.out.println("  Payload/topic  : 1 KB  (2 KB total per transaction)");
        System.out.println("  ACKs           : all (Raft quorum)");
        System.out.printf ("  In-flight      : %d (%s)%n", inFlight,
                inFlight > 1 ? "batching ON — multiple txns per Raft proposal" : "serial — 1 txn per Raft proposal");
        if (bounded) {
            System.out.printf("  Transactions   : %,d%n%n", numTransactions);
        } else {
            System.out.println("  Transactions   : ∞ (run until Ctrl+C)\n");
        }

        // ── Payloads ──────────────────────────────────────────────────────────
        final byte[] payloadA = new byte[1024];
        final byte[] payloadB = new byte[1024];
        Arrays.fill(payloadA, (byte) 'A');
        Arrays.fill(payloadB, (byte) 'B');

        // ── Counters / latency ────────────────────────────────────────────────
        final long[] latencies        = bounded ? new long[(int) numTransactions] : null;
        AtomicLong   txDone           = new AtomicLong(0);
        AtomicLong   errors           = new AtomicLong(0);
        AtomicLong   txIndex          = new AtomicLong(0);
        CountDownLatch doneLatch      = bounded ? new CountDownLatch(1) : null;

        // ── Connect shared producer ───────────────────────────────────────────
        DRMQProducer sharedProducer = new DRMQProducer(bootstrapServers);
        try {
            sharedProducer.connect();
        } catch (java.io.IOException e) {
            System.err.println("Failed to connect: " + e.getMessage());
            System.exit(1);
        }

        // ── Semaphore ─────────────────────────────────────────────────────────
        // Controlled by the inFlight argument:
        //   inFlight=5000 (default): keeps atomicAccumulator full → atomicSenderLoop
        //     batches multiple sendAtomic() calls into ONE Raft proposal.
        //     This is DRMQ's natural architectural advantage.
        //   inFlight=1: serial mode — 1 txn at a time, 1 Raft proposal per txn,
        //     equivalent to Kafka's beginTransaction→commitTransaction.
        Semaphore inFlightSem = new Semaphore(inFlight);

        // ── Reporter ──────────────────────────────────────────────────────────
        long startTime = System.currentTimeMillis();
        AtomicLong lastCount = new AtomicLong(0);
        Thread reporter = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1000);
                    long done  = txDone.get();
                    long errs  = errors.get();
                    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
                    long rate  = done - lastCount.getAndSet(done);
                    if (bounded) {
                        System.out.printf("[%3ds] %,6d TPS | Total: %,d / %,d | Errors: %d%n",
                                elapsed, rate, done, numTransactions, errs);
                    } else {
                        System.out.printf("[%3ds] %,6d TPS | Total: %,d | Errors: %d%n",
                                elapsed, rate, done, errs);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "atomic-reporter");
        reporter.setDaemon(true);
        reporter.start();

        System.out.println("🚀 Starting atomic load test..." + (bounded ? "" : " Press Ctrl+C to stop."));

        // ── Worker threads ────────────────────────────────────────────────────
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        for (int i = 0; i < concurrency; i++) {
            final int tid = i;
            executor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        long idx = -1;
                        if (bounded) {
                            idx = txIndex.getAndIncrement();
                            if (idx >= numTransactions) break;
                        }

                        inFlightSem.acquire();
                        final long txStart      = System.currentTimeMillis();
                        final long capturedIdx  = idx;

                        Map<String, byte[]> atomicBatch = new HashMap<>();
                        atomicBatch.put("Topic-A", payloadA);
                        atomicBatch.put("Topic-B", payloadB);

                        sharedProducer.sendAtomic(atomicBatch).whenComplete((res, ex) -> {
                            inFlightSem.release();
                            if (ex == null) {
                                long done = txDone.incrementAndGet();
                                if (bounded && latencies != null && capturedIdx >= 0) {
                                    latencies[(int) capturedIdx] = System.currentTimeMillis() - txStart;
                                }
                                if (bounded && done >= numTransactions && doneLatch != null) {
                                    doneLatch.countDown();
                                }
                            } else {
                                errors.incrementAndGet();
                                if (bounded && (txDone.get() + errors.get()) >= numTransactions && doneLatch != null) {
                                    doneLatch.countDown();
                                }
                            }
                        });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("[T" + tid + "] failed: " + e.getMessage());
                }
            });
        }

        // ── Wait / shutdown ───────────────────────────────────────────────────
        if (bounded) {
            boolean finished = doneLatch.await(30, TimeUnit.MINUTES);
            long totalTimeMs = System.currentTimeMillis() - startTime;

            reporter.interrupt();
            executor.shutdownNow();
            sharedProducer.close();

            if (!finished) System.out.println("\n⚠️  Timed out waiting for all acks.");

            printReport(totalTimeMs, numTransactions, txDone.get(), errors.get(), latencies);

        } else {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down...");
                sharedProducer.close();
                executor.shutdownNow();
                reporter.interrupt();
                long totalTimeMs = System.currentTimeMillis() - startTime;
                long done = txDone.get();
                System.out.printf("%n📊 Final Results:%n");
                System.out.printf("  Total Tx Committed : %,d%n", done);
                System.out.printf("  Total Errors       : %,d%n", errors.get());
                System.out.printf("  Elapsed            : %.3f s%n", totalTimeMs / 1000.0);
                System.out.printf("  Avg TPS            : %.2f%n", (done * 1000.0) / Math.max(1, totalTimeMs));
                System.out.printf("  Throughput         : %.2f MB/s%n",
                        (done * 2048L * 1000.0) / Math.max(1, totalTimeMs) / (1024.0 * 1024.0));
            }));
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        }
    }

    // ── Kafka-style report ────────────────────────────────────────────────────
    private static void printReport(long totalMs, long target, long done, long errs, long[] latencies) {
        double totalSec = totalMs / 1000.0;
        double tps      = done / totalSec;
        double mbSec    = (done * 2048L) / totalSec / (1024.0 * 1024.0); // 2KB per txn

        System.out.println("\n" + "─".repeat(62));
        System.out.println("📊  DRMQ Atomic Transactions Performance Report");
        System.out.println("─".repeat(62));
        System.out.printf("%,d transactions committed, %,.1f TPS (%.2f MB/sec)%n",
                done, tps, mbSec);

        if (latencies != null && done > 0) {
            long[] filled = Arrays.copyOf(latencies, (int) Math.min(done, latencies.length));
            Arrays.sort(filled);
            double avg = Arrays.stream(filled).average().orElse(0);
            long p50   = percentile(filled, 50);
            long p95   = percentile(filled, 95);
            long p99   = percentile(filled, 99);
            long p999  = percentile(filled, 99.9);
            long max   = filled[filled.length - 1];

            System.out.printf("  avg latency : %.2f ms%n", avg);
            System.out.printf("  max latency : %d ms%n",   max);
            System.out.printf("  p50 latency : %d ms%n",   p50);
            System.out.printf("  p95 latency : %d ms%n",   p95);
            System.out.printf("  p99 latency : %d ms%n",   p99);
            System.out.printf("  p999 latency: %d ms%n",   p999);
        }

        System.out.println("─".repeat(62));
        System.out.printf("  Elapsed       : %.3f s%n",    totalSec);
        System.out.printf("  Committed     : %,d / %,d%n", done, target);
        System.out.printf("  Errors        : %,d%n",       errs);
        System.out.printf("  Payload/txn   : 2 × 1 KB (2 topics)%n");
        System.out.printf("  ACKs          : all (Raft quorum)%n");
        System.out.println("─".repeat(62));
    }

    private static long percentile(long[] sorted, double pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
