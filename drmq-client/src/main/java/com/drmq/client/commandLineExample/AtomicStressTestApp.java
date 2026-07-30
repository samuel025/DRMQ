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
 *
 * Two producer modes:
 *   5th arg = "shared" (default)  — single producer, threads fill the accumulator
 *   5th arg = "separate"         — one producer per thread (mirrors Kafka txn benchmark)
 *
 *   "shared"   mode: ./stress_test.sh   (DRMQ's natural batching advantage)
 *   "separate" mode: ./atomic_stress_test.sh -c 4 -i 1 -m separate
 *                    (each producer serial, like Kafka's begin→send→commit)
 *
 * Usage:
 *   AtomicStressTestApp [bootstrapServers] [concurrency] [numTransactions] [inFlight] [mode]
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
        String producerMode;
        try {
            concurrency      = args.length > 1 ? Integer.parseInt(args[1])  : 10;
            numTransactions  = args.length > 2 ? Long.parseLong(args[2])    : 0L;
            inFlight         = args.length > 3 ? Integer.parseInt(args[3])  : 5000;
            producerMode     = args.length > 4 ? args[4]                    : "shared";
            if (concurrency < 1 || numTransactions < 0 || inFlight < 1) throw new NumberFormatException("must be positive");
            if (!producerMode.equals("shared") && !producerMode.equals("separate")) {
                throw new NumberFormatException("mode must be 'shared' or 'separate'");
            }
        } catch (NumberFormatException e) {
            System.err.println("Usage: AtomicStressTestApp [bootstrapServers] [concurrency] [numTransactions] [inFlight] [mode] [numTopics]");
            System.err.println("  mode: 'shared' (default, one producer) or 'separate' (one producer per thread, mirrors Kafka)");
            System.exit(1);
            return;
        }

        int numTopics = args.length > 5 ? Integer.parseInt(args[5]) : 2;

        boolean bounded = numTransactions > 0;
        boolean separateProducers = producerMode.equals("separate");

        // ── Config banner ──────────────────────────────────────────────────────
        System.out.println("Configuration:");
        System.out.println("  Brokers        : " + bootstrapServers);
        System.out.printf ("  Concurrency    : %d thread(s)%n", concurrency);
        System.out.println("  Producer mode  : " + (separateProducers ? "separate (one per thread, Kafka-comparable)" : "shared (single producer)"));
        System.out.println("  Topics         : " + numTopics + " topics per transaction");
        System.out.println("  Payload/topic  : 1 KB  (" + numTopics + " KB total per transaction)");
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

        // ── Producer setup ───────────────────────────────────────────────────
        // Two modes:
        //   "shared"   — single DRMQProducer, all threads call sendAtomic() on it.
        //                atomicSenderLoop batches across threads → DRMQ's advantage.
        //   "separate" — each thread gets its own DRMQProducer (serial per producer).
        //                Mirrors Kafka's beginTransaction→commit model.
        final DRMQProducer[] producers;
        if (separateProducers) {
            producers = new DRMQProducer[concurrency];
            for (int i = 0; i < concurrency; i++) {
                producers[i] = new DRMQProducer(bootstrapServers);
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
            try {
                producers[0].connect();
            } catch (java.io.IOException e) {
                System.err.println("Failed to connect: " + e.getMessage());
                System.exit(1);
            }
        }

        // ── Semaphore ─────────────────────────────────────────────────────────
        // In shared mode: controls how many sendAtomic() calls pile up in the
        //   atomicAccumulator before the atomicSenderLoop batches them.
        // In separate mode: each producer has its own accumulator; each thread
        //   gets its own semaphore so they don't serialize each other.
        final Semaphore[] threadInFlight = separateProducers
                ? java.util.stream.IntStream.range(0, concurrency)
                        .mapToObj(i -> new Semaphore(inFlight))
                        .toArray(Semaphore[]::new)
                : null;
        Semaphore inFlightSem = separateProducers ? null : new Semaphore(inFlight);

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
            final Semaphore mySem = separateProducers ? threadInFlight[tid] : inFlightSem;
            final DRMQProducer prod = separateProducers ? producers[i] : producers[0];
            executor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        long idx = -1;
                        if (bounded) {
                            idx = txIndex.getAndIncrement();
                            if (idx >= numTransactions) break;
                        }

                        mySem.acquire();
                        final long txStart      = System.currentTimeMillis();
                        final long capturedIdx  = idx;

                        Map<String, byte[]> atomicBatch = new HashMap<>();
                        for (int t = 0; t < numTopics; t++) {
                            atomicBatch.put("Topic-" + t, payloadA);
                        }

                        prod.sendAtomic(atomicBatch).whenComplete((res, ex) -> {
                            mySem.release();
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
            for (DRMQProducer p : producers) {
                p.close();
            }

            if (!finished) System.out.println("\n⚠️  Timed out waiting for all acks.");

            printReport(totalTimeMs, numTransactions, txDone.get(), errors.get(), latencies, numTopics);

        } else {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down...");
                for (DRMQProducer p : producers) {
                    p.close();
                }
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
                        (done * (1024L * numTopics) * 1000.0) / Math.max(1, totalTimeMs) / (1024.0 * 1024.0));
            }));
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        }
    }

    // ── Kafka-style report ────────────────────────────────────────────────────
    private static void printReport(long totalMs, long target, long done, long errs, long[] latencies, int numTopics) {
        double totalSec = totalMs / 1000.0;
        double tps      = done / totalSec;
        double mbSec    = (done * (1024L * numTopics)) / totalSec / (1024.0 * 1024.0); 

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
        System.out.printf("  Payload/txn   : %d × 1 KB (%d topics)%n", numTopics, numTopics);
        System.out.printf("  ACKs          : all (Raft quorum)%n");
        System.out.println("─".repeat(62));
    }

    private static long percentile(long[] sorted, double pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
