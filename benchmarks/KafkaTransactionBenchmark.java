import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Kafka Transactions Benchmark
 * ─────────────────────────────────────────────────────────────────────────────
 * Mirrors DRMQ AtomicStressTestApp exactly:
 *   - 200,000 transactions
 *   - Each transaction writes atomically to 2 topics (topic-A + topic-B)
 *   - 1 KB payload per topic per transaction  →  2 KB total per transaction
 *   - Uses Kafka Transactions API: beginTransaction → send × 2 → commitTransaction
 *   - Measures TPS and latency percentiles (p50 / p95 / p99 / p999 / max)
 *
 * Usage: java KafkaTransactionBenchmark <bootstrap> <numTransactions> <concurrency> [mode]
 *
 * NOTE: Kafka transactions are per-producer — each producer handles one
 * transaction at a time (serial: begin → send → commit → begin → ...).
 * To increase throughput we use multiple transactional producers in parallel,
 * each with a unique transactional.id.
 * This mirrors AtomicStressTestApp's separate-producers mode.
 * "shared" mode is not supported for Kafka transactions (each producer must
 * be single-threaded).
 * ─────────────────────────────────────────────────────────────────────────────
 */
public class KafkaTransactionBenchmark {

    // ── Configuration (matches AtomicStressTestApp / kafka_benchmark.sh) ──────
    static final String TOPIC_A        = "txn-topic-a";
    static final String TOPIC_B        = "txn-topic-b";
    static final int    RECORD_SIZE    = 1024;          // 1 KB per topic per txn
    static final String ACKS           = "all";
    static final int    BATCH_SIZE     = 1048576;       // 1 MiB
    static final int    LINGER_MS      = 10;

    public static void main(String[] args) throws Exception {
        String bootstrap      = args.length > 0 ? args[0] : "localhost:9092";
        long   numTransactions = args.length > 1 ? Long.parseLong(args[1]) : 200_000;
        int    concurrency    = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        String producerMode   = args.length > 3 ? args[3] : "separate";
        int    numTopics      = args.length > 4 ? Integer.parseInt(args[4]) : 2;
        if (!producerMode.equals("separate")) {
            System.err.println("WARNING: Kafka transactions require per-producer serial execution.");
            System.err.println("  'separate' mode is always used (one producer per thread).");
            System.err.println("  Ignoring mode '" + producerMode + "'.");
        }

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║      Kafka Transactions Benchmark                    ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Configuration:");
        System.out.printf("  Bootstrap      : %s%n", bootstrap);
        System.out.printf("  Topics         : %d topics per transaction%n", numTopics);
        System.out.printf("  Transactions   : %,d%n", numTransactions);
        System.out.printf("  Concurrency    : %d transactional producer(s)%n", concurrency);
        System.out.printf("  Producer mode  : separate (one per thread — Kafka txns require it)%n");
        System.out.printf("  Payload/topic  : %,d bytes (1 KB)%n", RECORD_SIZE);
        System.out.printf("  Total data/txn : %,d bytes (%d KB)%n", RECORD_SIZE * numTopics, numTopics);
        System.out.printf("  Batch size     : %,d bytes (1 MiB)%n", BATCH_SIZE);
        System.out.printf("  Linger         : %d ms%n", LINGER_MS);
        System.out.printf("  ACKs           : %s%n", ACKS);
        System.out.println();

        byte[] payloadA = new byte[RECORD_SIZE];
        byte[] payloadB = new byte[RECORD_SIZE];
        Arrays.fill(payloadA, (byte) 'A');
        Arrays.fill(payloadB, (byte) 'B');

        // ── Latency tracking ──────────────────────────────────────────────────
        long[] latencies   = new long[(int) numTransactions];
        AtomicLong txDone  = new AtomicLong(0);
        AtomicLong errors  = new AtomicLong(0);
        AtomicLong txIndex = new AtomicLong(0);   // global index for latency array

        // ── Create one transactional producer per concurrent worker ────────────
        // Each Kafka transactional producer requires a unique transactional.id
        // and processes ONE transaction at a time (begin → send → commit is serial
        // within a producer). Multiple producers give us parallel transaction pipelines.
        KafkaProducer<String, byte[]>[] producers = new KafkaProducer[concurrency];
        for (int i = 0; i < concurrency; i++) {
            producers[i] = buildProducer(bootstrap, "drmq-txn-benchmark-" + i);
            producers[i].initTransactions();
        }

        CountDownLatch doneLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);

        AtomicLong lastPrint  = new AtomicLong(System.currentTimeMillis());
        AtomicLong lastCount  = new AtomicLong(0);
        long startTime        = System.currentTimeMillis();

        // ── Reporter thread ───────────────────────────────────────────────────
        Thread reporter = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1000);
                    long now   = System.currentTimeMillis();
                    long done  = txDone.get();
                    long errs  = errors.get();
                    long elapsed = (now - startTime) / 1000;
                    long rate  = done - lastCount.getAndSet(done);
                    System.out.printf("[%3ds] %,6d TPS | Total: %,d / %,d | Errors: %d%n",
                            elapsed, rate, done, numTransactions, errs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "txn-reporter");
        reporter.setDaemon(true);
        reporter.start();

        System.out.printf("🚀 Starting Kafka transaction benchmark...%n%n");

        // ── Worker threads ────────────────────────────────────────────────────
        for (int i = 0; i < concurrency; i++) {
            final KafkaProducer<String, byte[]> prod = producers[i];
            executor.submit(() -> {
                try {
                    while (true) {
                        long idx = txIndex.getAndIncrement();
                        if (idx >= numTransactions) break;

                        long txStart = System.currentTimeMillis();
                        try {
                            prod.beginTransaction();
                            for (int t = 0; t < numTopics; t++) {
                                prod.send(new ProducerRecord<>("txn-topic-" + t, payloadA));
                            }
                            prod.commitTransaction();   // blocks until broker acks both

                            long lat = System.currentTimeMillis() - txStart;
                            latencies[(int) idx] = lat;

                            long done = txDone.incrementAndGet();
                            if (done >= numTransactions) doneLatch.countDown();

                        } catch (Exception e) {
                            errors.incrementAndGet();
                            try { prod.abortTransaction(); } catch (Exception ignored) {}
                            long done = txDone.get() + errors.get();
                            if (done >= numTransactions) doneLatch.countDown();
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Worker failed: " + e.getMessage());
                }
            });
        }

        // ── Wait for all transactions ─────────────────────────────────────────
        boolean finished = doneLatch.await(30, TimeUnit.MINUTES);
        long totalTimeMs = System.currentTimeMillis() - startTime;

        reporter.interrupt();
        executor.shutdownNow();
        for (KafkaProducer<?, ?> p : producers) {
            try { p.close(); } catch (Exception ignored) {}
        }

        if (!finished) {
            System.out.println("\n⚠️  Timed out.");
        }

        printReport(totalTimeMs, numTransactions, txDone.get(), errors.get(), latencies, numTopics);
    }

    // ── Report ────────────────────────────────────────────────────────────────
    static void printReport(long totalMs, long target, long done, long errs, long[] latencies, int numTopics) {
        double totalSec   = totalMs / 1000.0;
        double tps        = done / totalSec;
        double mbSec      = (done * RECORD_SIZE * numTopics) / totalSec / (1024.0 * 1024.0);

        System.out.println();
        System.out.println("─".repeat(62));
        System.out.println("📊  Kafka Transactions Performance Report");
        System.out.println("─".repeat(62));
        System.out.printf("%,d transactions committed, %,.1f TPS (%.2f MB/sec)%n",
                done, tps, mbSec);

        if (done > 0) {
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
        System.out.printf("  Errors/Aborts : %,d%n",       errs);
        System.out.printf("  Payload/txn   : %d × 1 KB (%d topics)%n", numTopics, numTopics);
        System.out.printf("  ACKs          : all (ISR quorum)%n");
        System.out.println("─".repeat(62));
    }

    static long percentile(long[] sorted, double pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    static KafkaProducer<String, byte[]> buildProducer(String bootstrap, String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,       bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,    StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG,                    "all");
        p.put(ProducerConfig.BATCH_SIZE_CONFIG,              1048576);   // 1 MiB
        p.put(ProducerConfig.LINGER_MS_CONFIG,               10);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,      true);      // required for transactions
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,        txnId);
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // required for exactly-once
        return new KafkaProducer<>(p);
    }
}
