import com.rabbitmq.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * RabbitMQ producer benchmark — mirrors kafka_benchmark.sh / stress_test.sh
 *
 * Batches sends to match Kafka's 1 MiB batch (1024 msgs × 1 KiB).
 * Uses waitForConfirmsOrDie() per batch — equivalent to Kafka's
 * batch.size + acks=all at the batch level.
 *
 * Usage:
 *   java RabbitMQBenchmark [amqp://uri] [numMessages] [batchSize]
 */
public class RabbitMQBenchmark {

    static final String QUEUE_NAME = "benchmark-queue";
    static final int    MSG_SIZE   = 1024;

    public static void main(String[] args) throws Exception {
        String uri          = args.length > 0 ? args[0] : "amqp://guest:guest@localhost:5672";
        long   numMessages  = args.length > 1 ? Long.parseLong(args[1]) : 200_000;
        int    batchSize    = args.length > 2 ? Integer.parseInt(args[2]) : 1024; // 1 MiB / 1 KiB

        // ── Payload ──────────────────────────────────────────────────────
        byte[] payload = new byte[MSG_SIZE];
        Arrays.fill(payload, (byte) 'x');

        // ── Connection ───────────────────────────────────────────────────
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(uri);
        factory.setAutomaticRecoveryEnabled(false);
        Connection conn = factory.newConnection();
        Channel    ch   = conn.createChannel();

        // Declare quorum queue (3 nodes, majority ACK)
        Map<String, Object> qargs = new HashMap<>();
        qargs.put("x-queue-type", "quorum");
        qargs.put("x-quorum-initial-group-size", 3);
        ch.queueDeclare(QUEUE_NAME, true, false, false, qargs);
        ch.confirmSelect();

        AMQP.BasicProperties props = MessageProperties.PERSISTENT_BASIC;

        // ── Latency tracking ─────────────────────────────────────────────
        long[] latencies   = new long[(int) numMessages];
        long   startTime   = System.currentTimeMillis();

        // ── Batch send loop ──────────────────────────────────────────────
        long   idx         = 0;
        long   errors      = 0;

        System.out.println("╔═══════════════════════════════════════════════════╗");
        System.out.println("║     RabbitMQ Batch Producer Benchmark            ║");
        System.out.println("╚═══════════════════════════════════════════════════╝\n");
        System.out.println("Configuration:");
        System.out.println("  URI          : " + uri);
        System.out.println("  Queue        : " + QUEUE_NAME + " (quorum, RF=3)");
        System.out.println("  Messages     : " + numMessages);
        System.out.println("  Payload      : " + MSG_SIZE + " bytes (1 KB)");
        System.out.println("  Batch size   : " + batchSize + " msgs (~" + (batchSize * MSG_SIZE / 1024) + " KiB)");
        System.out.println("  Confirms     : batch (equivalent to acks=all)\n");

        while (idx < numMessages) {
            long batchStart = System.nanoTime();
            int  batchCount = 0;

            // Send batch
            try {
                for (int i = 0; i < batchSize && idx < numMessages; i++) {
                    ch.basicPublish("", QUEUE_NAME, props, payload);
                    batchCount++;
                    idx++;
                }
                // Wait for all confirms in this batch
                ch.waitForConfirmsOrDie(10_000);
            } catch (Exception e) {
                errors += batchCount;
                try { ch.abort(); } catch (Exception ignored) {}
                // Reconnect
                ch = conn.createChannel();
                ch.confirmSelect();
                continue;
            }

            long batchEndNanos = System.nanoTime();
            long batchLatencyMs = (batchEndNanos - batchStart) / 1_000_000;

            // Assign the same batch latency to all messages in this batch
            // (mimics how kafka-producer-perf-test reports per-record latency)
            for (int i = 0; i < batchCount; i++) {
                latencies[(int)(idx - batchCount + i)] = batchLatencyMs;
            }
        }

        long totalTimeMs = System.currentTimeMillis() - startTime;
        ch.close();
        conn.close();

        // ── Report ───────────────────────────────────────────────────────
        double totalSec  = totalTimeMs / 1000.0;
        long   sent      = numMessages - errors;
        double thruMsg   = sent / totalSec;
        double thruMB    = (sent * (long) MSG_SIZE) / totalSec / (1024.0 * 1024.0);

        System.out.println("─".repeat(60));
        System.out.println("📊  RabbitMQ Producer Performance Report");
        System.out.println("─".repeat(60));
        System.out.printf("%,d records sent, %,.1f records/sec (%.2f MB/sec)%n",
                sent, thruMsg, thruMB);

        if (sent > 0) {
            long[] filled = Arrays.copyOf(latencies, (int) Math.min(sent, latencies.length));
            Arrays.sort(filled);
            double avg = Arrays.stream(filled).average().orElse(0);
            long p50   = percentile(filled, 50);
            long p95   = percentile(filled, 95);
            long p99   = percentile(filled, 99);
            long p999  = percentile(filled, 99.9);
            long max   = filled[filled.length - 1];

            System.out.printf("  avg latency: %.2f ms%n", avg);
            System.out.printf("  max latency: %d ms%n",   max);
            System.out.printf("  p50 latency: %d ms%n",   p50);
            System.out.printf("  p95 latency: %d ms%n",   p95);
            System.out.printf("  p99 latency: %d ms%n",   p99);
            System.out.printf("  p999 latency: %d ms%n",  p999);
        }

        System.out.println("─".repeat(60));
        System.out.printf("  Elapsed      : %.3f s%n",    totalSec);
        System.out.printf("  Sent         : %,d / %,d%n", sent, numMessages);
        System.out.printf("  Errors       : %,d%n",       errors);
        System.out.printf("  Payload      : %,d bytes (1 KB)%n", MSG_SIZE);
        System.out.printf("  Batch size   : %d msgs%n",  batchSize);
        System.out.printf("  Queue type   : quorum (RF=3, majority ACK)%n");
        System.out.println("─".repeat(60));
    }

    static long percentile(long[] sorted, double pct) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
