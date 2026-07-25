package com.drmq.client.commandLineExample;

import com.drmq.client.DRMQProducer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AtomicStressTestApp {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("╔═══════════════════════════════════════════════════╗");
        System.out.println("║    DRMQ Atomic Cross-Topic Stress Test            ║");
        System.out.println("╚═══════════════════════════════════════════════════╝\n");

        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092,localhost:9093,localhost:9094";
        int concurrency;
        try {
            concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        } catch (NumberFormatException e) {
            System.err.println("Error: Concurrency must be a valid positive integer.");
            System.exit(1);
            return;
        }

        System.out.println("Configuration:");
        System.out.println("  Brokers      : " + bootstrapServers);
        System.out.println("  Concurrency  : " + concurrency + " threads");
        System.out.println("  Topics       : Topic-A and Topic-B\n");

        AtomicLong transactionsSent = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        System.out.println("🚀 Starting atomic load test... Press Ctrl+C to stop.");

        Thread reporter = new Thread(() -> {
            try {
                long lastSent = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1000);
                    long currentSent = transactionsSent.get();
                    long currentErrors = errors.get();
                    System.out.printf("[Metrics] %d TPS (Atomic Tx/sec) | Total: %d | Errors: %d\n",
                            (currentSent - lastSent), currentSent, currentErrors);
                    lastSent = currentSent;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        reporter.start();

        final byte[] payloadA = "ATOMIC_PAYLOAD_A".getBytes(StandardCharsets.UTF_8);
        final byte[] payloadB = "ATOMIC_PAYLOAD_B".getBytes(StandardCharsets.UTF_8);

        DRMQProducer sharedProducer = new DRMQProducer(bootstrapServers);
        try {
            sharedProducer.connect();
        } catch (java.io.IOException e) {
            System.err.println("Failed to connect to broker: " + e.getMessage());
            System.exit(1);
        }

        java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(5000);

        for (int i = 0; i < concurrency; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        inFlight.acquire();
                        Map<String, byte[]> atomicBatch = new HashMap<>();
                        atomicBatch.put("Topic-A", payloadA);
                        atomicBatch.put("Topic-B", payloadB);

                        sharedProducer.sendAtomic(atomicBatch).whenComplete((res, ex) -> {
                            inFlight.release();
                            if (ex == null) {
                                transactionsSent.incrementAndGet();
                            } else {
                                errors.incrementAndGet();
                            }
                        });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("[P" + threadId + "] Producer failed: " + e.getMessage());
                }
            });
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Shutting down stress test...");
            sharedProducer.close();
            executor.shutdownNow();
            reporter.interrupt();
            long totalTimeMs = System.currentTimeMillis() - startTime;
            System.out.printf("\n📊 Final Results:\n");
            System.out.printf("  Total Tx Sent: %d transactions\n", transactionsSent.get());
            System.out.printf("  Total Errors : %d\n", errors.get());
            System.out.printf("  Avg Rate     : %.2f TPS\n", (transactionsSent.get() * 1000.0) / Math.max(1, totalTimeMs));
        }));

        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
