package com.drmq.client.commandLineExample;

import com.drmq.client.DRMQProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-threaded Atomic Stress Test App
 * Hammer the broker with cross-topic atomic batch messages.
 */
public class AtomicStressTestApp {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("╔═══════════════════════════════════════════════════╗");
        System.out.println("║    DRMQ Atomic Cross-Topic Stress Test            ║");
        System.out.println("╚═══════════════════════════════════════════════════╝\n");

        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092,localhost:9093,localhost:9094";
        int concurrency;
        int msgSize;
        int numTopics;
        try {
            concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 2;
            numTopics = args.length > 2 ? Integer.parseInt(args[2]) : 3;
            msgSize = args.length > 3 ? Integer.parseInt(args[3]) : 256;
            if (concurrency < 1 || msgSize < 1 || numTopics < 2) {
                throw new NumberFormatException("Concurrency > 0, msgSize > 0, numTopics >= 2");
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: AtomicStressTestApp [bootstrapServers] [concurrency] [numTopics] [msgSize]");
            System.exit(1);
            return;
        }

        System.out.println("Configuration:");
        System.out.println("  Brokers      : " + bootstrapServers);
        System.out.println("  Concurrency  : " + concurrency + " threads");
        System.out.println("  Topics Count : " + numTopics + " topics per atomic batch");
        System.out.println("  Payload Size : " + msgSize + " bytes per topic\n");

        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append((char) ('a' + (Math.random() * 26)));
        }
        final String basePayload = sb.toString();

        AtomicLong batchesSent = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        System.out.println("🚀 Starting atomic load test... Press Ctrl+C to stop.");

        Thread reporter = new Thread(() -> {
            try {
                long lastSent = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1000);
                    long currentSent = batchesSent.get();
                    long currentErrors = errors.get();
                    System.out.printf("[Metrics] %d atomic batches/sec | Total Batches: %d | Total Errors: %d\n",
                            (currentSent - lastSent), currentSent, currentErrors);
                    lastSent = currentSent;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        reporter.start();

        for (int i = 0; i < concurrency; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try (DRMQProducer producer = new DRMQProducer(bootstrapServers)) {
                    producer.connect();
                    long count = 0;
                    while (!Thread.currentThread().isInterrupted()) {
                        java.util.List<CompletableFuture<Map<String, Long>>> futures = new java.util.ArrayList<>(100);
                        
                        // Pipeline 100 atomic requests asynchronously
                        for (int k = 0; k < 100; k++) {
                            Map<String, byte[]> atomicBatch = new HashMap<>();
                            String payload = "T" + threadId + "_" + count + "_" + basePayload;
                            byte[] payloadBytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            
                            for (int t = 0; t < numTopics; t++) {
                                atomicBatch.put("atomic-topic-" + t, payloadBytes);
                            }
                            
                            futures.add(producer.sendAtomic(atomicBatch));
                            count++;
                        }

                        // Wait for the pipeline to drain and record results
                        for (var f : futures) {
                            try {
                                f.join();
                                batchesSent.incrementAndGet();
                            } catch (Exception e) {
                                errors.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                     errors.incrementAndGet();
                     System.err.println("[P" + threadId + "] Producer failed: " + e.getMessage());
                }
            });
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Shutting down atomic stress test...");
            executor.shutdownNow();
            reporter.interrupt();
            long totalTimeMs = System.currentTimeMillis() - startTime;
            System.out.printf("\n📊 Final Results:\n");
            System.out.printf("  Total Batches Sent : %d\n", batchesSent.get());
            System.out.printf("  Total Errors       : %d\n", errors.get());
            System.out.printf("  Avg Rate           : %.2f batches/sec\n", (batchesSent.get() * 1000.0) / Math.max(1, totalTimeMs));
        }));

        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
