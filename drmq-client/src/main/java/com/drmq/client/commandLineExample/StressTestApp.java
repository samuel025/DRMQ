package com.drmq.client.commandLineExample;

import com.drmq.client.DRMQProducer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-threaded Stress Test App
 * Spawns multiple threads within a single JVM to hammer the broker
 * with messages. Each thread creates its own DRMQProducer and connection.
 */
public class StressTestApp {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("╔═══════════════════════════════════════════════════╗");
        System.out.println("║        DRMQ Multi-Threaded Stress Test            ║");
        System.out.println("╚═══════════════════════════════════════════════════╝\n");

        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092,localhost:9093,localhost:9094";
        int concurrency;
        int msgSize;
        try {
            concurrency = args.length > 1 ? Integer.parseInt(args[1]) : 4;
            msgSize = args.length > 3 ? Integer.parseInt(args[3]) : 16;
            if (concurrency < 1 || msgSize < 1) {
                throw new NumberFormatException("Concurrency and msgSize must be positive integers");
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: Concurrency and Payload Size must be valid positive integers.");
            System.err.println("Usage: StressTestApp [bootstrapServers] [concurrency] [topic] [msgSize]");
            System.exit(1);
            return;
        }

        String topic = args.length > 2 ? args[2] : "load-test-topic";

        System.out.println("Configuration:");
        System.out.println("  Brokers      : " + bootstrapServers);
        System.out.println("  Concurrency  : " + concurrency + " threads");
        System.out.println("  Topic        : " + topic);
        System.out.println("  Payload Size : " + msgSize + " bytes\n");

        // Generate a fixed byte array payload for speed (no string allocations in tight loop)
        final byte[] payloadBytes = new byte[msgSize];
        for (int i = 0; i < msgSize; i++) {
            payloadBytes[i] = (byte) ('a' + (i % 26));
        }

        AtomicLong messagesSent = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        long startTime = System.currentTimeMillis();

        System.out.println("🚀 Starting load test... Press Ctrl+C to stop.");

        // Start a reporter thread to print metrics every second
        Thread reporter = new Thread(() -> {
            try {
                long lastSent = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1000);
                    long currentSent = messagesSent.get();
                    long currentErrors = errors.get();
                    System.out.printf("[Metrics] %d msgs/sec | Total: %d | Errors: %d\n",
                            (currentSent - lastSent), currentSent, currentErrors);
                    lastSent = currentSent;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        reporter.start();

        java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(8000);

        DRMQProducer[] producers = new DRMQProducer[concurrency];
        for (int i = 0; i < concurrency; i++) {
            producers[i] = new DRMQProducer(bootstrapServers);
            producers[i].setBatchSizeBytes(1 * 1024 * 1024);
            producers[i].setLingerMs(10);
            try {
                producers[i].connect();
            } catch (Exception e) {
                System.err.println("Failed to connect producer " + i + ": " + e.getMessage());
                System.exit(1);
            }
        }

        // Start producer threads
        for (int i = 0; i < concurrency; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    long count = 0;
                    DRMQProducer myProducer = producers[threadId];
                    while (!Thread.currentThread().isInterrupted()) {
                        inFlight.acquire();
                        myProducer.send(topic, payloadBytes).whenComplete((result, ex) -> {
                            inFlight.release();
                            if (ex == null && result.isSuccess()) {
                                messagesSent.incrementAndGet();
                            } else {
                                errors.incrementAndGet();
                            }
                        });
                        count++;
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
            for (DRMQProducer p : producers) {
                if (p != null) p.close();
            }
            executor.shutdownNow();
            reporter.interrupt();
            long totalTimeMs = System.currentTimeMillis() - startTime;
            System.out.printf("\n📊 Final Results:\n");
            System.out.printf("  Total Sent   : %d messages\n", messagesSent.get());
            System.out.printf("  Total Errors : %d\n", errors.get());
            System.out.printf("  Avg Rate     : %.2f msgs/sec\n", (messagesSent.get() * 1000.0) / Math.max(1, totalTimeMs));
        }));

        // Block forever
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
