package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.protocol.AtomicBatchTopicSlice;
import com.drmq.protocol.ProduceBatchRequest;
import com.drmq.protocol.StoredMessage;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrictReadIsolationTest {

    @TempDir
    Path tempDir;

    private LogManager logManager;
    private MessageStore messageStore;

    @BeforeEach
    void setUp() throws IOException {
        logManager = new LogManager(tempDir.toString());
        BrokerConfig config = new BrokerConfig(9092, tempDir.toString());
        config.setLogSegmentBytes(1024 * 1024); // Large segments to avoid rolling during test
        messageStore = new MessageStore(logManager, config);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (messageStore != null) messageStore.close();
        if (logManager != null) logManager.close();
    }

    @Test
    void testConsumersCannotSeePartialAtomicBatches() throws Exception {
        int numTopics = 3;
        int messagesPerTopic = 1000;
        int numBatches = 50;

        List<String> topics = new ArrayList<>();
        for (int i = 0; i < numTopics; i++) {
            topics.add("topic-" + i);
        }

        AtomicBoolean isRunning = new AtomicBoolean(true);
        AtomicInteger failureCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Consumer thread
        Future<?> consumerFuture = executor.submit(() -> {
            while (isRunning.get()) {
                for (int i = 0; i < numTopics; i++) {
                    String topic = topics.get(i);
                    // Fetch everything available
                    List<StoredMessage> messages = messageStore.getMessages(topic, 0, messagesPerTopic * numBatches * 2);
                    int count = messages.size();
                    // Because each batch writes 'messagesPerTopic' to EVERY topic, the count should always be a multiple of 'messagesPerTopic'
                    if (count % messagesPerTopic != 0) {
                        System.err.println("Dirty read detected on " + topic + ": " + count + " messages (not a multiple of " + messagesPerTopic + ")");
                        failureCount.incrementAndGet();
                        isRunning.set(false);
                    }
                }
            }
        });

        // Producer thread
        Future<?> producerFuture = executor.submit(() -> {
            try {
                for (int b = 0; b < numBatches; b++) {
                    List<AtomicBatchTopicSlice> slices = new ArrayList<>();
                    for (int i = 0; i < numTopics; i++) {
                        AtomicBatchTopicSlice.Builder sliceBuilder = AtomicBatchTopicSlice.newBuilder()
                                .setTopic(topics.get(i));
                        for (int m = 0; m < messagesPerTopic; m++) {
                            sliceBuilder.addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                                    .setPayload(ByteString.copyFromUtf8("msg-" + b + "-" + m))
                                    .setClientTimestamp(System.currentTimeMillis())
                                    .build());
                        }
                        slices.add(sliceBuilder.build());
                    }
                    messageStore.appendAtomicBatch(slices, b + 1);
                    
                    // Yield to give consumer a chance to read
                    Thread.yield();
                }
            } finally {
                isRunning.set(false);
            }
        });

        producerFuture.get(10, TimeUnit.SECONDS);
        consumerFuture.get(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(0, failureCount.get(), "Consumers should never read a partial atomic batch");

        // Verify final state
        for (String topic : topics) {
            List<StoredMessage> finalMessages = messageStore.getMessages(topic, 0, messagesPerTopic * numBatches * 2);
            assertEquals(messagesPerTopic * numBatches, finalMessages.size(), "All messages should be visible after completion");
        }
    }
}
