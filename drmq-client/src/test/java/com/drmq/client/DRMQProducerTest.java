package com.drmq.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class DRMQProducerTest {

    private DRMQProducer producer;

    @BeforeEach
    void setUp() {
        // Initialize producer with host/port (without opening socket immediately)
        producer = new DRMQProducer("localhost", 19092);
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void testBootstrapServerParsing() {
        DRMQProducer multiServerProducer = new DRMQProducer("127.0.0.1:9092,127.0.0.1:9093");
        assertNotNull(multiServerProducer);
        multiServerProducer.close();
    }

    @Test
    void testInvalidBootstrapServersThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DRMQProducer("   "));
    }

    @Test
    void testSetBatchSizeAndLinger() {
        producer.setBatchSizeBytes(512 * 1024);
        producer.setLingerMs(10);
        // Configuration setters executed successfully
    }

    @Test
    void testSendOverlyLargePayloadFails() {
        byte[] largePayload = new byte[11 * 1024 * 1024]; // 11MB exceeds 10MB limit
        CompletableFuture<DRMQProducer.SendResult> future = producer.send("test-topic", largePayload);
        assertTrue(future.isCompletedExceptionally(), "Payload over 10MB should fail exceptionally");
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    @Test
    void testAccumulatorOverflowBackpressure() throws Exception {
        // Pause background sender threads by setting running = false via reflection
        java.lang.reflect.Field runningField = DRMQProducer.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(producer, false);

        // Fill atomicAccumulator queue to capacity (10,000) using typed PendingAtomicMessage objects
        java.lang.reflect.Field field = DRMQProducer.class.getDeclaredField("atomicAccumulator");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.BlockingQueue<Object> queue = (java.util.concurrent.BlockingQueue<Object>) field.get(producer);

        Class<?> pmClass = Class.forName("com.drmq.client.DRMQProducer$PendingAtomicMessage");
        Constructor<?> ctor = pmClass.getDeclaredConstructor(Map.class, CompletableFuture.class);
        ctor.setAccessible(true);
        Object dummyMsg = ctor.newInstance(Map.of("t1", new byte[0], "t2", new byte[0]), new CompletableFuture<>());

        while (queue.offer(dummyMsg)) {
            // Fill until remaining capacity is 0
        }

        CompletableFuture<Map<String, Long>> future = producer.sendAtomic(Map.of(
            "t1", "v1".getBytes(StandardCharsets.UTF_8),
            "t2", "v2".getBytes(StandardCharsets.UTF_8)
        ));
        assertTrue(future.isCompletedExceptionally(), "Should fail immediately when atomic accumulator is full");
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertTrue(ex.getCause() instanceof IllegalStateException);
        assertTrue(ex.getCause().getMessage().contains("Atomic accumulator is full"));
    }

    @Test
    void testSendAtomicRequiresAtLeastTwoTopics() {
        Map<String, byte[]> singleTopicMap = Map.of(
            "t1", "v1".getBytes(StandardCharsets.UTF_8)
        );
        CompletableFuture<Map<String, Long>> future = producer.sendAtomic(singleTopicMap);
        assertTrue(future.isCompletedExceptionally(), "Should fail when less than 2 topics provided");
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }
}
