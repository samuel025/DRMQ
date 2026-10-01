package com.drmq.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DRMQConsumerTest {

    @Test
    void testCommitThrowsInSingleMode() {
        // In single mode (groupMode = false)
        DRMQConsumer consumer = new DRMQConsumer("localhost:19092", null);
        assertFalse(consumer.isGroupMode());

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            consumer.commit("test-topic", 10);
        });

        assertTrue(ex.getMessage().contains("consumer group mode"));
    }

    @Test
    void testSingleModeExplicitConfiguration() {
        DRMQConsumer consumer = new DRMQConsumer("localhost:19092", "my-group");
        assertTrue(consumer.isGroupMode());

        // Switch to single mode
        consumer.setGroupMode(false);
        assertFalse(consumer.isGroupMode());

        assertThrows(IllegalStateException.class, () -> {
            consumer.commit("test-topic", 5);
        });
    }
}
