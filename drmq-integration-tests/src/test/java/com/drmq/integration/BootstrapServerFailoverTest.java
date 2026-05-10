package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQConsumer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Bootstrap Server Failover.
 *
 * Tests verify that when one broker dies, the producer and consumer
 * automatically failover to the next bootstrap server and retry.
 */
class BootstrapServerFailoverTest {

    @TempDir
    Path tempDir;

    // Ports for the 3-node cluster
    private static final int PORT_1 = 19200;
    private static final int PORT_2 = 19201;
    private static final int PORT_3 = 19202;

    private BrokerServer broker1;
    private BrokerServer broker2;
    private BrokerServer broker3;

    /**
     * Create a BrokerConfig for one node in a 3-node cluster.
     */
    private BrokerConfig clusterConfig(String nodeId, int port, String dataDirName) {
        List<PeerAddress> peers = switch (nodeId) {
            case "b1" -> List.of(
                    new PeerAddress("b2", "localhost", PORT_2),
                    new PeerAddress("b3", "localhost", PORT_3));
            case "b2" -> List.of(
                    new PeerAddress("b1", "localhost", PORT_1),
                    new PeerAddress("b3", "localhost", PORT_3));
            case "b3" -> List.of(
                    new PeerAddress("b1", "localhost", PORT_1),
                    new PeerAddress("b2", "localhost", PORT_2));
            default -> throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        };
        return new BrokerConfig(nodeId, port, tempDir.resolve(dataDirName).toString(), peers);
    }

    /**
     * Start a 3-node cluster and wait for leader election.
     */
    private void startCluster() throws Exception {
        broker1 = new BrokerServer(clusterConfig("b1", PORT_1, "data-failover-1"));
        broker2 = new BrokerServer(clusterConfig("b2", PORT_2, "data-failover-2"));
        broker3 = new BrokerServer(clusterConfig("b3", PORT_3, "data-failover-3"));

        broker1.startAsync();
        broker2.startAsync();
        broker3.startAsync();

        // Wait for leader election to complete (up to 3 seconds)
        waitForLeader(3000);
    }

    /**
     * Wait until exactly one node is the leader.
     */
    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leaderCount = 0;
            if (broker1 != null && broker1.getRaftNode() != null && broker1.getRaftNode().isLeader()) leaderCount++;
            if (broker2 != null && broker2.getRaftNode() != null && broker2.getRaftNode().isLeader()) leaderCount++;
            if (broker3 != null && broker3.getRaftNode() != null && broker3.getRaftNode().isLeader()) leaderCount++;
            if (leaderCount == 1) return;
            Thread.sleep(100);
        }
    }

    /**
     * Find the current leader broker.
     */
    private BrokerServer findLeader() {
        if (broker1 != null && broker1.getRaftNode() != null && broker1.getRaftNode().isLeader()) return broker1;
        if (broker2 != null && broker2.getRaftNode() != null && broker2.getRaftNode().isLeader()) return broker2;
        if (broker3 != null && broker3.getRaftNode() != null && broker3.getRaftNode().isLeader()) return broker3;
        return null;
    }

    /**
     * Find a follower broker.
     */
    private BrokerServer findFollower() {
        if (broker1 != null && broker1.getRaftNode() != null && !broker1.getRaftNode().isLeader()) return broker1;
        if (broker2 != null && broker2.getRaftNode() != null && !broker2.getRaftNode().isLeader()) return broker2;
        if (broker3 != null && broker3.getRaftNode() != null && !broker3.getRaftNode().isLeader()) return broker3;
        return null;
    }

    @AfterEach
    void tearDown() {
        if (broker1 != null) broker1.shutdown();
        if (broker2 != null) broker2.shutdown();
        if (broker3 != null) broker3.shutdown();
    }

    // =====================================================
    // Producer Failover Tests with Bootstrap Servers
    // =====================================================

    @Test
    @DisplayName("Producer with bootstrap servers connects to first available")
    void producerUsesBootstrapServers() throws Exception {
        startCluster();

        // Create producer with all 3 brokers as bootstrap servers
        String bootstrapServers = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        try (DRMQProducer producer = new DRMQProducer(bootstrapServers)) {
            // Send a message — should connect to one of the brokers and eventually reach leader
            var result = producer.send("bootstrap-test", "Message 1");
            assertTrue(result.isSuccess(), "Producer should connect to one of the bootstrap servers");
        }

        Thread.sleep(300);
        // Message should exist on at least the leader (and be replicated)
        BrokerServer leader = findLeader();
        assertTrue(leader.getMessageStore().getMessageCount("bootstrap-test") > 0);
    }

    @Test
    @DisplayName("Producer retries when first bootstrap server is down")
    void producerRetriesWhenFirstServerDown() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();
        assertNotNull(leader);

        // Determine which broker is at PORT_1
        BrokerServer broker_port1 = null;
        if (broker1 != null && broker1.getPort() == PORT_1) broker_port1 = broker1;
        else if (broker2 != null && broker2.getPort() == PORT_1) broker_port1 = broker2;
        else if (broker3 != null && broker3.getPort() == PORT_1) broker_port1 = broker3;

        assertNotNull(broker_port1, "Should find broker at PORT_1");

        // Shut down the first bootstrap server
        broker_port1.shutdown();
        if (broker_port1 == broker1) broker1 = null;
        else if (broker_port1 == broker2) broker2 = null;
        else broker3 = null;

        // Give time for shutdown
        Thread.sleep(300);

        // Create producer with bootstrap servers starting with the dead one
        String bootstrapServers = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        try (DRMQProducer producer = new DRMQProducer(bootstrapServers)) {
            // This should retry and connect to PORT_2 or PORT_3
            var result = producer.send("failover-test", "Should work despite first server down");
            assertTrue(result.isSuccess(), "Producer should failover to next bootstrap server");
        }

        Thread.sleep(300);
        leader = findLeader();
        if (leader != null) {
            assertTrue(leader.getMessageStore().getMessageCount("failover-test") > 0,
                    "Message should reach the leader");
        }
    }

    @Test
    @DisplayName("Producer retries and sends message after leader dies")
    void producerSendsAfterLeaderDies() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();
        assertNotNull(leader);
        int leaderPort = leader.getPort();

        // Create producer with bootstrap servers
        String bootstrapServers = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;

        // Send a message successfully to establish baseline
        try (DRMQProducer producer = new DRMQProducer(bootstrapServers)) {
            var result = producer.send("baseline", "Baseline message");
            assertTrue(result.isSuccess());
        }

        Thread.sleep(300);

        // Kill the leader
        leader.shutdown();
        if (leader == broker1) broker1 = null;
        else if (leader == broker2) broker2 = null;
        else broker3 = null;

        // Wait for new leader election
        waitForLeader(2000);

        // Now try to send with the same producer bootstrap servers
        // The producer should retry and eventually connect to the new leader
        try (DRMQProducer producer = new DRMQProducer(bootstrapServers)) {
            var result = producer.send("after-failover", "Message after leader dies");
            assertTrue(result.isSuccess(), "Producer should handle leader failover and retry");
        }

        Thread.sleep(300);
        BrokerServer newLeader = findLeader();
        assertNotNull(newLeader);
        assertTrue(newLeader.getMessageStore().getMessageCount("after-failover") > 0,
                "New leader should have the message");
    }

    // =====================================================
    // Consumer Failover Tests with Bootstrap Servers
    // =====================================================

    @Test
    @DisplayName("Consumer with bootstrap servers connects to first available")
    void consumerUsesBootstrapServers() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();

        // Produce some messages
        try (DRMQProducer producer = new DRMQProducer("localhost", leader.getPort())) {
            producer.connect();
            producer.send("consumer-bootstrap", "Message 1");
            producer.send("consumer-bootstrap", "Message 2");
            producer.send("consumer-bootstrap", "Message 3");
        }

        Thread.sleep(300);

        // Create consumer with bootstrap servers
        String bootstrapServers = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        try (DRMQConsumer consumer = new DRMQConsumer(bootstrapServers, "test-group")) {
            consumer.subscribe("consumer-bootstrap");
            var messages = consumer.poll(10);
            assertEquals(3, messages.size(), "Consumer should read messages from one of bootstrap servers");
        }
    }

    @Test
    @DisplayName("Consumer retries when first bootstrap server is down")
    void consumerRetriesWhenFirstServerDown() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();

        // Produce some messages
        try (DRMQProducer producer = new DRMQProducer("localhost", leader.getPort())) {
            producer.connect();
            producer.send("consumer-failover", "Message 1");
            producer.send("consumer-failover", "Message 2");
        }

        Thread.sleep(300);

        // Determine which broker is at PORT_1 and shut it down
        BrokerServer broker_port1 = null;
        if (broker1 != null && broker1.getPort() == PORT_1) broker_port1 = broker1;
        else if (broker2 != null && broker2.getPort() == PORT_1) broker_port1 = broker2;
        else if (broker3 != null && broker3.getPort() == PORT_1) broker_port1 = broker3;

        if (broker_port1 != null) {
            broker_port1.shutdown();
            if (broker_port1 == broker1) broker1 = null;
            else if (broker_port1 == broker2) broker2 = null;
            else broker3 = null;
            Thread.sleep(300);
        }

        // Create consumer with bootstrap servers starting with the dead one
        String bootstrapServers = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        try (DRMQConsumer consumer = new DRMQConsumer(bootstrapServers, "test-group-2")) {
            consumer.subscribe("consumer-failover");
            var messages = consumer.poll(10);
            assertEquals(2, messages.size(), "Consumer should failover and read messages");
        }
    }

    @Test
    @DisplayName("Consumer continues reading after leader changes")
    void consumerContinuesAfterLeaderChange() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();

        // Produce initial messages
        try (DRMQProducer producer = new DRMQProducer("localhost", leader.getPort())) {
            producer.connect();
            producer.send("consumer-continuous", "Message 1");
            producer.send("consumer-continuous", "Message 2");
        }

        Thread.sleep(300);

        // Create consumer and read messages
        String bootstrapServers = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        try (DRMQConsumer consumer = new DRMQConsumer(bootstrapServers, "continuous-group")) {
            consumer.subscribe("consumer-continuous");
            var messages = consumer.poll(10);
            assertEquals(2, messages.size());

            // Now kill the leader
            leader.shutdown();
            if (leader == broker1) broker1 = null;
            else if (leader == broker2) broker2 = null;
            else broker3 = null;

            // Wait for new leader election
            waitForLeader(2000);

            // Produce more messages via the new leader
            BrokerServer newLeader = findLeader();
            assertNotNull(newLeader);
            try (DRMQProducer producer2 = new DRMQProducer("localhost", newLeader.getPort())) {
                producer2.connect();
                producer2.send("consumer-continuous", "Message 3");
                producer2.send("consumer-continuous", "Message 4");
            }

            Thread.sleep(300);

            // Consumer should be able to reconnect and read the new messages
            // (It should have offset 2, so it will get messages 2 and 3)
            messages = consumer.poll(10);
            assertTrue(messages.size() >= 2,
                    "Consumer should read new messages after leader change");
        }
    }
}
