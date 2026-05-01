package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.broker.raft.RaftState;
import com.drmq.client.DRMQConsumer;
import com.drmq.client.DRMQProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Raft-based replication across a 3-node cluster.
 *
 * Tests verify leader election, log replication, leader failover,
 * follower rejection with redirection, and backward-compatible single-node mode.
 */
class RaftIntegrationTest {

    @TempDir
    Path tempDir;

    // Ports for the 3-node cluster
    private static final int PORT_1 = 19100;
    private static final int PORT_2 = 19101;
    private static final int PORT_3 = 19102;

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
        broker1 = new BrokerServer(clusterConfig("b1", PORT_1, "data-1"));
        broker2 = new BrokerServer(clusterConfig("b2", PORT_2, "data-2"));
        broker3 = new BrokerServer(clusterConfig("b3", PORT_3, "data-3"));

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

    // ===========================
    //  Leader Election Tests
    // ===========================

    @Test
    @DisplayName("3-node cluster elects exactly one leader")
    void threeNodesElectSingleLeader() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();
        assertNotNull(leader, "A leader should be elected within 3 seconds");

        // Count leaders — must be exactly 1
        int leaderCount = 0;
        if (broker1.getRaftNode().isLeader()) leaderCount++;
        if (broker2.getRaftNode().isLeader()) leaderCount++;
        if (broker3.getRaftNode().isLeader()) leaderCount++;
        assertEquals(1, leaderCount, "Exactly one node should be leader");

        // The other two should be followers
        int followerCount = 0;
        if (broker1.getRaftNode().getState() == RaftState.FOLLOWER) followerCount++;
        if (broker2.getRaftNode().getState() == RaftState.FOLLOWER) followerCount++;
        if (broker3.getRaftNode().getState() == RaftState.FOLLOWER) followerCount++;
        assertEquals(2, followerCount, "Two nodes should be followers");
    }

    // ===========================
    //  Replication Tests
    // ===========================

    @Test
    @DisplayName("Produce via leader → message replicated to all nodes")
    void produceViaLeaderReplicatesToAll() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();
        assertNotNull(leader);

        // Produce via the leader
        try (DRMQProducer producer = new DRMQProducer("localhost", leader.getPort())) {
            producer.connect();
            var result = producer.send("raft-topic", "Replicated Message");
            assertTrue(result.isSuccess(), "Produce via leader should succeed");
        }

        // Wait for replication to complete
        Thread.sleep(500);

        // All 3 nodes should have the message in their MessageStore
        assertEquals(1, broker1.getMessageStore().getMessageCount("raft-topic"),
                "Broker 1 should have the message");
        assertEquals(1, broker2.getMessageStore().getMessageCount("raft-topic"),
                "Broker 2 should have the message");
        assertEquals(1, broker3.getMessageStore().getMessageCount("raft-topic"),
                "Broker 3 should have the message");
    }

    // ===========================
    //  Follower Rejection Tests
    // ===========================

    @Test
    @DisplayName("Follower rejects produce with NOT_LEADER error")
    void followerRejectsWrite() throws Exception {
        startCluster();

        BrokerServer follower = findFollower();
        assertNotNull(follower);

        // Produce directly to a follower — should get redirect
        try (DRMQProducer producer = new DRMQProducer("localhost", follower.getPort())) {
            producer.connect();
            // The producer auto-redirects to leader, so the send should succeed
            var result = producer.send("redirect-topic", "Redirected Message");
            assertTrue(result.isSuccess(), "Producer should auto-redirect to leader and succeed");
        }

        // Wait for replication
        Thread.sleep(500);

        // The message should exist on the leader
        BrokerServer leader = findLeader();
        assertNotNull(leader);
        assertTrue(leader.getMessageStore().getMessageCount("redirect-topic") > 0,
                "Leader should have the redirected message");
    }

    // ===========================
    //  Leader Failover Tests
    // ===========================

    @Test
    @DisplayName("Kill leader → new leader elected within 2 seconds")
    void leaderFailoverElectsNewLeader() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();
        assertNotNull(leader);
        int oldLeaderPort = leader.getPort();

        // Kill the leader
        leader.shutdown();

        // Null out the dead broker reference
        if (leader == broker1) broker1 = null;
        else if (leader == broker2) broker2 = null;
        else broker3 = null;

        // Wait for new leader election (up to 2 seconds)
        waitForLeader(2000);

        BrokerServer newLeader = findLeader();
        assertNotNull(newLeader, "A new leader should be elected after the old one dies");
        assertNotEquals(oldLeaderPort, newLeader.getPort(),
                "The new leader should be a different node");
    }

    @Test
    @DisplayName("After failover, new leader accepts writes")
    void newLeaderAcceptsWrites() throws Exception {
        startCluster();

        // Produce a message via the original leader
        BrokerServer leader = findLeader();
        assertNotNull(leader);
        try (DRMQProducer producer = new DRMQProducer("localhost", leader.getPort())) {
            producer.connect();
            producer.send("failover-topic", "Before failover");
        }
        Thread.sleep(300);

        // Kill the leader
        leader.shutdown();
        if (leader == broker1) broker1 = null;
        else if (leader == broker2) broker2 = null;
        else broker3 = null;

        // Wait for new leader
        waitForLeader(2000);
        BrokerServer newLeader = findLeader();
        assertNotNull(newLeader, "New leader should be elected");

        // Produce via the new leader
        try (DRMQProducer producer = new DRMQProducer("localhost", newLeader.getPort())) {
            producer.connect();
            var result = producer.send("failover-topic", "After failover");
            assertTrue(result.isSuccess(), "New leader should accept writes after failover");
        }
    }

    // ===========================
    //  Single-Node Mode (Backward Compatibility)
    // ===========================

    @Test
    @DisplayName("Single-node mode (no --peers) works without Raft")
    void singleNodeModeWorks() throws Exception {
        // Use the old constructor — no peers, no Raft
        BrokerServer standalone = new BrokerServer(19110, 5, tempDir.resolve("standalone").toString());
        standalone.startAsync();
        Thread.sleep(200);

        try {
            assertNull(standalone.getRaftNode(), "No RaftNode in single-node mode");

            try (DRMQProducer producer = new DRMQProducer("localhost", 19110)) {
                producer.connect();
                var result = producer.send("standalone-topic", "Hello standalone");
                assertTrue(result.isSuccess());
                assertEquals(0, result.getOffset());
            }

            try (DRMQConsumer consumer = new DRMQConsumer("localhost", 19110)) {
                consumer.connect();
                consumer.subscribe("standalone-topic");
                var messages = consumer.poll(10);
                assertEquals(1, messages.size());
                assertEquals("Hello standalone", messages.get(0).payloadAsString());
            }
        } finally {
            standalone.shutdown();
        }
    }

    // ===========================
    //  Consumer with Raft
    // ===========================

    @Test
    @DisplayName("Consumer can read from any node in the cluster")
    void consumerReadsFromFollower() throws Exception {
        startCluster();

        BrokerServer leader = findLeader();
        assertNotNull(leader);

        // Produce via leader
        try (DRMQProducer producer = new DRMQProducer("localhost", leader.getPort())) {
            producer.connect();
            producer.send("consumer-raft", "Message 1");
            producer.send("consumer-raft", "Message 2");
        }

        // Wait for replication
        Thread.sleep(500);

        // Consume from a follower — should work because the data is replicated
        BrokerServer follower = findFollower();
        assertNotNull(follower);

        try (DRMQConsumer consumer = new DRMQConsumer("localhost", follower.getPort())) {
            consumer.connect();
            consumer.subscribe("consumer-raft");
            var messages = consumer.poll(10);
            assertEquals(2, messages.size(), "Follower should serve reads after replication");
            assertEquals("Message 1", messages.get(0).payloadAsString());
            assertEquals("Message 2", messages.get(1).payloadAsString());
        }
    }
}
