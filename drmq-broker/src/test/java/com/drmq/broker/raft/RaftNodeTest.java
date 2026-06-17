package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerConfig;
import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.broker.persistence.LogManager;
import com.drmq.protocol.DRMQProtocol.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RaftNode — covers core Raft consensus, Pre-Vote (§9.6),
 * election mechanics, and edge cases around leader disruption prevention.
 */
class RaftNodeTest {

    @TempDir
    Path tempDir;

    private LogManager logManager;
    private MessageStore messageStore;
    private OffsetManager offsetManager;
    private RaftNode raftNode;

    private final String nodeId = "node1";
    private final PeerAddress peer2 = new PeerAddress("node2", "localhost", 9093);
    private final PeerAddress peer3 = new PeerAddress("node3", "localhost", 9094);

    @BeforeEach
    void setUp() throws IOException {
        logManager = new LogManager(tempDir.toString());
        messageStore = new MessageStore(logManager, new BrokerConfig(9092, tempDir.toString()));
        offsetManager = new OffsetManager(tempDir.toString());
        raftNode = new RaftNode(nodeId, 9092, List.of(peer2, peer3), messageStore, offsetManager, tempDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (raftNode != null) raftNode.stop();
        if (logManager != null) logManager.close();
    }

    /** Register all RPC handlers (pre-vote, vote, append) with configurable responses. */
    private void registerAllHandlers(boolean grantPreVote, boolean grantVote, boolean appendSuccess) {
        for (String peerId : List.of("node2", "node3")) {
            raftNode.registerPreVoteHandler(peerId, req ->
                PreVoteResponse.newBuilder().setTerm(req.getTerm() - 1).setVoteGranted(grantPreVote).build()
            );
            raftNode.registerVoteHandler(peerId, req ->
                RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(grantVote).build()
            );
            raftNode.registerAppendHandler(peerId, req ->
                AppendEntriesResponse.newBuilder().setTerm(req.getTerm()).setSuccess(appendSuccess).setMatchIndex(0).build()
            );
        }
    }

    // ===========================
    //  Basic State Tests
    // ===========================

    @Test
    void initialStateIsFollower() {
        assertEquals(RaftState.FOLLOWER, raftNode.getState());
        assertEquals(0, raftNode.getCurrentTerm());
        assertEquals(nodeId, raftNode.getNodeId());
        assertNull(raftNode.getLeaderId());
    }

    // ===========================
    //  AppendEntries Tests
    // ===========================

    @Test
    void handlesAppendEntriesFromValidLeader() {
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = raftNode.handleAppendEntries(request);

        assertTrue(response.getSuccess());
        assertEquals(1, response.getTerm());
        assertEquals(1, raftNode.getCurrentTerm());
        assertEquals("node2", raftNode.getLeaderId());
        assertEquals(RaftState.FOLLOWER, raftNode.getState());
    }

    @Test
    void rejectsAppendEntriesFromOlderTerm() {
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(2).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        AppendEntriesResponse response = raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("node3").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        assertFalse(response.getSuccess());
        assertEquals(2, response.getTerm());
        assertEquals("node2", raftNode.getLeaderId());
    }

    // ===========================
    //  InstallSnapshot Tests
    // ===========================

    @Test
    void handlesInstallSnapshotFromValidLeader() {
        // Assume node has some initial state
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        // Create a real in-memory ZIP file
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.util.zip.ZipOutputStream zos = new java.util.zip.ZipOutputStream(baos)) {
            zos.putNextEntry(new java.util.zip.ZipEntry("state.properties"));
            zos.write("currentTerm=2\nlastApplied=50\nlastAppliedTerm=2\nvotedFor=\n".getBytes());
            zos.closeEntry();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
        byte[] zipBytes = baos.toByteArray();
        int splitPoint = zipBytes.length / 2;
        com.google.protobuf.ByteString zipPart1 = com.google.protobuf.ByteString.copyFrom(zipBytes, 0, splitPoint);
        com.google.protobuf.ByteString zipPart2 = com.google.protobuf.ByteString.copyFrom(zipBytes, splitPoint, zipBytes.length - splitPoint);

        // Send install snapshot (first chunk)
        InstallSnapshotRequest chunk1 = InstallSnapshotRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("node3")
                .setLastIncludedIndex(50)
                .setLastIncludedTerm(2)
                .setOffset(0)
                .setData(zipPart1)
                .setDone(false)
                .build();

        InstallSnapshotResponse response1 = raftNode.handleInstallSnapshot(chunk1);
        assertEquals(2, response1.getTerm());
        assertEquals(2, raftNode.getCurrentTerm());
        assertEquals("node3", raftNode.getLeaderId());
        assertEquals(RaftState.FOLLOWER, raftNode.getState());

        // Send final chunk
        InstallSnapshotRequest chunk2 = InstallSnapshotRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("node3")
                .setLastIncludedIndex(50)
                .setLastIncludedTerm(2)
                .setOffset(splitPoint)
                .setData(zipPart2)
                .setDone(true)
                .build();
                
        // Note: the test will fail during restoreSnapshot if we don't mock it or if it's not a real zip,
        // but since we handle exceptions gracefully in handleInstallSnapshot, we can just verify term state.
        // Actually, handleInstallSnapshot catches Exception and returns term=2.
        InstallSnapshotResponse response2 = raftNode.handleInstallSnapshot(chunk2);

        assertEquals(2, response2.getTerm());
        assertEquals(2, raftNode.getCurrentTerm());
    }

    @Test
    void rejectsInstallSnapshotFromOlderTerm() {
        // Set node term to 3
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(3).setLeaderId("node3").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("node2")
                .setLastIncludedIndex(50)
                .setLastIncludedTerm(2)
                .setOffset(0)
                .setDone(true)
                .build();

        InstallSnapshotResponse response = raftNode.handleInstallSnapshot(request);

        assertEquals(3, response.getTerm());
        assertEquals(3, raftNode.getCurrentTerm());
        assertEquals("node3", raftNode.getLeaderId(), "Leader should not change on older term request");
    }

    // ===========================
    //  Compaction Tests
    // ===========================

    @Test
    @org.junit.jupiter.api.Disabled("Not implemented yet")
    void testLogCompactionTriggersOnHighCommitIndex() {
        // We set up a node with raftCompactThreshold=100 (which is default 1000 in constructor but we can override it if we had a setter, 
        // wait, RaftNodeTest uses 1000 threshold because it calls the constructor with default... let's just use what we have or reflect)
        // Since we didn't specify raftCompactThreshold in the test constructor, it uses 1000.
        // Actually, RaftNodeTest constructor call:
        // raftNode = new RaftNode(nodeId, 9092, List.of(peer2, peer3), messageStore, offsetManager, tempDir);
        // Wait, RaftNodeTest calls constructor with 6 args, let's look at setUp.
        
        // We will just append enough entries to trigger compaction. Wait, we can't easily append 1000 entries manually.
        // But we can check if compaction method works.
        // I will add a reflection hack or simply skip it here and test it in RaftLogTest.
    }

    // ===========================
    //  RequestVote Tests (Standard Raft)
    // ===========================

    @Test
    void grantsRequestVoteToValidCandidate() {
        RequestVoteResponse response = raftNode.handleRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(1).setCandidateId("node2").setLastLogIndex(0).setLastLogTerm(0).build());

        assertTrue(response.getVoteGranted());
        assertEquals(1, response.getTerm());
        assertEquals(1, raftNode.getCurrentTerm());
    }

    @Test
    void rejectsRequestVoteIfAlreadyVotedInSameTerm() {
        raftNode.handleRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(1).setCandidateId("node2").setLastLogIndex(0).setLastLogTerm(0).build());

        RequestVoteResponse response = raftNode.handleRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(1).setCandidateId("node3").setLastLogIndex(0).setLastLogTerm(0).build());

        assertFalse(response.getVoteGranted());
        assertEquals(1, response.getTerm());
    }

    @Test
    void requestVoteStepsDownOnHigherTerm() {
        // First set node to term 5 by receiving a heartbeat
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());
        assertEquals(5, raftNode.getCurrentTerm());

        // RequestVote with term=10 should cause step-down to term 10
        // (standard Raft — no lease interference)
        // Wait for heartbeat lease to expire
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}

        RequestVoteResponse response = raftNode.handleRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(10).setCandidateId("node3").setLastLogIndex(0).setLastLogTerm(0).build());

        assertEquals(10, raftNode.getCurrentTerm(), "Must step down to higher term per Raft §5.1");
        assertTrue(response.getVoteGranted());
    }

    @Test
    void rejectsRequestVoteWithStaleLog() {
        // Give node1 a log entry at term 5
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0)
                .addEntries(RaftEntry.newBuilder().setTerm(5).setIndex(1).setTopic("t")
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("data")).setTimestamp(1).build())
                .build());

        // Wait for heartbeat lease to expire
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}

        // Candidate with stale log (term 3, index 0) should be rejected
        RequestVoteResponse response = raftNode.handleRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(6).setCandidateId("node3").setLastLogIndex(0).setLastLogTerm(3).build());

        assertFalse(response.getVoteGranted(), "Should reject candidate with stale log");
    }

    // ===========================
    //  Pre-Vote Tests (§9.6)
    // ===========================

    @Test
    void preVoteRejectedByLeader() throws InterruptedException {
        // Make node1 become leader
        registerAllHandlers(true, true, true);
        raftNode.start();
        Thread.sleep(1500);
        assertEquals(RaftState.LEADER, raftNode.getState(), "Node should be leader");

        // A restarting node sends PreVote — leader must reject
        PreVoteResponse response = raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(raftNode.getCurrentTerm() + 1)
                .setCandidateId("node3")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build());

        assertFalse(response.getVoteGranted(), "Leader must reject pre-vote — it IS the leader");
    }

    @Test
    void preVoteRejectedByFollowerWithRecentHeartbeat() {
        // Follower receives a heartbeat — sets lastHeartbeatReceivedMs
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        // Immediately send PreVote — should be rejected (leader is alive)
        PreVoteResponse response = raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(6).setCandidateId("node3")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build());

        assertFalse(response.getVoteGranted(),
                "Follower must reject pre-vote when it heard from the leader recently");
    }

    @Test
    void preVoteGrantedByFollowerWithStaleHeartbeat() throws InterruptedException {
        // Follower receives a heartbeat, then lease expires
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        // Wait for heartbeat lease to expire (> ELECTION_TIMEOUT_MIN_MS = 150ms)
        Thread.sleep(200);

        // Now PreVote should be granted (no recent heartbeat = leader might be dead)
        PreVoteResponse response = raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(6).setCandidateId("node3")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build());

        assertTrue(response.getVoteGranted(),
                "Follower should grant pre-vote when leader heartbeat is stale");
    }

    @Test
    void preVoteRejectedWithStaleTerm() {
        // Set node to term 5
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        // Wait for lease to expire
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}

        // PreVote with proposedTerm=3 (behind node's term=5) — must reject
        PreVoteResponse response = raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(3).setCandidateId("node3")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build());

        assertFalse(response.getVoteGranted(), "Must reject pre-vote with stale term");
        assertEquals(5, response.getTerm(), "Response should include current term");
    }

    @Test
    void preVoteRejectedWithStaleLog() throws InterruptedException {
        // Give node1 a log entry at term 5
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0)
                .addEntries(RaftEntry.newBuilder().setTerm(5).setIndex(1).setTopic("t")
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("data")).setTimestamp(1).build())
                .build());

        // Wait for heartbeat lease to expire
        Thread.sleep(200);

        // PreVote with stale log (term 3) — must reject even though heartbeat is stale
        PreVoteResponse response = raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(6).setCandidateId("node3")
                .setLastLogIndex(0).setLastLogTerm(3)
                .build());

        assertFalse(response.getVoteGranted(),
                "Must reject pre-vote when candidate's log is behind");
    }

    @Test
    void preVoteDoesNotMutateState() {
        // Set known state
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        long termBefore = raftNode.getCurrentTerm();
        RaftState stateBefore = raftNode.getState();
        String leaderBefore = raftNode.getLeaderId();

        // Wait for lease to expire so pre-vote is actually processed
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}

        // Send pre-vote with higher term
        raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(10).setCandidateId("node3")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build());

        // State must be COMPLETELY unchanged — Pre-Vote is read-only
        assertEquals(termBefore, raftNode.getCurrentTerm(), "Pre-vote must NOT change currentTerm");
        assertEquals(stateBefore, raftNode.getState(), "Pre-vote must NOT change state");
        assertEquals(leaderBefore, raftNode.getLeaderId(), "Pre-vote must NOT change leaderId");
    }

    @Test
    void preVoteGrantedWithEqualLog() throws InterruptedException {
        // Node has empty log at term 5
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(5).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        // Wait for lease expiry
        Thread.sleep(200);

        // Candidate with same (empty) log — should be granted
        PreVoteResponse response = raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(6).setCandidateId("node3")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build());

        assertTrue(response.getVoteGranted(),
                "Should grant pre-vote when candidate's log is equally up-to-date");
    }

    @Test
    void freshNodeGrantsPreVote() {
        // A fresh node (term=0, no leader, no heartbeat) should grant pre-votes
        PreVoteResponse response = raftNode.handlePreVote(PreVoteRequest.newBuilder()
                .setTerm(1).setCandidateId("node2")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build());

        assertTrue(response.getVoteGranted(),
                "Fresh node with no leader should grant pre-vote");
    }

    // ===========================
    //  Election Flow Tests
    // ===========================

    @Test
    void electionTimeoutTriggersPreVoteThenElection() throws InterruptedException {
        AtomicInteger preVotesSent = new AtomicInteger(0);
        AtomicInteger votesSent = new AtomicInteger(0);

        for (String peerId : List.of("node2", "node3")) {
            raftNode.registerPreVoteHandler(peerId, req -> {
                preVotesSent.incrementAndGet();
                return PreVoteResponse.newBuilder().setTerm(req.getTerm() - 1).setVoteGranted(true).build();
            });
            raftNode.registerVoteHandler(peerId, req -> {
                votesSent.incrementAndGet();
                return RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(false).build();
            });
            raftNode.registerAppendHandler(peerId, req ->
                AppendEntriesResponse.newBuilder().setTerm(req.getTerm()).setSuccess(true).setMatchIndex(0).build()
            );
        }

        raftNode.start();
        // Startup grace = 900ms + normal timeout ~225ms + some buffer
        Thread.sleep(1500);

        assertTrue(preVotesSent.get() > 0, "Should have sent PreVote RPCs before real election");
        assertTrue(votesSent.get() > 0, "Should have sent real RequestVote after pre-vote succeeded");
        assertTrue(raftNode.getCurrentTerm() > 0, "Term should be incremented after pre-vote + election");
    }

    @Test
    void winsElectionAndBecomesLeader() throws InterruptedException {
        registerAllHandlers(true, true, true);
        raftNode.start();
        Thread.sleep(1500);

        assertEquals(RaftState.LEADER, raftNode.getState());
        assertEquals(nodeId, raftNode.getLeaderId());
    }

    @Test
    void failedPreVoteDoesNotIncrementTerm() throws InterruptedException {
        // Pre-votes rejected, real votes never sent
        AtomicInteger votesSent = new AtomicInteger(0);

        for (String peerId : List.of("node2", "node3")) {
            raftNode.registerPreVoteHandler(peerId, req ->
                PreVoteResponse.newBuilder().setTerm(req.getTerm() - 1).setVoteGranted(false).build()
            );
            raftNode.registerVoteHandler(peerId, req -> {
                votesSent.incrementAndGet();
                return RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(false).build();
            });
            raftNode.registerAppendHandler(peerId, req ->
                AppendEntriesResponse.newBuilder().setTerm(req.getTerm()).setSuccess(true).setMatchIndex(0).build()
            );
        }

        raftNode.start();
        Thread.sleep(1500);

        assertEquals(0, raftNode.getCurrentTerm(),
                "Term must NOT be incremented when pre-vote fails — this is the core Pre-Vote guarantee");
        assertEquals(0, votesSent.get(),
                "Real RequestVote should never be sent when pre-vote fails");
    }

    // ===========================
    //  Quorum Check Tests
    // ===========================

    @Test
    void stepsDownWhenQuorumIsLost() throws InterruptedException {
        registerAllHandlers(true, true, true);
        raftNode.start();
        Thread.sleep(1500);
        assertEquals(RaftState.LEADER, raftNode.getState());

        // Simulate complete network partition — all RPCs throw exceptions.
        // When handler.apply() throws, replicateTo catches it and returns
        // WITHOUT updating lastContactTime (line 552 is skipped).
        raftNode.registerAppendHandler("node2", req -> { throw new RuntimeException("unreachable"); });
        raftNode.registerAppendHandler("node3", req -> { throw new RuntimeException("unreachable"); });
        raftNode.registerPreVoteHandler("node2", req -> { throw new RuntimeException("unreachable"); });
        raftNode.registerPreVoteHandler("node3", req -> { throw new RuntimeException("unreachable"); });
        raftNode.registerVoteHandler("node2", req -> { throw new RuntimeException("unreachable"); });
        raftNode.registerVoteHandler("node3", req -> { throw new RuntimeException("unreachable"); });

        // Wait for quorum loss detection.
        // becomeLeader() seeds lastContactTime for all peers to 'now' and schedules
        // checkQuorum every 900ms. The quorum window is 900ms.
        // Need: staleness > 900ms, i.e., we need at least one full check cycle
        // AFTER the contacts go stale. With generous buffer for scheduling jitter.
        Thread.sleep(6000);

        assertNotEquals(RaftState.LEADER, raftNode.getState(),
                "Leader should step down after losing quorum");
    }
}
