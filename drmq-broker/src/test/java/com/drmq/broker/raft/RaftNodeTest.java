package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerConfig;
import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.broker.persistence.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.drmq.protocol.*;

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
    //  Incremental Sync Tests
    // ===========================

    @Test
    void handlesIncrementalSnapshotChunkFromValidLeader() {
        // Assume node has some initial state
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        // Send a chunk
        IncrementalSnapshotChunk chunk = IncrementalSnapshotChunk.newBuilder()
                .setTerm(2)
                .setLeaderId("node3")
                .setTopic("test-topic")
                .setFileName("000000.log")
                .setFileOffset(0)
                .setData(com.google.protobuf.ByteString.copyFromUtf8("hello"))
                .setIsLastChunkForFile(false)
                .build();

        IncrementalSnapshotChunkResponse response = raftNode.handleIncrementalSnapshotChunk(chunk);
        assertEquals(2, response.getTerm());
        assertEquals(2, raftNode.getCurrentTerm());
        assertEquals("node3", raftNode.getLeaderId());
        assertEquals(RaftState.FOLLOWER, raftNode.getState());
        assertTrue(response.getSuccess());

        // And the Done request
        IncrementalSnapshotDoneRequest doneReq = IncrementalSnapshotDoneRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("node3")
                .setLastIncludedIndex(50)
                .setLastIncludedTerm(2)
                .build();
        
        IncrementalSnapshotDoneResponse doneResponse = raftNode.handleIncrementalSnapshotDone(doneReq);
        assertEquals(2, doneResponse.getTerm());
        assertTrue(doneResponse.getSuccess());
    }

    @Test
    void rejectsIncrementalSnapshotChunkFromOlderTerm() {
        // Set node term to 3
        raftNode.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(3).setLeaderId("node3").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());

        IncrementalSnapshotChunk chunk = IncrementalSnapshotChunk.newBuilder()
                .setTerm(2)
                .setLeaderId("node2")
                .setTopic("test-topic")
                .setFileName("000000.log")
                .setFileOffset(0)
                .setData(com.google.protobuf.ByteString.copyFromUtf8("hello"))
                .build();

        IncrementalSnapshotChunkResponse response = raftNode.handleIncrementalSnapshotChunk(chunk);

        assertEquals(3, response.getTerm());
        assertEquals(3, raftNode.getCurrentTerm());
        assertEquals("node3", raftNode.getLeaderId(), "Leader should not change on older term request");
        assertFalse(response.getSuccess());
    }

    // ===========================
    //  Compaction Tests
    // ===========================

    @Test
    void testLogCompactionTriggersOnHighCommitIndex() throws Exception {
        // Set raftCompactThreshold to 5 via reflection for fast testing
        java.lang.reflect.Field thresholdField = RaftNode.class.getDeclaredField("raftCompactThreshold");
        thresholdField.setAccessible(true);
        thresholdField.set(raftNode, 5);

        // Access RaftLog via reflection
        java.lang.reflect.Field logField = RaftNode.class.getDeclaredField("raftLog");
        logField.setAccessible(true);
        RaftLog log = (RaftLog) logField.get(raftNode);

        // Append 20 entries to log so that (lastApplied - threshold) - startIndex >= threshold
        java.util.List<RaftEntry> entries = new java.util.ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            entries.add(RaftEntry.newBuilder()
                    .setIndex(i)
                    .setTerm(1)
                    .setTopic("test-topic")
                    .setPayload(com.google.protobuf.ByteString.copyFromUtf8("data-" + i))
                    .setCommandType(RaftCommandType.MESSAGE)
                    .build());
        }
        log.append(entries);

        // Set commitIndex to 20
        java.lang.reflect.Field commitField = RaftNode.class.getDeclaredField("commitIndex");
        commitField.setAccessible(true);
        commitField.set(raftNode, 20L);

        // Invoke private applyCommitted method
        java.lang.reflect.Method applyMethod = RaftNode.class.getDeclaredMethod("applyCommitted");
        applyMethod.setAccessible(true);
        applyMethod.invoke(raftNode);

        // Wait up to 3 seconds for async compaction executor to complete
        long startNanos = System.nanoTime();
        while (log.getStartIndex() <= 1 && System.nanoTime() - startNanos < 3_000_000_000L) {
            Thread.sleep(50);
        }

        assertTrue(log.getStartIndex() > 1, "RaftLog startIndex should advance after compaction");
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
        try { Thread.sleep(2500); } catch (InterruptedException ignored) {}

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
        try { Thread.sleep(2500); } catch (InterruptedException ignored) {}

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
        Thread.sleep(8500);
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
        Thread.sleep(2500);

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
        try { Thread.sleep(2500); } catch (InterruptedException ignored) {}

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
        Thread.sleep(2500);

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
        try { Thread.sleep(2500); } catch (InterruptedException ignored) {}

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
        Thread.sleep(2500);

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
        Thread.sleep(8500);

        assertTrue(preVotesSent.get() > 0, "Should have sent PreVote RPCs before real election");
        assertTrue(votesSent.get() > 0, "Should have sent real RequestVote after pre-vote succeeded");
        assertTrue(raftNode.getCurrentTerm() > 0, "Term should be incremented after pre-vote + election");
    }

    @Test
    void winsElectionAndBecomesLeader() throws InterruptedException {
        registerAllHandlers(true, true, true);
        raftNode.start();
        Thread.sleep(8500);

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
        Thread.sleep(8500);

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
        Thread.sleep(8500);
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
        boolean steppedDown = false;
        for (int i = 0; i < 40; i++) {
            Thread.sleep(500);
            if (raftNode.getState() != RaftState.LEADER) {
                steppedDown = true;
                break;
            }
        }

        assertTrue(steppedDown, "Leader should step down after losing quorum");
    }

    // ===========================
    //  Edge Cases & Backpressure
    // ===========================

    @Test
    void proposalQueueOverflowBackpressure() throws Exception {
        // Set state to LEADER via reflection so proposeAsync passes leader check
        java.lang.reflect.Field stateField = RaftNode.class.getDeclaredField("state");
        stateField.setAccessible(true);
        stateField.set(raftNode, RaftState.LEADER);

        // Access proposalQueue field via reflection
        java.lang.reflect.Field field = RaftNode.class.getDeclaredField("proposalQueue");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.concurrent.BlockingQueue<Object> queue = (java.util.concurrent.BlockingQueue<Object>) field.get(raftNode);

        Class<?> prClass = Class.forName("com.drmq.broker.raft.RaftNode$ProposalRequest");
        java.lang.reflect.Constructor<?> ctor = prClass.getDeclaredConstructor(String.class, java.util.List.class, java.util.concurrent.CompletableFuture.class);
        ctor.setAccessible(true);
        Object dummyReq = ctor.newInstance("test-topic", java.util.List.of(), new java.util.concurrent.CompletableFuture<>());

        // Fill proposalQueue to max capacity (10,000) with dummy ProposalRequest objects
        while (queue.offer(dummyReq)) {
            // fill until full
        }

        // Propose message while queue is full
        java.util.concurrent.CompletableFuture<Long> future =
                raftNode.proposeAsync("test-topic", "overflow-data".getBytes(java.nio.charset.StandardCharsets.UTF_8), null, System.currentTimeMillis());

        assertTrue(future.isCompletedExceptionally(), "Should fail immediately when proposal queue is full");
        java.util.concurrent.ExecutionException ex = assertThrows(java.util.concurrent.ExecutionException.class, future::get);
        assertTrue(ex.getCause().getMessage().contains("Proposal queue full"), "Exception message should indicate queue overflow");
    }

    @Test
    void followerLogTruncationOnConflictingTerm() throws Exception {
        // Appends initial entries at Term 1: Index 1..5
        AppendEntriesRequest reqInitial = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("node2").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0)
                .addEntries(RaftEntry.newBuilder().setTerm(1).setIndex(1).setTopic("t1").setPayload(com.google.protobuf.ByteString.copyFromUtf8("e1")).build())
                .addEntries(RaftEntry.newBuilder().setTerm(1).setIndex(2).setTopic("t1").setPayload(com.google.protobuf.ByteString.copyFromUtf8("e2")).build())
                .addEntries(RaftEntry.newBuilder().setTerm(1).setIndex(3).setTopic("t1").setPayload(com.google.protobuf.ByteString.copyFromUtf8("e3")).build())
                .addEntries(RaftEntry.newBuilder().setTerm(1).setIndex(4).setTopic("t1").setPayload(com.google.protobuf.ByteString.copyFromUtf8("e4")).build())
                .addEntries(RaftEntry.newBuilder().setTerm(1).setIndex(5).setTopic("t1").setPayload(com.google.protobuf.ByteString.copyFromUtf8("e5")).build())
                .build();
        assertTrue(raftNode.handleAppendEntries(reqInitial).getSuccess());
        assertEquals(5, raftNode.getRaftLog().getLastIndex());

        // Now new leader (node3) at Term 2 sends AppendEntries starting at prevLogIndex=2, prevLogTerm=1
        // with new entries for index 3, 4 at Term 2 (conflicting with index 3, 4, 5 at Term 1)
        AppendEntriesRequest reqConflict = AppendEntriesRequest.newBuilder()
                .setTerm(2).setLeaderId("node3").setPrevLogIndex(2).setPrevLogTerm(1).setLeaderCommit(4)
                .addEntries(RaftEntry.newBuilder().setTerm(2).setIndex(3).setTopic("t1").setPayload(com.google.protobuf.ByteString.copyFromUtf8("e3-new")).build())
                .addEntries(RaftEntry.newBuilder().setTerm(2).setIndex(4).setTopic("t1").setPayload(com.google.protobuf.ByteString.copyFromUtf8("e4-new")).build())
                .build();

        AppendEntriesResponse resp = raftNode.handleAppendEntries(reqConflict);
        assertTrue(resp.getSuccess(), "Follower should accept new entries from leader at higher term");
        assertEquals(4, raftNode.getRaftLog().getLastIndex(), "Conflicting entries beyond index 4 should be truncated");
        assertEquals(2, raftNode.getRaftLog().getEntry(3).getTerm(), "Entry 3 should be updated to Term 2");
        assertEquals(2, raftNode.getRaftLog().getEntry(4).getTerm(), "Entry 4 should be updated to Term 2");
        assertNull(raftNode.getRaftLog().getEntry(5), "Entry 5 should be truncated");
    }
}
