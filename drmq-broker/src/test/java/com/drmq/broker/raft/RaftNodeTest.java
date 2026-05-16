package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig.PeerAddress;
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
 * Unit tests for RaftNode, heavily isolating the core Raft consensus state machine.
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
        messageStore = new MessageStore(logManager);
        offsetManager = new OffsetManager(tempDir.toString());

        raftNode = new RaftNode(nodeId, 9092, List.of(peer2, peer3), messageStore, offsetManager, tempDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logManager != null) {
            logManager.close();
        }
    }

    @Test
    void initialStateIsFollower() {
        assertEquals(RaftState.FOLLOWER, raftNode.getState());
        assertEquals(0, raftNode.getCurrentTerm());
        assertEquals(nodeId, raftNode.getNodeId());
        assertNull(raftNode.getLeaderId());
    }

    @Test
    void handlesAppendEntriesFromValidLeader() {
        // Simulate an AppendEntries heartbeat from a valid leader
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("node2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
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
        AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("node2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build();
        raftNode.handleAppendEntries(heartbeat);

        // Now receive from older term 1
        AppendEntriesRequest staleRequest = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("node3")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build();
        AppendEntriesResponse response = raftNode.handleAppendEntries(staleRequest);

        assertFalse(response.getSuccess());
        assertEquals(2, response.getTerm());
        assertEquals("node2", raftNode.getLeaderId());
    }

    @Test
    void grantsRequestVoteToValidCandidate() {
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = raftNode.handleRequestVote(request);

        assertTrue(response.getVoteGranted());
        assertEquals(1, response.getTerm());
        assertEquals(1, raftNode.getCurrentTerm());
    }

    @Test
    void rejectsRequestVoteIfAlreadyVotedInSameTerm() {
        RequestVoteRequest request1 = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();
        raftNode.handleRequestVote(request1);

        // Node3 also requests vote in term 1
        RequestVoteRequest request2 = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node3")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();
        RequestVoteResponse response = raftNode.handleRequestVote(request2);

        assertFalse(response.getVoteGranted());
        assertEquals(1, response.getTerm());
    }

    @Test
    void electionTimeoutTransitionsToCandidate() throws InterruptedException {
        AtomicInteger voteRequestsSent = new AtomicInteger(0);

        // Register dummy handlers to count RPCs and simulate a split vote / failure to win
        raftNode.registerVoteHandler("node2", req -> {
            voteRequestsSent.incrementAndGet();
            return RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(false).build();
        });
        raftNode.registerVoteHandler("node3", req -> {
            voteRequestsSent.incrementAndGet();
            return RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(false).build();
        });

        raftNode.start();

        // The election timeout is 150-300ms. Wait for 500ms to ensure it fires.
        Thread.sleep(500);

        // Node should have transitioned to CANDIDATE
        assertEquals(RaftState.CANDIDATE, raftNode.getState());
        assertTrue(raftNode.getCurrentTerm() > 0);
        assertTrue(voteRequestsSent.get() > 0, "Should have sent RequestVote RPCs");
    }

    @Test
    void winsElectionAndBecomesLeader() throws InterruptedException {
        // Register dummy handlers that GRANT the vote
        raftNode.registerVoteHandler("node2", req -> 
            RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(true).build()
        );
        raftNode.registerVoteHandler("node3", req -> 
            RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(true).build()
        );

        // Dummy append handler just acknowledges heartbeats
        raftNode.registerAppendHandler("node2", req -> 
            AppendEntriesResponse.newBuilder().setTerm(req.getTerm()).setSuccess(true).setMatchIndex(req.getEntriesCount()).build()
        );
        raftNode.registerAppendHandler("node3", req -> 
            AppendEntriesResponse.newBuilder().setTerm(req.getTerm()).setSuccess(true).setMatchIndex(req.getEntriesCount()).build()
        );

        raftNode.start();

        // Wait for election timeout
        Thread.sleep(500);

        // Node should have won the election and become LEADER
        assertEquals(RaftState.LEADER, raftNode.getState());
        assertEquals(nodeId, raftNode.getLeaderId());
    }
}
