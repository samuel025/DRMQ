package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerMetrics;
import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Core Raft consensus state machine.
 *
 * Implements leader election, log replication, and commitment as described in
 * "In Search of an Understandable Consensus Algorithm" (Ongaro et al., 2014).
 */
public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    // Raft timing constants — randomized election timeout prevents split votes
    private static final long ELECTION_TIMEOUT_MIN_MS = 150;
    private static final long ELECTION_TIMEOUT_MAX_MS = 300;
    private static final long HEARTBEAT_INTERVAL_MS = 75;

    // Proposal timeout — how long a client blocks waiting for Raft commitment
    private static final long PROPOSAL_TIMEOUT_SECONDS = 5;
    

    private static final long STALE_PROPOSAL_THRESHOLD_MS = 30000;  // 30 seconds
    private static final long PROPOSAL_CLEANUP_INTERVAL_MS = 5000;  // Check every 5 seconds
    private static final int MAX_PENDING_PROPOSALS = 10000;  // Safety limit

    //  Persistent state (survives restart) 
    private long currentTerm;
    private String votedFor;    
    private final RaftLog raftLog;

    //  Volatile state 
    private RaftState state;
    private long commitIndex; 
    private long lastApplied;   
    private String leaderId;  

    // Leader-only volatile state 
    private final Map<String, Long> nextIndex;   
    private final Map<String, Long> matchIndex; 

    private final String nodeId;
    private final int port;
    private final List<PeerAddress> peers;
    private final MessageStore messageStore;
    private final OffsetManager offsetManager;  // For applying replicated offset commits
    private final Path stateFilePath;

    private final Map<String, Function<RequestVoteRequest, RequestVoteResponse>> voteRpcHandlers = new ConcurrentHashMap<>();
    private final Map<String, Function<AppendEntriesRequest, AppendEntriesResponse>> appendRpcHandlers = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService raftExecutor;
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;    private ScheduledFuture<?> proposalCleanupTimer;    private volatile boolean running = false;
    private volatile long electionStartNanos;

    private static class ProposalState {
        final long term;
        final CompletableFuture<Long> future;
        final long createdAtNanos;
        volatile boolean timedOut = false;

        ProposalState(long term, CompletableFuture<Long> future) {
            this.term = term;
            this.future = future;
            this.createdAtNanos = System.nanoTime();
        }
    }
    private final Map<Long, ProposalState> pendingProposals = new ConcurrentHashMap<>();

    private final Map<String, Long> lastLogTime = new ConcurrentHashMap<>();
    private static final long LOG_RATE_LIMIT_MS = 1000;  

    public RaftNode(String nodeId, int port, List<PeerAddress> peers,
                    MessageStore messageStore, OffsetManager offsetManager, Path dataDir) throws IOException {
        this.nodeId = nodeId;
        this.port = port;
        this.peers = peers;
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.raftLog = new RaftLog(dataDir);
        this.state = RaftState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        // Size the pool to handle concurrent RPCs to all peers + a small buffer
        this.raftExecutor = Executors.newFixedThreadPool(
                Math.max(4, peers.size() + 2),
                r -> {
                    Thread t = new Thread(r, "raft-rpc-" + nodeId);
                    t.setDaemon(true);
                    return t;
                });

        Path raftDir = dataDir.resolve("raft");
        Files.createDirectories(raftDir);
        this.stateFilePath = raftDir.resolve("state.properties");
        loadPersistentState();
    }

  
    private boolean shouldLog(String key) {
        long now = System.currentTimeMillis();
        Long lastTime = lastLogTime.get(key);
        if (lastTime == null || (now - lastTime) >= LOG_RATE_LIMIT_MS) {
            lastLogTime.put(key, now);
            return true;
        }
        return false;
    }

   
    public void start() {
        running = true;
        resetElectionTimer();
        startProposalCleanupTask();
        logger.info("[{}] Raft node started (term={}, state=FOLLOWER, peers={})",
                nodeId, currentTerm, peers.size());
    }
    
    /**
     * Start a periodic task to clean up stale pending proposals.
     * Prevents unbounded growth of pendingProposals when replication fails.
     */
    private void startProposalCleanupTask() {
        proposalCleanupTimer = scheduler.scheduleAtFixedRate(
                this::cleanupStaleProposals,
                PROPOSAL_CLEANUP_INTERVAL_MS,
                PROPOSAL_CLEANUP_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }
    
    /**
     * Remove proposals that have been pending for too long.
     * These are proposals that timed out but were never applied.
     */
    private void cleanupStaleProposals() {
        if (!running) return;
        
        long now = System.nanoTime();
        long staleThresholdNanos = TimeUnit.MILLISECONDS.toNanos(STALE_PROPOSAL_THRESHOLD_MS);
        int removed = 0;
        
        for (var iter = pendingProposals.entrySet().iterator(); iter.hasNext(); ) {
            var entry = iter.next();
            ProposalState ps = entry.getValue();
            long ageNanos = now - ps.createdAtNanos;
            
            if (ageNanos > staleThresholdNanos) {
                // Remove stale proposal and fail the future
                iter.remove();
                ps.future.completeExceptionally(
                        new IOException("Proposal removed: stale after " + 
                                TimeUnit.NANOSECONDS.toMillis(ageNanos) + "ms"));
                removed++;
            }
        }
        
        if (removed > 0) {
            logger.warn("[{}] Cleaned up {} stale proposals, {} remaining",
                    nodeId, removed, pendingProposals.size());
        }
    }


    public void stop() {
        running = false;
        if (electionTimer != null) electionTimer.cancel(false);
        if (heartbeatTimer != null) heartbeatTimer.cancel(false);
        if (proposalCleanupTimer != null) proposalCleanupTimer.cancel(false);
        scheduler.shutdownNow();
        raftExecutor.shutdownNow();

        pendingProposals.values().forEach(ps -> ps.future.completeExceptionally(
                new IOException("Raft node shutting down")));
        pendingProposals.clear();

        try {
            raftLog.close();
        } catch (IOException e) {
            logger.error("[{}] Error closing raft log", nodeId, e);
        }
        logger.info("[{}] Raft node stopped", nodeId);
    }


    //  Peer RPC Registration

    /**
     * Register an RPC handler for sending RequestVote to a peer.
     */
    public void registerVoteHandler(String peerId, Function<RequestVoteRequest, RequestVoteResponse> handler) {
        voteRpcHandlers.put(peerId, handler);
    }

    /**
     * Register an RPC handler for sending AppendEntries to a peer.
     */
    public void registerAppendHandler(String peerId, Function<AppendEntriesRequest, AppendEntriesResponse> handler) {
        appendRpcHandlers.put(peerId, handler);
    }

    
    
    //  Election 

    /**
     * Reset the election timer with a random timeout (150–300ms).
     * If the timer fires, the node starts an election.
     */
    private void resetElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        long timeout = ELECTION_TIMEOUT_MIN_MS +
                ThreadLocalRandom.current().nextLong(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
        electionTimer = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Start an election: transition to CANDIDATE, vote for self, request votes from peers.
     */
    private void startElection() {
        lock.lock();
        try {
            if (!running) return;

            electionStartNanos = System.nanoTime();
            currentTerm++;
            state = RaftState.CANDIDATE;
            votedFor = nodeId;
            leaderId = null;
            savePersistentState();

            logger.info("[{}] Starting election for term {}", nodeId, currentTerm);

            long lastLogIndex = raftLog.getLastIndex();
            long lastLogTerm = raftLog.getLastTerm();

            RequestVoteRequest request = RequestVoteRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setCandidateId(nodeId)
                    .setLastLogIndex(lastLogIndex)
                    .setLastLogTerm(lastLogTerm)
                    .build();

            long myTerm = currentTerm;
            int votesNeeded = (peers.size() + 1) / 2 + 1;  
            AtomicLong votesReceived = new AtomicLong(1);   

            // Send RequestVote to all peers asynchronously
            for (PeerAddress peer : peers) {
                CompletableFuture.supplyAsync(() -> {
                    Function<RequestVoteRequest, RequestVoteResponse> handler = voteRpcHandlers.get(peer.id());
                    if (handler == null) return null;
                    try {
                        return handler.apply(request);
                    } catch (Exception e) {
                        return null;
                    }
                }, raftExecutor).thenAcceptAsync(response -> {
                    if (response == null) return;
                    lock.lock();
                    try {
                        if (currentTerm != myTerm || state != RaftState.CANDIDATE) return;

                        if (response.getTerm() > currentTerm) {
                            stepDown(response.getTerm());
                            return;
                        }

                        if (response.getVoteGranted()) {
                            long votes = votesReceived.incrementAndGet();
                            logger.info("[{}] Received vote from {} ({}/{})", nodeId, peer.id(), votes, votesNeeded);
                            if (votes >= votesNeeded) {
                                becomeLeader();
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                });
            }

        } finally {
            lock.unlock();
        }

        // If still candidate after timeout, try again
        resetElectionTimer();
    }

    /**
     * Transition to LEADER: reset peer tracking, start heartbeats.
     */
    private void becomeLeader() {
        state = RaftState.LEADER;
        leaderId = nodeId;

        long electionDuration = recordElectionDuration(true);

        long lastLogIndex = raftLog.getLastIndex();
        for (PeerAddress peer : peers) {
            nextIndex.put(peer.id(), lastLogIndex + 1);
            matchIndex.put(peer.id(), 0L);
        }

        if (electionTimer != null) electionTimer.cancel(false);

        logger.info("[{}] ★ Became LEADER for term {} (lastLogIndex={}, electionMs={})",
            nodeId, currentTerm, lastLogIndex, electionDuration);

        // Send initial heartbeat immediately
        sendHeartbeats();

        // Start periodic heartbeats
        heartbeatTimer = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Step down to FOLLOWER upon discovering a higher term.
     * Also fail all pending proposals from the old term to prevent data loss.
     */
    private void stepDown(long newTerm) {
        boolean wasCandidate = state == RaftState.CANDIDATE;
        long oldTerm = currentTerm;
        logger.info("[{}] Stepping down: term {} → {}", nodeId, oldTerm, newTerm);
        currentTerm = newTerm;
        state = RaftState.FOLLOWER;
        votedFor = null;
        leaderId = null;
        savePersistentState();

        // Fail proposals from the old term to prevent cross-term confusion
        // If we step down, any pending proposals are no longer valid
        pendingProposals.values().stream()
                .filter(ps -> ps.term == oldTerm)
                .forEach(ps -> ps.future.completeExceptionally(
                        new IOException("Lost leadership at term " + oldTerm + "; stepped down to term " + newTerm)));

        if (wasCandidate) {
            recordElectionDuration(false);
        }

        if (heartbeatTimer != null) heartbeatTimer.cancel(false);
        resetElectionTimer();
    }

    //  Heartbeats & Replication 
    /**
     * Leader sends AppendEntries (heartbeat or data) to all peers.
     */
    private void sendHeartbeats() {
        if (state != RaftState.LEADER || !running) return;

        for (PeerAddress peer : peers) {
            CompletableFuture.runAsync(() -> replicateTo(peer), raftExecutor);
        }
    }

    /**
     * Replicate log entries to a single peer.
     */
    private void replicateTo(PeerAddress peer) {
        lock.lock();
        AppendEntriesRequest request;
        try {
            if (state != RaftState.LEADER) return;

            long peerNextIndex = nextIndex.getOrDefault(peer.id(), raftLog.getLastIndex() + 1);
            long prevLogIndex = peerNextIndex - 1;
            long prevLogTerm = raftLog.getTermAt(prevLogIndex);

            List<RaftEntry> entries = raftLog.getEntriesFrom(peerNextIndex);

            request = AppendEntriesRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setLeaderId(nodeId)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .addAllEntries(entries)
                    .setLeaderCommit(commitIndex)
                    .build();
        } finally {
            lock.unlock();
        }

        Function<AppendEntriesRequest, AppendEntriesResponse> handler = appendRpcHandlers.get(peer.id());
        if (handler == null) return;

        AppendEntriesResponse response;
        try {
            response = handler.apply(request);
        } catch (Exception e) {
            if (shouldLog("append_failure_" + peer.id())) {
                logger.debug("[{}] AppendEntries to {} failed: {}", nodeId, peer.id(), e.getMessage());
            }
            return;
        }

        lock.lock();
        try {
            if (state != RaftState.LEADER) return;

            if (response.getTerm() > currentTerm) {
                stepDown(response.getTerm());
                return;
            }

            if (response.getSuccess()) {
                matchIndex.put(peer.id(), response.getMatchIndex());
                nextIndex.put(peer.id(), response.getMatchIndex() + 1);
                advanceCommitIndex();
            } else {
                long current = nextIndex.getOrDefault(peer.id(), 1L);
                long supposedNextIndex = Math.min(current - 1, response.getMatchIndex() + 1);
                nextIndex.put(peer.id(), Math.max(1, supposedNextIndex));
                if (shouldLog("backtrack_" + peer.id())) {
                    logger.debug("[{}] AppendEntries to {} failed, backing nextIndex to {}",
                            nodeId, peer.id(), nextIndex.get(peer.id()));
                }
            }
        } finally {
            lock.unlock();
        }
    }


    private void advanceCommitIndex() {
        long lastIndex = raftLog.getLastIndex();
        for (long n = lastIndex; n > commitIndex; n--) {
            if (raftLog.getTermAt(n) != currentTerm) continue;

            int replicaCount = 1; 
            for (PeerAddress peer : peers) {
                if (matchIndex.getOrDefault(peer.id(), 0L) >= n) {
                    replicaCount++;
                }
            }

            // Majority = floor(clusterSize/2) + 1 (e.g., 3-node cluster needs 2)
            int majority = (peers.size() + 1) / 2 + 1;
            if (replicaCount >= majority) {
                commitIndex = n;
                logger.info("[{}] Advanced commitIndex to {}", nodeId, commitIndex);
                applyCommitted();
                return;
            }
        }
    }



    /**
     * Apply committed but unapplied entries to the state machine.
     * Also completes the CompletableFuture created in propose(), which
     * unblocks the client thread that is waiting for Raft commitment.
     */
    private void applyCommitted() {
        boolean applied = false;

        while (lastApplied < commitIndex) {
            lastApplied++;
            applied = true;
            RaftEntry entry = raftLog.getEntry(lastApplied);
            if (entry == null) {
                logger.error("[{}] Missing raft entry at index {} during apply", nodeId, lastApplied);
                break;
            }

            long completionValue = lastApplied;
            boolean applySucceeded = true;
            Exception applyException = null;

            try {
                switch (entry.getCommandType()) {
                    case OFFSET_COMMIT -> {
                        // Apply offset commit to OffsetManager
                        if (offsetManager != null && entry.hasConsumerGroup() && entry.hasOffsetValue()) {
                            offsetManager.commit(
                                    entry.getConsumerGroup(),
                                    entry.getTopic(),
                                    entry.getOffsetValue()
                            );
                            logger.debug("[{}] Applied offset commit: group={}, topic={}, offset={}",
                                    nodeId, entry.getConsumerGroup(), entry.getTopic(), entry.getOffsetValue());
                        }
                    }
                    default -> {
                        // MESSAGE (default): apply to MessageStore
                        long msgOffset = messageStore.append(
                                entry.getTopic(),
                                entry.getPayload().toByteArray(),
                                entry.hasKey() ? entry.getKey() : null,
                                entry.getTimestamp()
                        );
                        completionValue = msgOffset;
                        logger.debug("[{}] Applied raft entry {} to MessageStore (topic={})",
                                nodeId, lastApplied, entry.getTopic());
                    }
                }
            } catch (Exception e) {
                applySucceeded = false;
                applyException = e;
                logger.error("[{}] Failed to apply entry {} (type={})",
                        nodeId, lastApplied, entry.getCommandType(), e);
            }

            ProposalState ps = pendingProposals.get(lastApplied);
            if (ps != null && ps.term == currentTerm) {
                pendingProposals.remove(lastApplied);
                if (applySucceeded) {
                    ps.future.complete(completionValue);
                    logger.debug("[{}] Completed proposal for entry index {} (term={})",
                            nodeId, lastApplied, ps.term);
                } else {
                    ps.future.completeExceptionally(applyException != null
                            ? applyException
                            : new IOException("Failed to apply entry " + lastApplied));
                }
            } else if (ps != null) {
                pendingProposals.remove(lastApplied);
                logger.warn("[{}] Discarding future for entry {} (was term {}, now term {})",
                        nodeId, lastApplied, ps.term, currentTerm);
            }
        }

        if (applied) {
            savePersistentState();
        }
    }


    /**
     * Propose a new message to be replicated via Raft.
     * Blocks until the entry is committed (majority ACK) or times out.

     */
    public long propose(String topic, byte[] payload, String key, long timestamp) throws IOException {
        lock.lock();
        long index;
        long proposalTerm;
        CompletableFuture<Long> future;
        try {
            if (state != RaftState.LEADER) {
                throw new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN"));
            }

            proposalTerm = currentTerm;
            index = raftLog.getLastIndex() + 1;

            if (pendingProposals.size() >= MAX_PENDING_PROPOSALS) {
                throw new IOException("Too many pending proposals (" + pendingProposals.size()
                        + "/" + MAX_PENDING_PROPOSALS + ")");
            }

            RaftEntry.Builder entryBuilder = RaftEntry.newBuilder()
                    .setTerm(proposalTerm)
                    .setIndex(index)
                    .setTopic(topic)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                    .setTimestamp(timestamp)
                    .setCommandType(RaftCommandType.MESSAGE);

            if (key != null && !key.isEmpty()) {
                entryBuilder.setKey(key);
            }

            RaftEntry entry = entryBuilder.build();
            raftLog.append(entry);
            future = new CompletableFuture<>();
            pendingProposals.put(index, new ProposalState(proposalTerm, future));

        } finally {
            lock.unlock();
        }
        sendHeartbeats();
        try {
            return future.get(PROPOSAL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            ProposalState ps = pendingProposals.get(index);
            if (ps != null) {
                ps.timedOut = true;
                if (pendingProposals.size() > MAX_PENDING_PROPOSALS / 2) {
                    logger.warn("[{}] High number of pending proposals: {} (threshold: {})",
                            nodeId, pendingProposals.size(), MAX_PENDING_PROPOSALS);
                }
            }
            throw new IOException("Raft proposal timed out (index=" + index + "); entry may still commit");
        } catch (ExecutionException e) {
            throw new IOException("Raft proposal failed: " + e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Raft proposal interrupted");
        }
    }

    /**
     * Propose a consumer offset commit to be replicated via Raft.
     * Ensures offset durability across leader failover.
     *
     * @param consumerGroup the consumer group
     * @param topic         the topic
     * @param offset        the next offset to read (last processed + 1)
     * @return the committed Raft log index
     * @throws IOException if not leader, or commitment fails
     */
    public long proposeOffsetCommit(String consumerGroup, String topic, long offset) throws IOException {
        lock.lock();
        long index;
        long proposalTerm;
        CompletableFuture<Long> future;
        try {
            if (state != RaftState.LEADER) {
                throw new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN"));
            }

            proposalTerm = currentTerm;
            index = raftLog.getLastIndex() + 1;

            if (pendingProposals.size() >= MAX_PENDING_PROPOSALS) {
                throw new IOException("Too many pending proposals (" + pendingProposals.size()
                        + "/" + MAX_PENDING_PROPOSALS + ")");
            }

            RaftEntry entry = RaftEntry.newBuilder()
                    .setTerm(proposalTerm)
                    .setIndex(index)
                    .setTopic(topic)
                    .setCommandType(RaftCommandType.OFFSET_COMMIT)
                    .setConsumerGroup(consumerGroup)
                    .setOffsetValue(offset)
                    .build();

            raftLog.append(entry);

            future = new CompletableFuture<>();
            pendingProposals.put(index, new ProposalState(proposalTerm, future));

        } finally {
            lock.unlock();
        }

        sendHeartbeats();

        try {
            return future.get(PROPOSAL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            ProposalState ps = pendingProposals.get(index);
            if (ps != null) {
                ps.timedOut = true;
                if (pendingProposals.size() > MAX_PENDING_PROPOSALS / 2) {
                    logger.warn("[{}] High number of pending proposals: {} (threshold: {})",
                            nodeId, pendingProposals.size(), MAX_PENDING_PROPOSALS);
                }
            }
            throw new IOException("Raft offset commit timed out (index=" + index + "); entry may still commit");
        } catch (ExecutionException e) {
            throw new IOException("Raft offset commit failed: " + e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Raft offset commit interrupted");
        }
    }


    /**
       Handle an incoming RequestVote RPC from a candidate.
     */
    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        lock.lock();
        try {
            // If request term is higher, step down
            if (request.getTerm() > currentTerm) {
                stepDown(request.getTerm());
            }

            boolean voteGranted = false;

            if (request.getTerm() >= currentTerm) {
                boolean canVote = (votedFor == null || votedFor.equals(request.getCandidateId()));
                boolean logOk = isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());

                if (canVote && logOk) {
                    votedFor = request.getCandidateId();
                    savePersistentState();
                    resetElectionTimer(); 
                    voteGranted = true;
                    logger.info("[{}] Granted vote to {} for term {}",
                            nodeId, request.getCandidateId(), request.getTerm());
                }
            }

            return RequestVoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(voteGranted)
                    .build();

        } finally {
            lock.unlock();
        }
    }

    /**
     * Handle an incoming AppendEntries RPC from the leader.
     * Also serves as heartbeat when entries list is empty.
     */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        lock.lock();
        try {
            if (request.getTerm() > currentTerm) {
                stepDown(request.getTerm());
            }

            // Reject if stale term
            if (request.getTerm() < currentTerm) {
                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .setMatchIndex(raftLog.getLastIndex())
                        .build();
            }

            // Valid AppendEntries from current leader — reset election timer
            state = RaftState.FOLLOWER;
            leaderId = request.getLeaderId();
            resetElectionTimer();

            // Log consistency check
            if (request.getPrevLogIndex() > 0) {
                long prevTerm = raftLog.getTermAt(request.getPrevLogIndex());
                if (request.getPrevLogIndex() > raftLog.getLastIndex() || prevTerm != request.getPrevLogTerm()) {
                    return AppendEntriesResponse.newBuilder()
                            .setTerm(currentTerm)
                            .setSuccess(false)
                            .setMatchIndex(raftLog.getLastIndex())
                            .build();
                }
            }

            // Append new entries (handling conflicts by truncation)
            if (!request.getEntriesList().isEmpty()) {
                for (RaftEntry entry : request.getEntriesList()) {
                    long existingTerm = raftLog.getTermAt(entry.getIndex());
                    if (existingTerm != 0 && existingTerm != entry.getTerm()) {
                        try {
                            raftLog.truncateFrom(entry.getIndex());
                        } catch (IOException e) {
                            logger.error("[{}] Failed to truncate raft log at {}", nodeId, entry.getIndex(), e);
                            return AppendEntriesResponse.newBuilder()
                                    .setTerm(currentTerm)
                                    .setSuccess(false)
                                    .setMatchIndex(raftLog.getLastIndex())
                                    .build();
                        }
                    }

                    // Append if new
                    if (entry.getIndex() > raftLog.getLastIndex()) {
                        try {
                            raftLog.append(entry);
                        } catch (IOException e) {
                            logger.error("[{}] Failed to append raft entry at index {}", nodeId, entry.getIndex(), e);
                            return AppendEntriesResponse.newBuilder()
                                    .setTerm(currentTerm)
                                    .setSuccess(false)
                                    .setMatchIndex(raftLog.getLastIndex())
                                    .build();
                        }
                    }
                }
            }

            // Update commitIndex
            if (request.getLeaderCommit() > commitIndex) {
                commitIndex = Math.min(request.getLeaderCommit(), raftLog.getLastIndex());
                applyCommitted();
            }

            return AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .setMatchIndex(raftLog.getLastIndex())
                    .build();

        } finally {
            lock.unlock();
        }
    }


    /**
     Election restriction: only vote for candidates whose log
     * is at least as up-to-date as ours.
     */
    private boolean isLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = raftLog.getLastTerm();
        long myLastIndex = raftLog.getLastIndex();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }


    private void savePersistentState() {
        Path tempPath = null;
        try {
            Properties props = new Properties();
            props.setProperty("currentTerm", String.valueOf(currentTerm));
            props.setProperty("votedFor", votedFor != null ? votedFor : "");
            props.setProperty("lastApplied", String.valueOf(lastApplied));
            tempPath = Files.createTempFile(stateFilePath.getParent(), stateFilePath.getFileName().toString(), ".tmp");
            try (FileOutputStream fos = new FileOutputStream(tempPath.toFile())) {
                props.store(fos, "Raft persistent state");
                fos.getFD().sync();  
            }

            try {
                Files.move(tempPath, stateFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.move(tempPath, stateFilePath, StandardCopyOption.REPLACE_EXISTING);
            }

            Path parentDir = stateFilePath.getParent();
            if (parentDir != null) {
                try (FileChannel dirChannel = FileChannel.open(parentDir, StandardOpenOption.READ)) {
                    dirChannel.force(true);
                }
            }
        } catch (IOException e) {
            logger.error("[{}] Failed to save persistent state", nodeId, e);
        } finally {
            if (tempPath != null) {
                try {
                    Files.deleteIfExists(tempPath);
                } catch (IOException ignored) {
                }
            }
        }
    }

    private void loadPersistentState() throws IOException {
        if (!Files.exists(stateFilePath)) {
            currentTerm = 0;
            votedFor = null;
            lastApplied = 0;
            commitIndex = Math.min(raftLog.getLastIndex(), Math.max(commitIndex, lastApplied));
            return;
        }

        try {
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(stateFilePath.toFile())) {
                props.load(in);
            }
            currentTerm = Long.parseLong(props.getProperty("currentTerm", "0"));
            String vf = props.getProperty("votedFor", "");
            votedFor = vf.isEmpty() ? null : vf;
            lastApplied = Long.parseLong(props.getProperty("lastApplied", "0"));
            commitIndex = Math.min(raftLog.getLastIndex(), Math.max(commitIndex, lastApplied));
            logger.info("[{}] Loaded persistent state: term={}, votedFor={}, lastApplied={}",
                    nodeId, currentTerm, votedFor, lastApplied);
        } catch (IOException | NumberFormatException e) {
            throw new IOException("Failed to load persistent state", e);
        }
    }

    // ===========================
    //  Getters
    // ===========================

    public RaftState getState() { return state; }
    public long getCurrentTerm() { return currentTerm; }
    public String getNodeId() { return nodeId; }
    public String getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public boolean isLeader() { return state == RaftState.LEADER; }

    /**
     * Get the leader's address as "host:port" for client redirection.
     */
    public String getLeaderAddress() {
        if (leaderId == null) return null;
        if (leaderId.equals(nodeId)) return "localhost:" + port;
        for (PeerAddress peer : peers) {
            if (peer.id().equals(leaderId)) {
                return peer.address();
            }
        }
        return null;
    }

    private long recordElectionDuration(boolean won) {
        if (electionStartNanos <= 0) {
            return 0;
        }
        long durationNanos = System.nanoTime() - electionStartNanos;
        electionStartNanos = 0;
        BrokerMetrics.get().recordRaftElection(won, durationNanos);
        return TimeUnit.NANOSECONDS.toMillis(durationNanos);
    }
}
