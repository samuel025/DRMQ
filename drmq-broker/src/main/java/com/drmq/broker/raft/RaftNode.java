package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerMetrics;
import com.drmq.broker.MessageStore;
import com.drmq.broker.OffsetManager;
import com.drmq.broker.ClusterEventBuffer;
import com.drmq.protocol.*;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final long ELECTION_TIMEOUT_MIN_MS = 1000;
    private static final long ELECTION_TIMEOUT_MAX_MS = 2000;
    private static final long HEARTBEAT_INTERVAL_MS = 300;

    // Proposal timeout — how long a client blocks waiting for Raft commitment
    private static final long PROPOSAL_TIMEOUT_SECONDS = 60;
    

    private static final long STALE_PROPOSAL_THRESHOLD_MS = 65000;  // 65 seconds (must exceed PROPOSAL_TIMEOUT_SECONDS)
    private static final long PROPOSAL_CLEANUP_INTERVAL_MS = 5000;  // Check every 5 seconds
    private static final int MAX_PENDING_PROPOSALS = 10000;  // Safety limit

    // Batch coalescing constants
    private static final int MAX_AGGREGATION_DRAIN = 512;  // Max proposals per aggregation cycle
    private static final long AGGREGATOR_LINGER_MS = 2;   // Max wait before draining queue (match client lingerMs)

    //  Persistent state (survives restart) 
    private volatile long currentTerm;
    private volatile String votedFor;    
    private final RaftLog raftLog;

    //  Volatile state 
    private volatile RaftState state;
    private volatile long commitIndex; 
    private volatile long lastApplied;   
    private volatile long lastAppliedTerm;
    private volatile String leaderId;  

    // Leader-only volatile state 
    private final Map<String, Long> nextIndex;   
    private final Map<String, Long> matchIndex; 
    private final Map<String, Boolean> snapshotInProgress = new ConcurrentHashMap<>();

    private final String nodeId;
    private final int port;
    private final List<PeerAddress> peers;
    private final MessageStore messageStore;
    private final OffsetManager offsetManager;  
    private final SnapshotManager snapshotManager;
    private final Path dataDir;
    private final Path stateFilePath;
    private final long raftCompactThreshold;

    private final AtomicBoolean isCompacting = new AtomicBoolean(false);

    private final Map<String, Function<RequestVoteRequest, RequestVoteResponse>> voteRpcHandlers = new ConcurrentHashMap<>();
    private final Map<String, Function<AppendEntriesRequest, AppendEntriesResponse>> appendRpcHandlers = new ConcurrentHashMap<>();
    private final Map<String, Function<PreVoteRequest, PreVoteResponse>> preVoteRpcHandlers = new ConcurrentHashMap<>();
    private final Map<String, Function<RequestTopicOffsetsRequest, RequestTopicOffsetsResponse>> requestTopicOffsetsRpcHandlers = new ConcurrentHashMap<>();
    private final Map<String, Function<IncrementalSnapshotChunk, IncrementalSnapshotChunkResponse>> incrementalSnapshotChunkRpcHandlers = new ConcurrentHashMap<>();
    private final Map<String, Function<IncrementalSnapshotDoneRequest, IncrementalSnapshotDoneResponse>> incrementalSnapshotDoneRpcHandlers = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final ExecutorService raftExecutor;
    private final ExecutorService snapshotExecutor;
    private final ExecutorService applyExecutor;
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;    
    private ScheduledFuture<?> proposalCleanupTimer;
    private ScheduledFuture<?> stateSaveTimer;
    private ScheduledFuture<?> quorumCheckTimer;
    private volatile boolean running = false;
    private volatile boolean stateSaveNeeded = false;
    private volatile long electionStartNanos;
    private volatile long lastHeartbeatReceivedMs;
    private volatile boolean startupGrace = true;

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

    /**
     * Holds multiple client futures that were coalesced into a single RaftEntry.
     * When the entry is applied, offsets are distributed sequentially to each constituent.
     */
    private static class AggregatedProposalState extends ProposalState {
        final List<ProposalRequest> constituents;

        AggregatedProposalState(long term, List<ProposalRequest> constituents) {
            // The "master" future is not directly returned to any client;
            // individual ProposalRequest futures are completed in applyCommitted.
            super(term, new CompletableFuture<>());
            this.constituents = constituents;
        }
    }

    /**
     * Represents a single client proposal waiting in the aggregation queue.
     */
    private static class ProposalRequest {
        final String topic;
        final List<ProduceBatchRequest.BatchEntry> entries;
        final CompletableFuture<Long> future;
        final long createdAtNanos;

        ProposalRequest(String topic, List<ProduceBatchRequest.BatchEntry> entries,
                        CompletableFuture<Long> future) {
            this.topic = topic;
            this.entries = entries;
            this.future = future;
            this.createdAtNanos = System.nanoTime();
        }
    }

    private static class AtomicProposalRequest {
        final List<AtomicBatchTopicSlice> slices;
        final CompletableFuture<Map<String, Long>> future;
        final long createdAtNanos;

        AtomicProposalRequest(List<AtomicBatchTopicSlice> slices,
                              CompletableFuture<Map<String, Long>> future) {
            this.slices = slices;
            this.future = future;
            this.createdAtNanos = System.nanoTime();
        }
    }

    private static class AtomicAggregatedProposalState extends ProposalState {
        final List<AtomicProposalRequest> constituents;
        final List<Map<String, Integer>> constituentStartPositions;

        AtomicAggregatedProposalState(long term, List<AtomicProposalRequest> constituents,
                                       List<Map<String, Integer>> constituentStartPositions) {
            super(term, new CompletableFuture<>());
            this.constituents = constituents;
            this.constituentStartPositions = constituentStartPositions;
        }
    }

    private final Map<Long, ProposalState> pendingProposals = new ConcurrentHashMap<>();


    private final Map<String, Long> lastLogTime = new ConcurrentHashMap<>();
    private final Map<String, Long> lastContactTime = new ConcurrentHashMap<>();
    private static final long LOG_RATE_LIMIT_MS = 1000;  

    private long snapshotReceiveOffset = 0;
    private java.io.OutputStream snapshotReceiveStream = null;
    private Path snapshotTempFile = null;
    private long expectedSnapshotIndex = -1;

    private final Map<String, AtomicBoolean> isReplicating;
    private final Map<String, AtomicBoolean> isHeartbeatInFlight = new ConcurrentHashMap<>();

    // Background log appender to prevent disk I/O from starving the consensus lock
    private final java.util.concurrent.LinkedBlockingQueue<Runnable> logAppenderQueue =
            new java.util.concurrent.LinkedBlockingQueue<>(10000);
    private volatile Thread logAppenderThread;
    
    private long uncommittedNextIndex = 1;

    // Proposal aggregation queue — lock-free intake, drained by aggregator thread
    private final LinkedBlockingQueue<ProposalRequest> proposalQueue =
            new LinkedBlockingQueue<>(MAX_PENDING_PROPOSALS);
    private volatile Thread aggregatorThread;

    // Atomic proposal aggregation queue — same pattern, but for cross-topic atomic requests
    private final LinkedBlockingQueue<AtomicProposalRequest> atomicProposalQueue =
            new LinkedBlockingQueue<>(MAX_PENDING_PROPOSALS);
    private volatile Thread atomicAggregatorThread;

    public RaftNode(String nodeId, int port, List<PeerAddress> peers,
                    MessageStore messageStore, OffsetManager offsetManager, Path dataDir,
                    long raftCompactThreshold, boolean raftFsyncEnabled) throws IOException {
        this.nodeId = nodeId;
        this.port = port;
        this.peers = peers;
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.dataDir = dataDir;
        this.snapshotManager = new SnapshotManager(dataDir, messageStore, offsetManager);
        this.raftCompactThreshold = raftCompactThreshold;
        this.raftLog = new RaftLog(dataDir, raftFsyncEnabled);
        this.state = RaftState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.isReplicating = new ConcurrentHashMap<>();
        this.raftExecutor = Executors.newFixedThreadPool(
                Math.max(4, peers.size() + 2),
                r -> {
                    Thread t = new Thread(r, "raft-rpc-" + nodeId);
                    t.setDaemon(true);
                    return t;
                });
        this.snapshotExecutor = Executors.newCachedThreadPool(
                r -> {
                    Thread t = new Thread(r, "raft-snapshot-" + nodeId);
                    t.setDaemon(true);
                    return t;
                });
        this.applyExecutor = Executors.newSingleThreadExecutor(
                r -> {
                    Thread t = new Thread(r, "raft-apply-" + nodeId);
                    t.setDaemon(true);
                    return t;
                });

        Path raftDir = dataDir.resolve("raft");
        Files.createDirectories(raftDir);
        this.stateFilePath = raftDir.resolve("state.properties");
        loadPersistentState();
    }

    public RaftNode(String nodeId, int port, List<PeerAddress> peers,
                    MessageStore messageStore, OffsetManager offsetManager, Path dataDir) throws IOException {
        this(nodeId, port, peers, messageStore, offsetManager, dataDir, 1000L, true);
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
        startupGrace = true;
        resetElectionTimer();
        startProposalCleanupTask();
        
        // Start the proposal aggregator threads
        aggregatorThread = new Thread(this::aggregatorLoop, "raft-aggregator-" + nodeId);
        aggregatorThread.setDaemon(true);
        aggregatorThread.start();

        atomicAggregatorThread = new Thread(this::atomicAggregatorLoop, "raft-atomic-aggregator-" + nodeId);
        atomicAggregatorThread.setDaemon(true);
        atomicAggregatorThread.start();

        logAppenderThread = new Thread(this::logAppenderLoop, "raft-log-appender-" + nodeId);
        logAppenderThread.setDaemon(true);
        logAppenderThread.start();

        stateSaveTimer = scheduler.scheduleAtFixedRate(() -> {
            if (stateSaveNeeded) {
                savePersistentState();
            }
        }, 1, 1, TimeUnit.SECONDS);

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
                iter.remove();
                IOException err = new IOException("Proposal removed: stale after " + 
                        TimeUnit.NANOSECONDS.toMillis(ageNanos) + "ms");
                if (ps instanceof AtomicAggregatedProposalState aaps) {
                    for (AtomicProposalRequest req : aaps.constituents) {
                        req.future.completeExceptionally(err);
                    }
                } else if (ps instanceof AggregatedProposalState aps) {
                    for (ProposalRequest req : aps.constituents) {
                        req.future.completeExceptionally(err);
                    }
                }
                ps.future.completeExceptionally(err);
                removed++;
            }
        }
        
        if (removed > 0) {
            logger.warn("[{}] Cleaned up {} stale proposals, {} remaining",
                    nodeId, removed, pendingProposals.size());
        }
    }

    /**
     * Retrieves and removes chunks of proposals from the concurrent queue, acquiring the ReentrantLock
     * ONCE per batch. It merges payloads of the same topic into a single `ProduceBatchRequest` prior to
     * lock acquisition. This turns N lock acquisitions into 1 per aggregation cycle.
     */
    private void logAppenderLoop() {
        logger.info("[{}] Log appender thread started", nodeId);
        while (running) {
            try {
                Runnable task = logAppenderQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("[{}] Log appender error", nodeId, e);
            }
        }
        logger.info("[{}] Log appender thread stopped", nodeId);
    }

    private void aggregatorLoop() {
        logger.info("[{}] Proposal aggregator thread started", nodeId);
        while (running) {
            try {
                List<ProposalRequest> drained = new ArrayList<>(MAX_AGGREGATION_DRAIN);
                ProposalRequest first = proposalQueue.poll(AGGREGATOR_LINGER_MS, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                drained.add(first);
                proposalQueue.drainTo(drained, MAX_AGGREGATION_DRAIN - 1);

                if (drained.isEmpty()) continue;

                // Check leadership before doing work
                if (state != RaftState.LEADER) {
                    IOException err = new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN"));
                    for (ProposalRequest req : drained) {
                        req.future.completeExceptionally(err);
                    }
                    continue;
                }

                // Group proposals by topic for coalescing
                Map<String, List<ProposalRequest>> byTopic = new LinkedHashMap<>();
                for (ProposalRequest req : drained) {
                    byTopic.computeIfAbsent(req.topic, k -> new ArrayList<>()).add(req);
                }

                // Acquire the Raft lock ONCE for all entries
                lock.lock();
                try {
                    if (state != RaftState.LEADER) {
                        IOException err = new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN"));
                        for (ProposalRequest req : drained) {
                            req.future.completeExceptionally(err);
                        }
                    } else {
                        long proposalTerm = currentTerm;
                        List<RaftEntry> toAppend = new ArrayList<>(byTopic.size());

                        for (var topicEntry : byTopic.entrySet()) {
                            String topic = topicEntry.getKey();
                            List<ProposalRequest> requests = topicEntry.getValue();

                            // Merge all entries from all requests into one ProduceBatchRequest
                            ProduceBatchRequest.Builder merged = ProduceBatchRequest.newBuilder().setTopic(topic);
                            int totalEntries = 0;
                            for (ProposalRequest req : requests) {
                                merged.addAllEntries(req.entries);
                                totalEntries += req.entries.size();
                            }

                            long index = uncommittedNextIndex++;
                            RaftEntry entry = RaftEntry.newBuilder()
                                    .setTerm(proposalTerm)
                                    .setIndex(index)
                                    .setTopic(topic)
                                    .setPayload(merged.build().toByteString())
                                    .setCommandType(RaftCommandType.BATCH_MESSAGE)
                                    .build();

                            toAppend.add(entry);
                            pendingProposals.put(index, new AggregatedProposalState(proposalTerm, requests));

                            if (shouldLog("aggregator-coalesce")) {
                                logger.info("[{}] Aggregator coalesced {} proposals ({} entries) for topic '{}' into raft index {}",
                                        nodeId, requests.size(), totalEntries, topic, index);
                            }
                        }

                        if (!toAppend.isEmpty()) {
                            try {
                                logAppenderQueue.put(() -> {
                                    try {
                                        raftLog.append(toAppend);
                                        sendHeartbeats();
                                    } catch (IOException e) {
                                        logger.error("[{}] Failed to append batch to RaftLog", nodeId, e);
                                        stepDown(proposalTerm);
                                    }
                                });
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("[{}] Aggregator loop error", nodeId, e);
            }
        }
        logger.info("[{}] Proposal aggregator thread stopped", nodeId);
    }

    /**
     * Atomic aggregator thread main loop. Drains the atomicProposalQueue, merges
     * multiple independent atomic requests (each with ≥2 topics) into a single
     * RaftEntry containing all topic slices concatenated per-topic.
     *
     * After commit, offsets are distributed to each constituent based on their
     * position within each topic's merged slice array — exactly like the plain
     * AggregatedProposalState pattern.
     */
    private void atomicAggregatorLoop() {
        logger.info("[{}] Atomic proposal aggregator thread started", nodeId);
        while (running) {
            try {
                List<AtomicProposalRequest> drained = new ArrayList<>(MAX_AGGREGATION_DRAIN);
                AtomicProposalRequest first = atomicProposalQueue.poll(AGGREGATOR_LINGER_MS, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                drained.add(first);
                atomicProposalQueue.drainTo(drained, MAX_AGGREGATION_DRAIN - 1);

                if (drained.isEmpty()) continue;

                if (state != RaftState.LEADER) {
                    IOException err = new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN"));
                    for (AtomicProposalRequest req : drained) {
                        req.future.completeExceptionally(err);
                    }
                    continue;
                }

                // Track per-constituent start positions within each topic's merged slice
                // constituentStartPositions[i] = {topic → startIndexInMergedSlice}
                List<Map<String, Integer>> constituentStartPositions = new ArrayList<>(drained.size());
                for (int i = 0; i < drained.size(); i++) {
                    constituentStartPositions.add(new LinkedHashMap<>());
                }

                // Merge slices: group all entries by topic, concatenating in order
                Map<String, List<ProduceBatchRequest.BatchEntry>> mergedByTopic = new LinkedHashMap<>();

                for (int i = 0; i < drained.size(); i++) {
                    AtomicProposalRequest req = drained.get(i);
                    Map<String, Integer> myPositions = constituentStartPositions.get(i);

                    for (AtomicBatchTopicSlice slice : req.slices) {
                        String topic = slice.getTopic();
                        List<ProduceBatchRequest.BatchEntry> mergedEntries =
                                mergedByTopic.computeIfAbsent(topic, k -> new ArrayList<>());
                        // Record this constituent's start position for this topic
                        if (!myPositions.containsKey(topic)) {
                            myPositions.put(topic, mergedEntries.size());
                        }
                        mergedEntries.addAll(slice.getEntriesList());
                    }
                }

                // Build the merged AtomicBatchRequest slices
                List<AtomicBatchTopicSlice> mergedSlices = new ArrayList<>();
                for (Map.Entry<String, List<ProduceBatchRequest.BatchEntry>> entry : mergedByTopic.entrySet()) {
                    mergedSlices.add(AtomicBatchTopicSlice.newBuilder()
                            .setTopic(entry.getKey())
                            .addAllEntries(entry.getValue())
                            .build());
                }

                // Acquire lock ONCE and append a single Raft entry
                lock.lock();
                try {
                    if (state != RaftState.LEADER) {
                        IOException err = new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN"));
                        for (AtomicProposalRequest req : drained) {
                            req.future.completeExceptionally(err);
                        }
                    } else {
                        long proposalTerm = currentTerm;
                        long index = uncommittedNextIndex++;

                        AtomicBatchRequest payload = AtomicBatchRequest.newBuilder()
                                .addAllSlices(mergedSlices)
                                .build();

                        String topicStr = mergedSlices.stream()
                                .map(AtomicBatchTopicSlice::getTopic)
                                .collect(java.util.stream.Collectors.joining(","));

                        RaftEntry entry = RaftEntry.newBuilder()
                                .setTerm(proposalTerm)
                                .setIndex(index)
                                .setTopic(topicStr)
                                .setPayload(payload.toByteString())
                                .setCommandType(RaftCommandType.ATOMIC_BATCH)
                                .build();

                        pendingProposals.put(index,
                                new AtomicAggregatedProposalState(proposalTerm, drained, constituentStartPositions));

                        if (shouldLog("atomic-aggregator-coalesce")) {
                            logger.info("[{}] Atomic aggregator coalesced {} requests ({} topics) into raft index {}",
                                    nodeId, drained.size(), mergedSlices.size(), index);
                        }

                        try {
                            logAppenderQueue.put(() -> {
                                try {
                                    raftLog.append(entry);
                                    sendHeartbeats();
                                } catch (IOException e) {
                                    logger.error("[{}] Failed to append atomic batch to RaftLog", nodeId, e);
                                    stepDown(proposalTerm);
                                }
                            });
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } finally {
                    lock.unlock();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("[{}] Atomic aggregator loop error", nodeId, e);
            }
        }
        logger.info("[{}] Atomic proposal aggregator thread stopped", nodeId);
    }

    /**
     * Drain all pending proposals from the aggregation queue, completing each
     * future exceptionally with the given reason. Called during stepDown() and stop().
     */
    private void drainProposalQueue(String reason) {
        List<ProposalRequest> remaining = new ArrayList<>();
        proposalQueue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            IOException err = new IOException(reason);
            for (ProposalRequest req : remaining) {
                req.future.completeExceptionally(err);
            }
            logger.info("[{}] Drained {} proposals from aggregation queue: {}", nodeId, remaining.size(), reason);
        }
    }

    private void drainAtomicProposalQueue(String reason) {
        List<AtomicProposalRequest> remaining = new ArrayList<>();
        atomicProposalQueue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            IOException err = new IOException(reason);
            for (AtomicProposalRequest req : remaining) {
                req.future.completeExceptionally(err);
            }
            logger.info("[{}] Drained {} atomic proposals from aggregation queue: {}", nodeId, remaining.size(), reason);
        }
    }

    public void stop() {
        running = false;

        // Stop the aggregator threads and drain any queued proposals
        if (aggregatorThread != null) {
            aggregatorThread.interrupt();
            try { aggregatorThread.join(2000); } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if (atomicAggregatorThread != null) {
            atomicAggregatorThread.interrupt();
            try { atomicAggregatorThread.join(2000); } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        drainProposalQueue("Raft node shutting down");
        drainAtomicProposalQueue("Raft node shutting down");

        if (electionTimer != null) electionTimer.cancel(false);
        if (heartbeatTimer != null) heartbeatTimer.cancel(false);
        if (proposalCleanupTimer != null) proposalCleanupTimer.cancel(false);
        if (stateSaveTimer != null) stateSaveTimer.cancel(false);
        if (quorumCheckTimer != null) quorumCheckTimer.cancel(false);
        scheduler.shutdownNow();
        raftExecutor.shutdownNow();
        applyExecutor.shutdownNow();

        pendingProposals.values().forEach(ps -> {
            ps.future.completeExceptionally(new IOException("Raft node shutting down"));
            if (ps instanceof AtomicAggregatedProposalState aaps) {
                for (AtomicProposalRequest req : aaps.constituents) {
                    req.future.completeExceptionally(new IOException("Raft node shutting down"));
                }
            } else if (ps instanceof AggregatedProposalState aps) {
                for (ProposalRequest req : aps.constituents) {
                    req.future.completeExceptionally(new IOException("Raft node shutting down"));
                }
            }
        });
        pendingProposals.clear();

        try {
            raftLog.close();
        } catch (IOException e) {
            logger.error("[{}] Error closing raft log", nodeId, e);
        }
        logger.info("[{}] Raft node stopped", nodeId);
    }

    public RaftLog getRaftLog() {
        return raftLog;
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

    /**
     * Register an RPC handler for sending PreVote to a peer.
     */
    public void registerPreVoteHandler(String peerId, Function<PreVoteRequest, PreVoteResponse> handler) {
        preVoteRpcHandlers.put(peerId, handler);
    }

    public void registerRequestTopicOffsetsHandler(String peerId, Function<RequestTopicOffsetsRequest, RequestTopicOffsetsResponse> handler) {
        requestTopicOffsetsRpcHandlers.put(peerId, handler);
    }

    public void registerIncrementalSnapshotChunkHandler(String peerId, Function<IncrementalSnapshotChunk, IncrementalSnapshotChunkResponse> handler) {
        incrementalSnapshotChunkRpcHandlers.put(peerId, handler);
    }

    public void registerIncrementalSnapshotDoneHandler(String peerId, Function<IncrementalSnapshotDoneRequest, IncrementalSnapshotDoneResponse> handler) {
        incrementalSnapshotDoneRpcHandlers.put(peerId, handler);
    }

    
    
    //  Election 

    /**
     * Reset the election timer with a random timeout (150–300ms).
     * If the timer fires, the node starts an election.
     */
    private void resetElectionTimer() {
        lock.lock();
        try {
            if (electionTimer != null) {
                electionTimer.cancel(false);
            }
            long timeout;
            if (startupGrace) {
                timeout = ELECTION_TIMEOUT_MAX_MS * 3;
                startupGrace = false;
                logger.info("[{}] Startup grace: election timeout set to {}ms", nodeId, timeout);
            } else {
                timeout = ELECTION_TIMEOUT_MIN_MS +
                        ThreadLocalRandom.current().nextLong(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
            }
            electionTimer = scheduler.schedule(this::startPreVote, timeout, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Pre-Vote phase.
     */
    private void startPreVote() {
        lock.lock();
        long proposedTerm;
        long lastLogIndex;
        long lastLogTerm;
        try {
            if (!running) return;
            if (state == RaftState.LEADER) return;

            proposedTerm = currentTerm + 1;
            lastLogIndex = raftLog.getLastIndex();
            lastLogTerm = raftLog.getLastTerm();

            logger.info("[{}] Starting pre-vote for proposed term {}", nodeId, proposedTerm);
        } finally {
            lock.unlock();
        }

        PreVoteRequest request = PreVoteRequest.newBuilder()
                .setTerm(proposedTerm)
                .setCandidateId(nodeId)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build();

        int votesNeeded = (peers.size() + 1) / 2 + 1;
        AtomicLong votesReceived = new AtomicLong(1); // self-vote
        AtomicBoolean electionStarted = new AtomicBoolean(false);


        for (PeerAddress peer : peers) {
            CompletableFuture.supplyAsync(() -> {
                Function<PreVoteRequest, PreVoteResponse> handler = preVoteRpcHandlers.get(peer.id());
                if (handler == null) return null;
                try {
                    return handler.apply(request);
                } catch (Exception e) {
                    return null;
                }
            }, raftExecutor).thenAcceptAsync(response -> {
                if (response == null) return;
                boolean shouldStartElection = false;
                lock.lock();
                try {
                    if (state == RaftState.LEADER || !running) return;

                    if (response.getTerm() > currentTerm) {
                        stepDown(response.getTerm());
                        return;
                    }

                    if (response.getVoteGranted()) {
                        long votes = votesReceived.incrementAndGet();
                        logger.info("[{}] Received pre-vote from {} ({}/{})",
                                nodeId, peer.id(), votes, votesNeeded);
                        if (votes >= votesNeeded) {
                            if (electionStarted.compareAndSet(false, true)) {
                                logger.info("[{}] Pre-vote succeeded, starting real election", nodeId);
                                shouldStartElection = true;
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }
                if (shouldStartElection) {
                    startElection(proposedTerm);
                }
            }, raftExecutor);
        }

        resetElectionTimer();
    }


    private void startElection(long proposedTerm) {
        if (!running) return;

        long myTerm;
        RequestVoteRequest request;

        lock.lock();
        try {
            if (!running || state == RaftState.LEADER) return;

            if (currentTerm >= proposedTerm) {
                return;
            }

            electionStartNanos = System.nanoTime();
            currentTerm = proposedTerm;
            state = RaftState.CANDIDATE;
            votedFor = nodeId;
            leaderId = null;

            myTerm = currentTerm;

            logger.info("[{}] Starting election for term {}", nodeId, currentTerm);

            long lastLogIndex = raftLog.getLastIndex();
            long lastLogTerm = raftLog.getLastTerm();

            request = RequestVoteRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setCandidateId(nodeId)
                    .setLastLogIndex(lastLogIndex)
                    .setLastLogTerm(lastLogTerm)
                    .build();
        } finally {
            lock.unlock();
        }

        savePersistentState();
        resetElectionTimer();

        int votesNeeded = (peers.size() + 1) / 2 + 1;  
        AtomicLong votesReceived = new AtomicLong(1);   // self-vote
        AtomicBoolean electionWon = new AtomicBoolean(false);


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
                            if (electionWon.compareAndSet(false, true)) {
                                becomeLeader();
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }, raftExecutor);
        }
    }

    /**
     * Transition to LEADER: reset peer tracking, start heartbeats.
     */
    private void becomeLeader() {
        state = RaftState.LEADER;
        leaderId = nodeId;

        long electionDuration = recordElectionDuration(true);

        long lastLogIndex = raftLog.getLastIndex();
        uncommittedNextIndex = lastLogIndex + 1;
        for (PeerAddress peer : peers) {
            nextIndex.put(peer.id(), lastLogIndex + 1);
            matchIndex.put(peer.id(), 0L);
            lastContactTime.put(peer.id(), System.currentTimeMillis());
        }

        if (electionTimer != null) electionTimer.cancel(false);
        if (quorumCheckTimer != null) quorumCheckTimer.cancel(false);

        logger.info("[{}] ★ Became LEADER for term {} (lastLogIndex={}, electionMs={})",
            nodeId, currentTerm, lastLogIndex, electionDuration);
        ClusterEventBuffer.emitElection(String.format("Broker-%s became LEADER for term %d", nodeId, currentTerm));

        sendHeartbeats();

        heartbeatTimer = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        
        long quorumCheckIntervalMs = ELECTION_TIMEOUT_MAX_MS * 3;
        quorumCheckTimer = scheduler.scheduleAtFixedRate(
                this::checkQuorum, quorumCheckIntervalMs, quorumCheckIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Step down to FOLLOWER upon discovering a higher term.
     * Also fail all pending proposals from the old term to prevent data loss.
     */
    private void stepDown(long newTerm) {
        boolean wasCandidate = state == RaftState.CANDIDATE;
        long oldTerm = currentTerm;
        logger.info("[{}] Stepping down: term {} → {}", nodeId, oldTerm, newTerm);
        ClusterEventBuffer.emitElection(String.format("Broker-%s stepped down to term %d", nodeId, newTerm));
        currentTerm = newTerm;
        state = RaftState.FOLLOWER;
        votedFor = null;
        leaderId = null;
        savePersistentState();

        // Drain any queued proposals that haven't been appended yet
        drainProposalQueue("Lost leadership at term " + oldTerm + "; stepped down to term " + newTerm);
        drainAtomicProposalQueue("Lost leadership at term " + oldTerm + "; stepped down to term " + newTerm);

        pendingProposals.values().stream()
                .filter(ps -> ps.term == oldTerm)
                .forEach(ps -> {
                    ps.future.completeExceptionally(
                            new IOException("Lost leadership at term " + oldTerm + "; stepped down to term " + newTerm));
                    if (ps instanceof AtomicAggregatedProposalState aaps) {
                        for (AtomicProposalRequest req : aaps.constituents) {
                            req.future.completeExceptionally(
                                    new IOException("Lost leadership at term " + oldTerm + "; stepped down to term " + newTerm));
                        }
                    } else if (ps instanceof AggregatedProposalState aps) {
                        for (ProposalRequest req : aps.constituents) {
                            req.future.completeExceptionally(
                                    new IOException("Lost leadership at term " + oldTerm + "; stepped down to term " + newTerm));
                        }
                    }
                });

        if (wasCandidate) {
            recordElectionDuration(false);
        }

        if (heartbeatTimer != null) heartbeatTimer.cancel(false);
        if (quorumCheckTimer != null) quorumCheckTimer.cancel(false);
        resetElectionTimer();
    }

    /**
     * Check if the leader has successfully communicated with a majority
     * of the cluster in the last timeout window. Step down if lost.
     */
    private void checkQuorum() {
        lock.lock();
        try {
            if (state != RaftState.LEADER) return;

            long now = System.currentTimeMillis();
            long quorumWindow = ELECTION_TIMEOUT_MAX_MS * 3;
            int activePeers = 1; 

            for (PeerAddress peer : peers) {
                if (snapshotInProgress.getOrDefault(peer.id(), false)) {
                    activePeers++;
                    continue;
                }
                Long lastContact = lastContactTime.get(peer.id());
                if (lastContact != null && (now - lastContact) <= quorumWindow) {
                    activePeers++;
                }
            }

            int majority = (peers.size() + 1) / 2 + 1;
            if (activePeers < majority) {
                logger.warn("[{}] Lost quorum (active: {}, majority: {}). Stepping down to FOLLOWER.",
                        nodeId, activePeers, majority);
                stepDown(currentTerm); 
            }
        } finally {
            lock.unlock();
        }
    }

    //  Heartbeats & Replication 
    /**
     * Leader sends AppendEntries (heartbeat or data) to all peers.
     */
    private void sendHeartbeats() {
        if (state != RaftState.LEADER || !running) return;

        for (PeerAddress peer : peers) {
            if (snapshotInProgress.getOrDefault(peer.id(), false)) {
                // If replication is blocked generating a massive snapshot, the peer lock is free.
                // Send a lightweight heartbeat so the follower's election timer doesn't fire.
                AtomicBoolean heartbeatInFlight = isHeartbeatInFlight.computeIfAbsent(peer.id(), k -> new AtomicBoolean(false));
                if (heartbeatInFlight.compareAndSet(false, true)) {
                    CompletableFuture.runAsync(() -> {
                        try {
                            sendLightweightHeartbeat(peer);
                        } finally {
                            heartbeatInFlight.set(false);
                        }
                    }, raftExecutor);
                }
            } else {
                AtomicBoolean replicating = isReplicating.computeIfAbsent(peer.id(), k -> new AtomicBoolean(false));
                if (replicating.compareAndSet(false, true)) {
                    CompletableFuture.runAsync(() -> {
                        try {
                            replicateTo(peer);
                        } finally {
                            replicating.set(false);
                            // If more entries were appended while replication was in flight,
                            // immediately start a new round instead of waiting for the
                            // 300ms heartbeat timer. This is critical for the atomic
                            // aggregator which appends entries rapidly.
                            if (state == RaftState.LEADER) {
                                long lastIdx = raftLog.getLastIndex();
                                long peerNext = nextIndex.getOrDefault(peer.id(), lastIdx + 1);
                                if (peerNext <= lastIdx) {
                                    sendHeartbeats();
                                }
                            }
                        }
                    }, raftExecutor);
                }
            }
        }
    }

    private void sendLightweightHeartbeat(PeerAddress peer) {
        long peerNextIndex;
        long prevLogIndex;
        long prevLogTerm = 0;
        long currentTermLocal;
        long commitIndexLocal;
        
        lock.lock();
        try {
            if (state != RaftState.LEADER) return;
            currentTermLocal = currentTerm;
            commitIndexLocal = commitIndex;
            peerNextIndex = nextIndex.getOrDefault(peer.id(), raftLog.getLastIndex() + 1);
            prevLogIndex = peerNextIndex - 1;
            
            if (prevLogIndex > 0 && prevLogIndex >= raftLog.getStartIndex() && prevLogIndex <= raftLog.getLastIndex()) {
                prevLogTerm = raftLog.getTermAt(prevLogIndex);
            } else if (prevLogIndex == lastApplied && lastApplied > 0) {
                prevLogTerm = lastAppliedTerm;
            }
        } finally {
            lock.unlock();
        }

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(currentTermLocal)
                .setLeaderId(nodeId)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommit(commitIndexLocal)
                .build();

        java.util.function.Function<AppendEntriesRequest, AppendEntriesResponse> handler = appendRpcHandlers.get(peer.id());
        if (handler != null) {
            try {
                AppendEntriesResponse response = handler.apply(request);
                if (response != null) {
                    lastContactTime.put(peer.id(), System.currentTimeMillis());
                }
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Replicate log entries to a single peer.
     */
    private void replicateTo(PeerAddress peer) {
        boolean needsSnapshot = false;
        long peerNextIndex;
        long prevLogIndex;
        long currentTermLocal;
        long commitIndexLocal;
        String leaderIdLocal;
        
        lock.lock();
        try {
            if (state != RaftState.LEADER) return;

            if (snapshotInProgress.getOrDefault(peer.id(), false)) {
                return;
            }

            peerNextIndex = nextIndex.getOrDefault(peer.id(), raftLog.getLastIndex() + 1);
            prevLogIndex = peerNextIndex - 1;
            
            if (peerNextIndex < raftLog.getStartIndex()) {
                needsSnapshot = true;
                snapshotInProgress.put(peer.id(), true);
                return;
            }

            currentTermLocal = currentTerm;
            commitIndexLocal = commitIndex;
            leaderIdLocal = nodeId;
        } finally {
            lock.unlock();
            if (needsSnapshot) {
                CompletableFuture.runAsync(() -> {
                    syncFollowerTier2(peer);
                }, snapshotExecutor);
            }
        }


        long prevLogTerm = raftLog.getTermAt(prevLogIndex);
        List<RaftEntry> entries = raftLog.getEntriesFrom(peerNextIndex);

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(currentTermLocal)
                .setLeaderId(leaderIdLocal)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .addAllEntries(entries)
                .setLeaderCommit(commitIndexLocal)
                .build();

        Function<AppendEntriesRequest, AppendEntriesResponse> handler = appendRpcHandlers.get(peer.id());
        if (handler == null) return;

        AppendEntriesResponse response;
        try {
            response = handler.apply(request);
            lastContactTime.put(peer.id(), System.currentTimeMillis());
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

    private void syncFollowerTier2(PeerAddress peer) {
        long snapshotIndex;
        long snapshotTerm;
        long term;
        lock.lock();
        try {
            if (state != RaftState.LEADER) return;
            snapshotIndex = lastApplied;
            snapshotTerm = lastAppliedTerm;
            term = currentTerm;
        } finally {
            lock.unlock();
        }

        snapshotInProgress.put(peer.id(), true);
        try {
            Function<RequestTopicOffsetsRequest, RequestTopicOffsetsResponse> offsetHandler = requestTopicOffsetsRpcHandlers.get(peer.id());
            if (offsetHandler == null) return;

            RequestTopicOffsetsRequest req = RequestTopicOffsetsRequest.newBuilder()
                    .setTerm(term)
                    .setLeaderId(nodeId)
                    .build();
            RequestTopicOffsetsResponse offsetResp = offsetHandler.apply(req);

            if (offsetResp.getTerm() > term) {
                lock.lock();
                try {
                    stepDown(offsetResp.getTerm());
                } finally {
                    lock.unlock();
                }
                return;
            }

            Map<String, Long> followerOffsets = offsetResp.getTopicOffsetsMap();

            Function<IncrementalSnapshotChunk, IncrementalSnapshotChunkResponse> chunkHandler = incrementalSnapshotChunkRpcHandlers.get(peer.id());
            Function<IncrementalSnapshotDoneRequest, IncrementalSnapshotDoneResponse> doneHandler = incrementalSnapshotDoneRpcHandlers.get(peer.id());

            if (chunkHandler == null || doneHandler == null) return;

            snapshotManager.streamIncrementalSegments(
                    followerOffsets,
                    snapshotIndex,
                    snapshotTerm,
                    nodeId,
                    peer,
                    chunkHandler,
                    doneHandler
            );

            // If we succeed, advance matchIndex and nextIndex.
            // snapshotIndex was captured at the START of this sync; by now the leader
            // may have compacted many times. Clamp nextIndex to at least the current
            // log start so the follower can receive normal AppendEntries immediately
            // instead of triggering another Tier 2 sync on the very next heartbeat.
            lock.lock();
            try {
                if (state == RaftState.LEADER) {
                    long logStart = raftLog.getStartIndex();
                    long newNextIndex = Math.max(snapshotIndex + 1, logStart);
                    nextIndex.put(peer.id(), newNextIndex);
                    matchIndex.put(peer.id(), newNextIndex - 1);
                    logger.info("[{}] Tier 2 Sync to {} succeeded. NextIndex set to {} (snapshotIndex={}, logStart={})",
                            nodeId, peer.id(), newNextIndex, snapshotIndex, logStart);
                    ClusterEventBuffer.emitSnapshot(String.format("Broker-%s installed Tier 2 sync successfully", peer.id()), peer.id());
                }
            } finally {
                lock.unlock();
            }

        } catch (Exception e) {
            logger.debug("[{}] Tier 2 Sync to {} failed: {}", nodeId, peer.id(), e.getMessage());
        } finally {
            snapshotInProgress.put(peer.id(), false);
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
        applyExecutor.execute(() -> {
            boolean applied = false;
            
            while (lastApplied < commitIndex) {
                Map<String, Long> localAtomicBatchBaseOffsets = null;
                lastApplied++;
                applied = true;
                RaftEntry entry = raftLog.getEntry(lastApplied);
                if (entry == null) {
                    logger.error("[{}] Missing raft entry at index {} during apply", nodeId, lastApplied);
                    break;
                }
                lastAppliedTerm = entry.getTerm();

                long completionValue = lastApplied;
                boolean applySucceeded = true;
                Exception applyException = null;

                try {
                    switch (entry.getCommandType()) {
                        case OFFSET_COMMIT -> {
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
                        case BATCH_MESSAGE -> {
                            ProduceBatchRequest batchRequest = ProduceBatchRequest.parseFrom(entry.getPayload());
                            long baseOffset = messageStore.appendBatch(entry.getTopic(), batchRequest.getEntriesList());
                            completionValue = baseOffset;
                            logger.debug("[{}] Applied raft batch entry {} to MessageStore (topic={}, count={})",
                                    nodeId, lastApplied, entry.getTopic(), batchRequest.getEntriesCount());
                        }
                        case ATOMIC_BATCH -> {
                            com.drmq.protocol.AtomicBatchRequest req = com.drmq.protocol.AtomicBatchRequest.parseFrom(entry.getPayload());
                            Map<String, Long> baseOffsets = messageStore.appendAtomicBatch(req.getSlicesList());
                            localAtomicBatchBaseOffsets = baseOffsets;
                            completionValue = lastApplied;
                            logger.debug("[{}] Applied ATOMIC_BATCH entry {} to {} topics: {}",
                                    nodeId, lastApplied, req.getSlicesCount(),
                                    baseOffsets.keySet());
                        }
                        default -> {
                            long msgOffset = messageStore.append(
                                    entry.getTopic(),
                                    entry.getPayload(),
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
                    logger.error("FATAL: [{}] Failed to apply entry {} (type={}) to MessageStore. Panicking to avoid becoming a zombie node!",
                            nodeId, lastApplied, entry.getCommandType(), e);
                    System.exit(1);
                }

                        // Complete futures — handle simple, aggregated, and atomic-aggregated proposals
                ProposalState ps = pendingProposals.get(lastApplied);
                if (ps != null && ps.term == currentTerm) {
                    pendingProposals.remove(lastApplied);
                    if (applySucceeded) {
                        if (ps instanceof AtomicAggregatedProposalState aaps) {
                            Map<String, Long> baseOffsets = localAtomicBatchBaseOffsets;
                            if (baseOffsets == null) baseOffsets = new java.util.LinkedHashMap<>();
                            for (int i = 0; i < aaps.constituents.size(); i++) {
                                AtomicProposalRequest req = aaps.constituents.get(i);
                                Map<String, Integer> positions = aaps.constituentStartPositions.get(i);
                                Map<String, Long> offsets = new java.util.LinkedHashMap<>();
                                for (AtomicBatchTopicSlice slice : req.slices) {
                                    String topic = slice.getTopic();
                                    Long base = baseOffsets.get(topic);
                                    Integer pos = positions.get(topic);
                                    if (base != null && pos != null) {
                                        offsets.put(topic, base + pos);
                                    }
                                }
                                req.future.complete(offsets);
                            }
                            aaps.future.complete(completionValue);
                            logger.debug("[{}] Completed atomic aggregated proposal for entry index {} ({} constituents, term={})",
                                    nodeId, lastApplied, aaps.constituents.size(), ps.term);
                        } else if (ps instanceof AggregatedProposalState aps) {
                            // Distribute offsets to each constituent future
                            long offset = completionValue; // baseOffset from appendBatch
                            for (ProposalRequest req : aps.constituents) {
                                req.future.complete(offset);
                                offset += req.entries.size();
                            }
                            ps.future.complete(completionValue);
                            logger.debug("[{}] Completed aggregated proposal for entry index {} ({} constituents, term={})",
                                    nodeId, lastApplied, aps.constituents.size(), ps.term);
                        } else {
                            ps.future.complete(completionValue);
                            logger.debug("[{}] Completed proposal for entry index {} (term={})",
                                    nodeId, lastApplied, ps.term);
                        }
                    } else {
                        IOException failure = applyException != null
                                ? new IOException(applyException)
                                : new IOException("Failed to apply entry " + lastApplied);
                        if (ps instanceof AtomicAggregatedProposalState aaps) {
                            for (AtomicProposalRequest req : aaps.constituents) {
                                req.future.completeExceptionally(failure);
                            }
                        } else if (ps instanceof AggregatedProposalState aps) {
                            for (ProposalRequest req : aps.constituents) {
                                req.future.completeExceptionally(failure);
                            }
                        }
                        ps.future.completeExceptionally(failure);
                    }
                } else if (ps != null) {
                    pendingProposals.remove(lastApplied);
                    logger.warn("[{}] Discarding future for entry {} (was term {}, now term {})",
                            nodeId, lastApplied, ps.term, currentTerm);
                    if (ps instanceof AtomicAggregatedProposalState aaps) {
                        for (AtomicProposalRequest req : aaps.constituents) {
                            req.future.completeExceptionally(
                                    new IOException("Term mismatch: entry term " + ps.term + " != current " + currentTerm));
                        }
                    } else if (ps instanceof AggregatedProposalState aps) {
                        for (ProposalRequest req : aps.constituents) {
                            req.future.completeExceptionally(
                                    new IOException("Term mismatch: entry term " + ps.term + " != current " + currentTerm));
                        }
                    }
                }
            }

            if (applied) {
                stateSaveNeeded = true;

                long retentionLimit = lastApplied - (raftCompactThreshold * 2);
                long safeCompactIndex;

                if (isLeader()) {
                    long minMatchIndex = lastApplied;
                    for (long idx : matchIndex.values()) {
                        minMatchIndex = Math.min(minMatchIndex, idx);
                    }
                    safeCompactIndex = Math.max(retentionLimit, minMatchIndex);
                } else {
                    safeCompactIndex = lastApplied;
                }

                long finalCompactIndex = Math.min(safeCompactIndex, lastApplied - raftCompactThreshold);
      
                long currentLogStart = raftLog.getStartIndex();
                boolean compactionDue = finalCompactIndex > 0
                        && (finalCompactIndex - currentLogStart) >= raftCompactThreshold;

                if (compactionDue) {
                    if (isCompacting.compareAndSet(false, true)) {
                        snapshotExecutor.execute(() -> {
                            try {
                                if (messageStore != null) messageStore.forceFlush();
                                if (offsetManager != null) offsetManager.forceFlush();
                                raftLog.compact(finalCompactIndex);
                                logger.debug("[{}] Chunked compaction complete: log now starts at {}",
                                        nodeId, finalCompactIndex + 1);
                            } catch (IOException e) {
                                logger.error("Failed to compact Raft log", e);
                            } finally {
                                isCompacting.set(false);
                            }
                        });
                    }
                }
            }
        });
    }


    /**
     * Propose a new message to be replicated via Raft.
     * Routes through the aggregation queue for batch coalescing.
     */
    public CompletableFuture<Long> proposeAsync(String topic, byte[] payload, String key, long timestamp) {
        return proposeAsync(topic, com.google.protobuf.ByteString.copyFrom(payload), key, timestamp);
    }

    public CompletableFuture<Long> proposeAsync(String topic, com.google.protobuf.ByteString payload, String key, long timestamp) {
        // Quick guard: must be leader (volatile read, no lock)
        if (state != RaftState.LEADER) {
            CompletableFuture<Long> err = new CompletableFuture<>();
            err.completeExceptionally(new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN")));
            return err;
        }

        // Wrap single message as a 1-entry batch and route through aggregation
        ProduceBatchRequest.BatchEntry.Builder batchEntry = ProduceBatchRequest.BatchEntry.newBuilder()
                .setPayload(payload)
                .setClientTimestamp(timestamp);
        if (key != null && !key.isEmpty()) {
            batchEntry.setKey(key);
        }

        CompletableFuture<Long> future = new CompletableFuture<>();
        ProposalRequest req = new ProposalRequest(topic, List.of(batchEntry.build()), future);

        if (!proposalQueue.offer(req)) {
            future.completeExceptionally(new IOException("Proposal queue full (" + proposalQueue.size()
                    + "/" + MAX_PENDING_PROPOSALS + "). Apply backpressure."));
            return future;
        }

        return future.orTimeout(PROPOSAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        throw new java.util.concurrent.CompletionException(
                                new IOException("Raft proposal timed out; entry may still commit"));
                    }
                    throw new java.util.concurrent.CompletionException(
                            new IOException("Raft proposal failed: " + e.getMessage(), e));
                });
    }

    public long propose(String topic, byte[] payload, String key, long timestamp) throws IOException {
        return propose(topic, com.google.protobuf.ByteString.copyFrom(payload), key, timestamp);
    }

    public long propose(String topic, com.google.protobuf.ByteString payload, String key, long timestamp) throws IOException {
        try {
            return proposeAsync(topic, payload, key, timestamp).join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException(e.getCause());
        }
    }

    /**
     * Propose a batch of messages to be replicated via Raft as a single log entry.
     * The entire batch is serialized into one RaftEntry, replicated once, and applied atomically.
     * Blocks until the entry is committed (majority ACK) or times out.
     *
     * @param topic   The target topic
     * @param entries The batch entries from the ProduceBatchRequest
     * @return The base offset (offset of the first message in the batch)
     */
    public CompletableFuture<Long> proposeBatchAsync(String topic, List<ProduceBatchRequest.BatchEntry> entries) {
        // Quick guard: must be leader (volatile read, no lock)
        if (state != RaftState.LEADER) {
            CompletableFuture<Long> err = new CompletableFuture<>();
            err.completeExceptionally(new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN")));
            return err;
        }

        CompletableFuture<Long> future = new CompletableFuture<>();
        ProposalRequest req = new ProposalRequest(topic, entries, future);

        if (!proposalQueue.offer(req)) {
            future.completeExceptionally(new IOException("Proposal queue full (" + proposalQueue.size()
                    + "/" + MAX_PENDING_PROPOSALS + "). Apply backpressure."));
            return future;
        }

        return future.orTimeout(PROPOSAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        throw new java.util.concurrent.CompletionException(
                                new IOException("Raft batch proposal timed out; entry may still commit"));
                    }
                    throw new java.util.concurrent.CompletionException(
                            new IOException("Raft batch proposal failed: " + e.getMessage(), e));
                });
    }

    public long proposeBatch(String topic, List<ProduceBatchRequest.BatchEntry> entries) throws IOException {
        try {
            return proposeBatchAsync(topic, entries).join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException(e.getCause());
        }
    }

    /**
     * Propose a cross-topic atomic batch to be replicated via Raft.
     * All topic writes are applied atomically in applyCommitted().
     *
     * @param slices  list of (topic, entries) pairs — must contain ≥2 topics
     * @return map of topic -> base offset, populated after commit
     */
    public CompletableFuture<Map<String, Long>> proposeAtomicBatchAsync(List<AtomicBatchTopicSlice> slices) {
        if (slices.size() < 2) {
            CompletableFuture<Map<String, Long>> err = new CompletableFuture<>();
            err.completeExceptionally(new IllegalArgumentException("ATOMIC_BATCH requires at least 2 topics. Use proposeBatch() for single-topic."));
            return err;
        }

        if (state != RaftState.LEADER) {
            CompletableFuture<Map<String, Long>> err = new CompletableFuture<>();
            err.completeExceptionally(new IOException("NOT_LEADER:" +
                (leaderId != null ? getLeaderAddress() : "UNKNOWN")));
            return err;
        }

        CompletableFuture<Map<String, Long>> future = new CompletableFuture<>();
        AtomicProposalRequest req = new AtomicProposalRequest(slices, future);

        if (!atomicProposalQueue.offer(req)) {
            future.completeExceptionally(new IOException("Atomic proposal queue full (" + atomicProposalQueue.size()
                    + "/" + MAX_PENDING_PROPOSALS + "). Apply backpressure."));
            return future;
        }

        return future.orTimeout(PROPOSAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        throw new java.util.concurrent.CompletionException(new IOException("Atomic batch proposal timed out; entry may still commit"));
                    }
                    throw new java.util.concurrent.CompletionException(
                            new IOException("Atomic batch proposal failed: " + e.getMessage(), e));
                });
    }

    public Map<String, Long> proposeAtomicBatch(List<AtomicBatchTopicSlice> slices) throws IOException {
        try {
            return proposeAtomicBatchAsync(slices).join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new IOException(e.getCause());
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
    public CompletableFuture<Long> proposeOffsetCommitAsync(String consumerGroup, String topic, long offset) {
        lock.lock();
        long index;
        long proposalTerm;
        CompletableFuture<Long> future;
        try {
            if (state != RaftState.LEADER) {
                CompletableFuture<Long> err = new CompletableFuture<>();
                err.completeExceptionally(new IOException("NOT_LEADER:" + (leaderId != null ? getLeaderAddress() : "UNKNOWN")));
                return err;
            }

            proposalTerm = currentTerm;
            index = raftLog.getLastIndex() + 1;

            if (pendingProposals.size() >= MAX_PENDING_PROPOSALS) {
                CompletableFuture<Long> err = new CompletableFuture<>();
                err.completeExceptionally(new IOException("Too many pending proposals (" + pendingProposals.size()
                        + "/" + MAX_PENDING_PROPOSALS + ")"));
                return err;
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

        } catch (Exception e) {
            CompletableFuture<Long> err = new CompletableFuture<>();
            err.completeExceptionally(e);
            return err;
        } finally {
            lock.unlock();
        }

        sendHeartbeats();
        final long finalIndex = index;
        return future.orTimeout(PROPOSAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        ProposalState ps = pendingProposals.get(finalIndex);
                        if (ps != null) {
                            ps.timedOut = true;
                            if (pendingProposals.size() > MAX_PENDING_PROPOSALS / 2) {
                                logger.warn("[{}] High number of pending proposals: {} (threshold: {})",
                                        nodeId, pendingProposals.size(), MAX_PENDING_PROPOSALS);
                            }
                        }
                        throw new java.util.concurrent.CompletionException(new IOException("Raft offset commit timed out (index=" + finalIndex + "); entry may still commit"));
                    }
                    throw new java.util.concurrent.CompletionException(new IOException("Raft offset commit failed: " + e.getMessage(), e));
                });
    }

    public long proposeOffsetCommit(String consumerGroup, String topic, long offset) throws IOException {
        try {
            return proposeOffsetCommitAsync(consumerGroup, topic, offset).join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException(e.getCause());
        }
    }


    /**
       Handle an incoming RequestVote RPC from a candidate.
     */
    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        lock.lock();
        try {
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
     * Handle an incoming PreVote RPC from a candidate.
     */
    public PreVoteResponse handlePreVote(PreVoteRequest request) {
        lock.lock();
        try {
            if (request.getTerm() <= currentTerm) {
                return PreVoteResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setVoteGranted(false)
                        .build();
            }

            boolean leaderAlive = (state == RaftState.LEADER)
                    || (lastHeartbeatReceivedMs > 0
                        && (System.currentTimeMillis() - lastHeartbeatReceivedMs) < ELECTION_TIMEOUT_MAX_MS);

            if (leaderAlive) {
                logger.info("[{}] Rejecting pre-vote for {} (term {}) — leader is alive (state={})",
                        nodeId, request.getCandidateId(), request.getTerm(), state);
                return PreVoteResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setVoteGranted(false)
                        .build();
            }

            boolean logOk = isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());

            if (logOk) {
                logger.info("[{}] Granted pre-vote to {} for proposed term {}",
                        nodeId, request.getCandidateId(), request.getTerm());
            }

            return PreVoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(logOk)
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

            state = RaftState.FOLLOWER;
            leaderId = request.getLeaderId();
            lastHeartbeatReceivedMs = System.currentTimeMillis();
            resetElectionTimer();

            if (request.getPrevLogIndex() > 0) {
                long prevTerm = raftLog.getTermAt(request.getPrevLogIndex());
                if (request.getPrevLogIndex() > raftLog.getLastIndex() || 
                   (prevTerm != 0 && prevTerm != request.getPrevLogTerm())) {
                    return AppendEntriesResponse.newBuilder()
                            .setTerm(currentTerm)
                            .setSuccess(false)
                            .setMatchIndex(raftLog.getLastIndex())
                            .build();
                }
            }

            if (!request.getEntriesList().isEmpty()) {
                List<RaftEntry> newEntries = new ArrayList<>();
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

                    long expectedIndex = raftLog.getLastIndex() + newEntries.size() + 1;
                    if (entry.getIndex() == expectedIndex) {
                        newEntries.add(entry);
                    } else if (entry.getIndex() > expectedIndex) {
                        logger.error("[{}] Detected gap in AppendEntries: expected {}, got {}", nodeId, expectedIndex, entry.getIndex());
                        return AppendEntriesResponse.newBuilder()
                                .setTerm(currentTerm)
                                .setSuccess(false)
                                .setMatchIndex(raftLog.getLastIndex())
                                .build();
                    }
                }

                if (!newEntries.isEmpty()) {
                    try {
                        raftLog.append(newEntries);
                    } catch (IOException e) {
                        logger.error("[{}] Failed to append batch of {} raft entries", nodeId, newEntries.size(), e);
                        return AppendEntriesResponse.newBuilder()
                                .setTerm(currentTerm)
                                .setSuccess(false)
                                .setMatchIndex(raftLog.getLastIndex())
                                .build();
                    }
                }
            }

            // Update commitIndex
            if (request.getLeaderCommit() > commitIndex) {
                commitIndex = Math.min(request.getLeaderCommit(), raftLog.getLastIndex());
                applyCommitted();
            }

            long matchIdx = request.getPrevLogIndex() + request.getEntriesCount();
            return AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .setMatchIndex(matchIdx)
                    .build();

        } finally {
            lock.unlock();
        }
    }

    /**
     * Handle an incoming Tier 2 Incremental Sync chunk.
     */
    public IncrementalSnapshotChunkResponse handleIncrementalSnapshotChunk(IncrementalSnapshotChunk request) {
        lock.lock();
        try {
            if (request.getTerm() > currentTerm) {
                stepDown(request.getTerm());
            }

            if (request.getTerm() < currentTerm) {
                return IncrementalSnapshotChunkResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();
            }

            resetElectionTimer();
            leaderId = request.getLeaderId();
            state = RaftState.FOLLOWER;

            String topic = request.getTopic();
            String fileName = request.getFileName();
            Path topicDir = dataDir.resolve(topic);
            Files.createDirectories(topicDir);
            Path filePath = topicDir.resolve(fileName);
            
            if (request.getFileOffset() == 0) {
                 logger.info("[{}] Receiving Tier 2 Sync chunk for topic: {}, file: {}", nodeId, topic, fileName);
            }

            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(filePath, 
                    java.nio.file.StandardOpenOption.CREATE, 
                    java.nio.file.StandardOpenOption.READ, 
                    java.nio.file.StandardOpenOption.WRITE)) {
                
                int size = request.getData().size();
                if (size > 0) {
                    java.nio.MappedByteBuffer mappedBuffer = channel.map(java.nio.channels.FileChannel.MapMode.READ_WRITE, request.getFileOffset(), size);
                    request.getData().copyTo(mappedBuffer);
                    mappedBuffer.force(); // Force flush for durability
                }
            }

            return IncrementalSnapshotChunkResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .build();
        } catch (Exception e) {
            logger.error("Error handling IncrementalSnapshotChunk", e);
            return IncrementalSnapshotChunkResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Handle the completion of a Tier 2 Incremental Sync.
     */
    public IncrementalSnapshotDoneResponse handleIncrementalSnapshotDone(IncrementalSnapshotDoneRequest request) {
        lock.lock();
        try {
            if (request.getTerm() > currentTerm) {
                stepDown(request.getTerm());
            }

            if (request.getTerm() < currentTerm) {
                return IncrementalSnapshotDoneResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();
            }

            resetElectionTimer();
            leaderId = request.getLeaderId();
            state = RaftState.FOLLOWER;

            long snapshotIndex = request.getLastIncludedIndex();
            if (snapshotIndex > lastApplied) {
                logger.info("[{}] Received IncrementalSnapshotDone. Advancing state up to index {}", nodeId, snapshotIndex);
                
                if (raftLog.getLastIndex() > 0) {
                    try {
                        long compactUpTo = Math.min(snapshotIndex, raftLog.getLastIndex());
                        raftLog.compact(compactUpTo);
                    } catch (IOException e) {
                        logger.error("Failed to compact Raft log during Incremental Sync", e);
                    }
                }
                raftLog.setStartIndex(snapshotIndex + 1);
                lastApplied = snapshotIndex;
                lastAppliedTerm = request.getLastIncludedTerm();
                commitIndex = Math.max(commitIndex, snapshotIndex);
                
                applyExecutor.execute(() -> {
                    try {
                        messageStore.reload();
                        if (offsetManager != null) {
                            offsetManager.reload();
                        }
                        logger.info("[{}] Successfully applied Tier 2 sync. lastApplied={}, commitIndex={}",
                                nodeId, snapshotIndex, commitIndex);
                    } catch (IOException e) {
                        logger.error("FATAL: Failed to reload MessageStore after Tier 2 Sync. Panicking!", e);
                        System.exit(1);
                    }
                });
            }

            return IncrementalSnapshotDoneResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .build();
        } catch (Exception e) {
            logger.error("Error handling IncrementalSnapshotDone", e);
            return IncrementalSnapshotDoneResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
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
        long term;
        String voted;
        long applied;
        long appliedTerm;
        
        lock.lock();
        try {
            term = currentTerm;
            voted = votedFor;
            applied = lastApplied;
            appliedTerm = lastAppliedTerm;
            stateSaveNeeded = false;
        } finally {
            lock.unlock();
        }

        Path tempPath = null;
        try {
            Properties props = new Properties();
            props.setProperty("currentTerm", String.valueOf(term));
            props.setProperty("votedFor", voted != null ? voted : "");
            props.setProperty("lastApplied", String.valueOf(applied));
            props.setProperty("lastAppliedTerm", String.valueOf(appliedTerm));
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
            lastAppliedTerm = Long.parseLong(props.getProperty("lastAppliedTerm", "0"));
            if (raftLog.getLastIndex() == 0 && lastApplied > 0) {
                raftLog.setStartIndex(lastApplied + 1);
            }
            commitIndex = Math.min(raftLog.getLastIndex(), Math.max(commitIndex, lastApplied));
            
            
            if (raftLog.getLastIndex() - raftLog.getStartIndex() > raftCompactThreshold) {
                long compactUpTo = lastApplied - 100;
                if (compactUpTo > raftLog.getStartIndex()) {
                    raftLog.compact(compactUpTo);
                    logger.info("[{}] Compacted Raft log on startup up to index {}", nodeId, compactUpTo);
                }
            }
            
            logger.info("[{}] Loaded persistent state: term={}, votedFor={}, lastApplied={}",
                    nodeId, currentTerm, votedFor, lastApplied);
        } catch (IOException | NumberFormatException e) {
            throw new IOException("Failed to load persistent state", e);
        }
    }

   

    public RaftState getState() { return state; }
    public long getCurrentTerm() { return currentTerm; }
    public String getNodeId() { return nodeId; }
    public String getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public boolean isLeader() { return state == RaftState.LEADER; }
    public long getLastLogIndex() { return raftLog.getLastIndex(); }
    public Map<String, Long> getMatchIndexMap() { return Collections.unmodifiableMap(matchIndex); }
    public List<String> getPeerIds() { return peers.stream().map(PeerAddress::id).toList(); }

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
