package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.raft.RaftNode;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

public class TelemetryWebSocketServer extends WebSocketServer {
    private static final Logger logger = LoggerFactory.getLogger(TelemetryWebSocketServer.class);
    
    private final BrokerServer brokerServer;
    private final CopyOnWriteArraySet<WebSocket> connections;
    private final Timer broadcastTimer;
    private final Gson gson;

    // Throughput history (0-100 scaled for chart)
    private final List<Double> throughputHistory = new CopyOnWriteArrayList<>();
    private final List<Double> produceHistory = new CopyOnWriteArrayList<>();
    private final List<Double> consumeHistory = new CopyOnWriteArrayList<>();
    private final List<Double> errorHistory = new CopyOnWriteArrayList<>();

    // Rolling byte snapshots to compute per-second rates
    private double lastProduceBytes = 0;
    private double lastConsumeBytes = 0;
    private double lastProduceRecords = 0;
    private double lastConsumeRecords = 0;
    private double lastErrorCount = 0;

    // Smoothed current rates (updated every broadcast tick)
    private double currentProduceMBps = 0;
    private double currentConsumeMBps = 0;
    private double currentProduceRecordRate = 0;
    private double currentConsumeRecordRate = 0;
    private double currentErrorRate = 0;

    public TelemetryWebSocketServer(int port, BrokerServer brokerServer) {
        super(new InetSocketAddress(port));
        this.brokerServer = brokerServer;
        this.connections = new CopyOnWriteArraySet<>();
        this.broadcastTimer = new Timer("TelemetryBroadcastTimer", true);
        this.gson = new Gson();
        
        for (int i = 0; i < 300; i++) {
            throughputHistory.add(0.0);
            produceHistory.add(0.0);
            consumeHistory.add(0.0);
            errorHistory.add(0.0);
        }
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        try {
            connections.add(conn);
            logger.info("New telemetry dashboard connected: {}", conn.getRemoteSocketAddress());
            conn.send(buildTelemetryPayload());
        } catch (Exception e) {
            logger.error("Error during onOpen, preventing SelectorThread crash", e);
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        connections.remove(conn);
        logger.info("Telemetry dashboard disconnected: {}", conn.getRemoteSocketAddress());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {}

    @Override
    public void onError(WebSocket conn, Exception ex) {
        logger.error("Telemetry WebSocket error", ex);
    }

    @Override
    public void onStart() {
        logger.info("Telemetry WebSocket server started on port {}", getPort());
        broadcastTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateRates();
                if (!connections.isEmpty()) {
                    String payload = buildTelemetryPayload();
                    for (WebSocket conn : connections) {
                        try {
                            conn.send(payload);
                        } catch (Exception e) {
                            logger.debug("Failed to send telemetry to client", e);
                        }
                    }
                }
            }
        }, 1000, 1000);
    }
    
    public void shutdown() {
        broadcastTimer.cancel();
        try {
            this.stop(1000); // 1-second timeout to force close the socket
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Update all per-second rate calculations by diffing cumulative counters.
     * Called once per broadcast tick (every 1 second), so delta ≈ per-second rate.
     */
    private void updateRates() {
        BrokerMetrics bm = BrokerMetrics.get();
        if (bm == null || !bm.isEnabled()) return;


        double produceBytes   = bm.getCounterValueByType("drmq.broker.request.bytes",   "produce")
                              + bm.getCounterValueByType("drmq.broker.request.bytes",   "produce_batch");
        double consumeBytes   = bm.getCounterValueByType("drmq.broker.request.bytes",   "consume");
        double produceRecords = bm.getCounterValueByType("drmq.broker.request.records", "produce")
                              + bm.getCounterValueByType("drmq.broker.request.records", "produce_batch");
        double consumeRecords = bm.getCounterValueByType("drmq.broker.request.records", "consume");
        double errors         = bm.getCounterValueByType("drmq.broker.request.errors",  "produce")
                              + bm.getCounterValueByType("drmq.broker.request.errors",  "produce_batch")
                              + bm.getCounterValueByType("drmq.broker.request.errors",  "consume");

        currentProduceMBps      = Math.max(0, (produceBytes   - lastProduceBytes)   / (1024.0 * 1024.0));
        currentConsumeMBps      = Math.max(0, (consumeBytes   - lastConsumeBytes)   / (1024.0 * 1024.0));
        currentProduceRecordRate = Math.max(0, produceRecords - lastProduceRecords);
        currentConsumeRecordRate = Math.max(0, consumeRecords - lastConsumeRecords);
        currentErrorRate         = Math.max(0, errors         - lastErrorCount);

        lastProduceBytes   = produceBytes;
        lastConsumeBytes   = consumeBytes;
        lastProduceRecords = produceRecords;
        lastConsumeRecords = consumeRecords;
        lastErrorCount     = errors;

        double totalMBps = currentProduceMBps + currentConsumeMBps;
        synchronized (throughputHistory) {
            throughputHistory.remove(0);
            throughputHistory.add(totalMBps);
            
            produceHistory.remove(0);
            produceHistory.add(currentProduceMBps);
            
            consumeHistory.remove(0);
            consumeHistory.add(currentConsumeMBps);
            
            errorHistory.remove(0);
            errorHistory.add(currentErrorRate);
        }
    }

    private String buildTelemetryPayload() {
        BrokerMetrics bm = BrokerMetrics.get();
        RaftNode raftNode = brokerServer.getRaftNode();

        JsonObject state = new JsonObject();

        // ── METRICS ────────────────────────────────────────────────────────────
        JsonObject metrics = new JsonObject();

        double totalMBps = currentProduceMBps + currentConsumeMBps;
        metrics.addProperty("totalThroughputMB",    round4(totalMBps));
        metrics.addProperty("produceThroughputMB",  round4(currentProduceMBps));
        metrics.addProperty("consumeThroughputMB",  round4(currentConsumeMBps));
        metrics.addProperty("produceRate",          Math.round(currentProduceRecordRate)); // msgs/s
        metrics.addProperty("consumeRate",          Math.round(currentConsumeRecordRate)); // msgs/s
        metrics.addProperty("errorRate",            Math.round(currentErrorRate));         // errors/s

        // Latencies from real Micrometer timers (milliseconds)
        double produceLatencyMs = 0;
        double consumeLatencyMs = 0;
        if (bm != null && bm.isEnabled()) {
            double singleLatency = bm.getTimerMeanMs("drmq.broker.request.latency", "produce");
            double batchLatency  = bm.getTimerMeanMs("drmq.broker.request.latency", "produce_batch");
            produceLatencyMs = Math.max(singleLatency, batchLatency); // use whichever path is active
            consumeLatencyMs = bm.getTimerMeanMs("drmq.broker.request.latency", "consume");
        }
        metrics.addProperty("produceLatencyMs", round4(produceLatencyMs));
        metrics.addProperty("consumeLatencyMs", round4(consumeLatencyMs));

        // Active connections: real handler count from Netty
        int totalHandlers = brokerServer.getActiveChannelsCount();
        
        // Estimate client connections by subtracting internal ones (1 WS + roughly 1-2 per peer)
        int peerCount = raftNode != null ? raftNode.getPeerIds().size() : 0;
        int assumedInternal = 1 + peerCount; // conservative estimate for dead/alive peers
        int clientConns = Math.max(0, totalHandlers - assumedInternal);

        // Real request rate gives us the producer/consumer split
        long totalRequests = Math.round(currentProduceRecordRate + currentConsumeRecordRate);
        int activeProducers = 0, activeConsumers = 0;
        
        if (totalRequests > 0) {
            // Weight connections by relative traffic
            double produceFraction = currentProduceRecordRate / Math.max(1.0, totalRequests);
            activeProducers = (int) Math.round(clientConns * produceFraction);
            activeConsumers = Math.max(0, clientConns - activeProducers);
            
            // Guarantee at least 1 if there is active traffic
            if (currentProduceRecordRate > 0 && activeProducers == 0) {
                activeProducers = 1;
                activeConsumers = Math.max(0, activeConsumers - 1);
            }
            if (currentConsumeRecordRate > 0 && activeConsumers == 0) {
                activeConsumers = 1;
                activeProducers = Math.max(0, activeProducers - 1);
            }
        } else {
            // No traffic yet — fall back to even split
            activeProducers = clientConns / 2 + (clientConns % 2);
            activeConsumers = clientConns / 2;
        }
        metrics.addProperty("activeProducers", activeProducers);
        metrics.addProperty("activeConsumers", activeConsumers);
        metrics.addProperty("totalConnections", totalHandlers);

        // Health: CRITICAL if no leader, DEGRADED if replication lag high, else OPTIMAL
        boolean hasLeader = raftNode != null && (raftNode.isLeader() || raftNode.getLeaderId() != null);
        long commitIndex = raftNode != null ? raftNode.getCommitIndex() : 0;
        long lastApplied = raftNode != null ? raftNode.getLastApplied() : 0;

        // followerSync: real replication lag from matchIndex
        // Fixed scale: 0 lag = 100%, >=1000 lag = 0%
        int followerSync = 100;
        long LAG_FULL_SCALE = 1000;
        if (raftNode != null && raftNode.isLeader() && commitIndex > 0) {
            long maxLag = 0;
            for (String peerId : raftNode.getPeerIds()) {
                Long matchIdx = raftNode.getMatchIndexMap().get(peerId);
                long lag = commitIndex - (matchIdx != null ? matchIdx : 0);
                if (lag > maxLag) maxLag = lag;
            }
            followerSync = (int) Math.max(0, Math.min(100, 100 - (maxLag * 100 / LAG_FULL_SCALE)));
        }

        String health;
        if (!hasLeader) {
            health = "CRITICAL";
        } else if (followerSync < 80 || currentErrorRate > 10) {
            health = "DEGRADED";
        } else {
            health = "OPTIMAL";
        }
        metrics.addProperty("health", health);
        metrics.addProperty("term",          raftNode != null ? raftNode.getCurrentTerm() : 0);
        metrics.addProperty("commitIndex",   commitIndex);
        metrics.addProperty("lastApplied",   lastApplied);
        metrics.addProperty("followerSync",  followerSync);

        // Storage metrics from BrokerMetrics gauges
        long globalOffset = 0;
        int  topicCount   = 0;
        int  logSegments  = 0;
        long cachedMsgs   = 0;
        if (bm != null && bm.isEnabled()) {
            globalOffset = (long) bm.getGaugeValue("drmq.broker.global_offset");
            topicCount   = (int)  bm.getGaugeValue("drmq.broker.topics");
            logSegments  = (int)  bm.getGaugeValue("drmq.broker.log.segments");
            cachedMsgs   = (long) bm.getGaugeValue("drmq.broker.cache.total_messages");
        }
        metrics.addProperty("globalOffset", globalOffset);
        metrics.addProperty("topicCount",   topicCount);
        metrics.addProperty("logSegments",  logSegments);
        metrics.addProperty("cachedMessages", cachedMsgs);

        // Throughput chart history
        JsonArray history = new JsonArray();
        JsonArray pHistory = new JsonArray();
        JsonArray cHistory = new JsonArray();
        JsonArray eHistory = new JsonArray();
        synchronized (throughputHistory) {
            for (int i = 0; i < throughputHistory.size(); i++) {
                history.add(round4(throughputHistory.get(i)));
                pHistory.add(round4(produceHistory.get(i)));
                cHistory.add(round4(consumeHistory.get(i)));
                eHistory.add(round4(errorHistory.get(i)));
            }
        }
        metrics.add("throughputHistory", history);
        metrics.add("produceHistory", pHistory);
        metrics.add("consumeHistory", cHistory);
        metrics.add("errorHistory", eHistory);

        // Produce-rate history (msgs/s scaled similarly)
        state.add("metrics", metrics);

        // ── NODES ──────────────────────────────────────────────────────────────
        JsonArray nodes = new JsonArray();
        
        String localId = raftNode != null ? raftNode.getNodeId() : "local";
        String localStatus = raftNode != null ? raftNode.getState().name() : "LEADER";

        JsonObject localNode = new JsonObject();
        localNode.addProperty("id",         localId);
        localNode.addProperty("name",       "Broker-" + localId.toUpperCase());
        localNode.addProperty("status",     localStatus);
        // throughput in bytes/s for this node (real produce traffic it handled)
        localNode.addProperty("throughputMBps", round4(currentProduceMBps + currentConsumeMBps));
        localNode.addProperty("produceRate",    Math.round(currentProduceRecordRate));
        localNode.addProperty("consumeRate",    Math.round(currentConsumeRecordRate));
        localNode.addProperty("commitIndex",    commitIndex);
        localNode.addProperty("lastApplied",    lastApplied);
        localNode.addProperty("color",  "LEADER".equals(localStatus) ? "#06b6d4" : ("CANDIDATE".equals(localStatus) ? "#f59e0b" : "#a855f7"));
        localNode.addProperty("x", 500);
        localNode.addProperty("y", 200);
        nodes.add(localNode);

        if (raftNode != null) {
            int[] positions = { 0, 1 };
            int i = 0;
            for (String peerId : raftNode.getPeerIds()) {
                JsonObject peerNode = new JsonObject();
                String peerStatus = peerId.equals(raftNode.getLeaderId()) ? "LEADER" : "FOLLOWER";
                Long matchIdx = raftNode.getMatchIndexMap().get(peerId);
                long peerApplied = matchIdx != null ? matchIdx : 0;

                long replicationLag = 0;
                if ((matchIdx == null || matchIdx == 0) && commitIndex > 100) {
                    replicationLag = -1; // unreachable / unknown
                } else {
                    long lagEntries = Math.max(0, commitIndex - peerApplied);
                    if (lagEntries == 0) {
                        replicationLag = 0;
                    } else if (raftNode.getRaftLog() != null && lagEntries <= 2000) {
                        // Calculate exact message lag by inspecting uncommitted Raft entries
                        long msgLag = 0;
                        for (long idx = peerApplied + 1; idx <= commitIndex; idx++) {
                            com.drmq.protocol.DRMQProtocol.RaftEntry entry = raftNode.getRaftLog().getEntry(idx);
                            if (entry != null && entry.getCommandType() == com.drmq.protocol.DRMQProtocol.RaftCommandType.BATCH_MESSAGE) {
                                try {
                                    com.drmq.protocol.DRMQProtocol.ProduceBatchRequest batch = 
                                        com.drmq.protocol.DRMQProtocol.ProduceBatchRequest.parseFrom(entry.getPayload());
                                    msgLag += batch.getEntriesCount();
                                } catch (Exception e) {
                                    msgLag += 1; // Fallback
                                }
                            } else if (entry != null && entry.getCommandType() == com.drmq.protocol.DRMQProtocol.RaftCommandType.MESSAGE) {
                                msgLag += 1;
                            }
                        }
                        replicationLag = msgLag;
                    } else if (lagEntries > 2000) {
                        // Fallback estimate if lag is extremely large to avoid blocking telemetry thread
                        double avgBatchSize = currentProduceRecordRate > 0 ? 
                            Math.max(1.0, currentProduceRecordRate / 10.0) : 50.0; 
                        replicationLag = (long) (lagEntries * avgBatchSize);
                    }
                }

                peerNode.addProperty("id",              peerId);
                peerNode.addProperty("name",            "Broker-" + peerId.toUpperCase());
                peerNode.addProperty("status",          peerStatus);
                peerNode.addProperty("throughputMBps",  0.0); // peers don't share their own traffic
                peerNode.addProperty("produceRate",     0L);
                peerNode.addProperty("consumeRate",     0L);
                peerNode.addProperty("commitIndex",     peerApplied);
                peerNode.addProperty("lastApplied",     peerApplied);
                peerNode.addProperty("replicationLag",  replicationLag);
                peerNode.addProperty("color",           "LEADER".equals(peerStatus) ? "#06b6d4" : "#a855f7");
                // Positions: two followers at bottom-left and bottom-right
                peerNode.addProperty("x", i == 0 ? 200 : 800);
                peerNode.addProperty("y", 500);
                nodes.add(peerNode);
                i++;
                if (i > 1) break; // support up to 2 peers in a 3-node cluster layout
            }
        }
        state.add("nodes", nodes);

        // ── LATENCIES ──────────────────────────────────────────────────────────
        // Real Raft RPC latency from Micrometer (append_entries type)
        double raftRpcLatencyMs = 0;
        if (bm != null && bm.isEnabled()) {
            raftRpcLatencyMs = bm.getTimerMeanMs("drmq.broker.raft.rpc.latency", "append_entries");
        }
        JsonObject latencies = new JsonObject();
        // Inter-node Raft RPC latency (same for all links since we only know our own outbound)
        latencies.addProperty("alphaBeta",   round4(raftRpcLatencyMs > 0 ? raftRpcLatencyMs : 0));
        latencies.addProperty("betaGamma",   round4(raftRpcLatencyMs > 0 ? raftRpcLatencyMs : 0));
        latencies.addProperty("raftRpcMs",   round4(raftRpcLatencyMs));
        state.add("latencies", latencies);

        // ── EVENTS ────────────────────────────────────────────────────────────
        state.add("events", ClusterEventBuffer.get().toJsonArray());

        return gson.toJson(state);
    }

    private static double round4(double v) {
        return Math.round(v * 10000.0) / 10000.0;
    }
}
