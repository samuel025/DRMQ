package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.raft.RaftNode;
import com.drmq.broker.raft.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * DRMQ Broker Server - TCP server accepting producer/consumer and Raft peer connections.
 * Supports two modes:
 * - Single-node mode (no --peers): operates as a standalone broker, no Raft.
 * - Cluster mode (--peers): runs a RaftNode for leader election and log replication.
 */
public class BrokerServer {
    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;
    public static final String DEFAULT_DATA_DIR = "./data";

    private final BrokerConfig config;
    private final ExecutorService executor;
    private final MessageStore messageStore;
    private final LogManager logManager;
    private final OffsetManager offsetManager;
    private final RaftNode raftNode;       
    private final List<RaftPeer> raftPeers; 
    private final Set<ClientHandler> activeHandlers = ConcurrentHashMap.newKeySet();

    private ServerSocket serverSocket;
    private volatile boolean running = false;

    /**
     * Create a broker from a BrokerConfig (supports both single-node and cluster mode).
     */
    public BrokerServer(BrokerConfig config) throws IOException {
        this.config = config;
        this.executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
        this.logManager = new LogManager(config.getDataDir());
        this.messageStore = new MessageStore(logManager);
        this.offsetManager = new OffsetManager(config.getDataDir());
        this.raftPeers = new ArrayList<>();

        if (config.isClusterMode()) {
            // Create RaftNode
            this.raftNode = new RaftNode(
                    config.getNodeId(),
                    config.getPort(),
                    config.getPeers(),
                    messageStore,
                    offsetManager,
                    Paths.get(config.getDataDir())
            );

            // Create RaftPeer connections and register them with RaftNode
            for (BrokerConfig.PeerAddress peer : config.getPeers()) {
                RaftPeer raftPeer = new RaftPeer(peer);
                raftPeers.add(raftPeer);
                raftNode.registerVoteHandler(peer.id(), raftPeer::sendRequestVote);
                raftNode.registerAppendHandler(peer.id(), raftPeer::sendAppendEntries);
            }

            logger.info("Cluster mode: nodeId={}, peers={}", config.getNodeId(), config.getPeers());
        } else {
            this.raftNode = null;
            logger.info("Single-node mode (no Raft)");
        }
    }

    /** Backward-compatible: single-node mode with port and threadPoolSize */
    public BrokerServer(int port, int threadPoolSize, String dataDir) throws IOException {
        this(new BrokerConfig(port, dataDir));
    }

    public BrokerServer(int port, int threadPoolSize) throws IOException {
        this(port, threadPoolSize, DEFAULT_DATA_DIR);
    }

    public BrokerServer() throws IOException {
        this(DEFAULT_PORT, DEFAULT_THREAD_POOL_SIZE);
    }

    /**
     * Start the broker server. Blocks until shutdown.
     */
    public void start() throws IOException {
        try {
            messageStore.recover();
        } catch (IOException e) {
            logger.error("Failed to recover message store: {}", e.getMessage());
            throw e;
        }

        serverSocket = new ServerSocket(config.getPort());
        running = true;

        // Start Raft if in cluster mode
        if (raftNode != null) {
            raftNode.start();
        }

        logger.info("DRMQ Broker started on port {} with data directory {}",
                config.getPort(), config.getDataDir());

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                ClientHandler handler = new ClientHandler(clientSocket, messageStore, offsetManager, raftNode, activeHandlers);
                activeHandlers.add(handler);

                executor.submit(handler);
            } catch (IOException e) {
                if (running) {
                    logger.error("Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Start the broker in a background thread.
     */
    public void startAsync() {
        Thread serverThread = new Thread(() -> {
            try {
                start();
            } catch (IOException e) {
                logger.error("Broker server error", e);
            }
        }, "broker-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for server to be ready
        while (!running && serverThread.isAlive()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Gracefully shutdown the broker.
     */
    public void shutdown() {
        logger.info("Shutting down broker...");
        running = false;

        // Stop Raft
        if (raftNode != null) {
            raftNode.stop();
        }

        // Close Raft peer connections
        for (RaftPeer peer : raftPeers) {
            peer.close();
        }

        // Close server socket to stop accepting new connections
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.debug("Error closing server socket", e);
        }

        // Stop all active handlers
        for (ClientHandler handler : activeHandlers) {
            handler.stop();
        }
        activeHandlers.clear();

        // Close LogManager
        try {
            if (logManager != null) {
                logManager.close();
            }
        } catch (IOException e) {
            logger.error("Error closing log manager", e);
        }

        // Shutdown executor
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Broker shutdown complete");
    }

    public boolean isRunning() { return running; }
    public MessageStore getMessageStore() { return messageStore; }
    public int getPort() { return config.getPort(); }
    public RaftNode getRaftNode() { return raftNode; }

    /**
     * Main entry point — supports both legacy and new-style arguments.
     */
    public static void main(String[] args) {
        BrokerConfig config = BrokerConfig.fromArgs(args);

        try {
            BrokerServer broker = new BrokerServer(config);
            Runtime.getRuntime().addShutdownHook(new Thread(broker::shutdown));
            broker.start();
        } catch (IOException e) {
            logger.error("Failed to start broker", e);
            System.exit(1);
        }
    }
}
