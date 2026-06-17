package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.raft.RaftNode;
import com.drmq.broker.raft.RaftPeer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class BrokerServer {
    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_THREAD_POOL_SIZE = 100;
    public static final String DEFAULT_DATA_DIR = "./data";

    private final BrokerConfig config;
    private final MessageStore messageStore;
    private final LogManager logManager;
    private final OffsetManager offsetManager;
    private final ConsumerGroupCoordinator groupCoordinator;
    private final RaftNode raftNode;       
    private final List<RaftPeer> raftPeers; 
    private final BrokerMetrics metrics;
    private TelemetryWebSocketServer telemetryServer;

    private volatile boolean running = false;
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup businessGroup;
    private Channel serverChannel;
    private final ChannelGroup activeChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public BrokerServer(BrokerConfig config) throws IOException {
        this.config = config;
        this.logManager = new LogManager(config);
        this.messageStore = new MessageStore(logManager, config);
        this.offsetManager = new OffsetManager(config.getDataDir());
        this.raftPeers = new ArrayList<>();
        this.metrics = BrokerMetrics.init(config);

        if (config.isClusterMode()) {
            this.raftNode = new RaftNode(
                    config.getNodeId(),
                    config.getPort(),
                    config.getPeers(),
                    messageStore,
                    offsetManager,
                    Paths.get(config.getDataDir())
            );

            for (BrokerConfig.PeerAddress peer : config.getPeers()) {
                RaftPeer raftPeer = new RaftPeer(peer);
                raftPeers.add(raftPeer);
                raftNode.registerVoteHandler(peer.id(), raftPeer::sendRequestVote);
                raftNode.registerAppendHandler(peer.id(), raftPeer::sendAppendEntries);
                raftNode.registerPreVoteHandler(peer.id(), raftPeer::sendPreVote);
                raftNode.registerInstallSnapshotHandler(peer.id(), raftPeer::sendInstallSnapshot);
            }

            logger.info("Cluster mode: nodeId={}, peers={}", config.getNodeId(), config.getPeers());
        } else {
            this.raftNode = null;
            logger.info("Single-node mode (no Raft)");
        }

        this.groupCoordinator = new ConsumerGroupCoordinator(messageStore, offsetManager,
                raftNode, ConsumerGroupCoordinator.DEFAULT_LEASE_TIMEOUT_MS);

        metrics.registerBroker(activeChannels::size, messageStore, offsetManager, logManager, raftNode);
    }

    public BrokerServer(int port, int threadPoolSize, String dataDir) throws IOException {
        this(new BrokerConfig(port, dataDir));
    }

    public BrokerServer(int port, int threadPoolSize) throws IOException {
        this(port, threadPoolSize, DEFAULT_DATA_DIR);
    }

    public BrokerServer() throws IOException {
        this(DEFAULT_PORT, DEFAULT_THREAD_POOL_SIZE);
    }

    public int getActiveChannelsCount() {
        return activeChannels != null ? activeChannels.size() : 0;
    }

    public void start() throws IOException {
        try {
            messageStore.recover();
        } catch (IOException e) {
            logger.error("Failed to recover message store: {}", e.getMessage());
            throw e;
        }

        metrics.start();

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        businessGroup = new DefaultEventExecutorGroup(DEFAULT_THREAD_POOL_SIZE);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     p.addLast(new LengthFieldBasedFrameDecoder(256 * 1024 * 1024, 0, 4, 0, 4));
                     p.addLast(new ByteArrayDecoder());
                     p.addLast(new LengthFieldPrepender(4));
                     p.addLast(new ByteArrayEncoder());
                     p.addLast(businessGroup, "clientHandler", new ClientHandler(messageStore, offsetManager, raftNode, activeChannels, groupCoordinator));
                 }
             });

            ChannelFuture f = b.bind(config.getPort()).sync();
            serverChannel = f.channel();
            running = true;

            if (raftNode != null) {
                raftNode.start();
            }
            int wsPort = config.getPort() + 200;
            telemetryServer = new TelemetryWebSocketServer(wsPort, this);
            telemetryServer.start();

            logger.info("DRMQ Broker started on port {} with data directory {}",
                    config.getPort(), config.getDataDir());

            serverChannel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            shutdown();
        }
    }

    public void startAsync() {
        Thread serverThread = new Thread(() -> {
            try {
                start();
            } catch (Exception e) {
                logger.error("Broker server error", e);
            }
        }, "broker-server");
        serverThread.setDaemon(true);
        serverThread.start();

        while (!running && serverThread.isAlive()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private volatile boolean isShutdownComplete = false;

    public void shutdown() {
        if (isShutdownComplete) return;
        logger.info("Shutting down Netty broker...");
        if (raftNode != null) {
            raftNode.stop();
        }
        if (telemetryServer != null) {
            telemetryServer.shutdown();
        }

        if (activeChannels != null) {
            activeChannels.close().awaitUninterruptibly();
        }

        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
        }

        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        if (businessGroup != null) businessGroup.shutdownGracefully();

        try {
            if (bossGroup != null) bossGroup.terminationFuture().await();
            if (workerGroup != null) workerGroup.terminationFuture().await();
            if (businessGroup != null) businessGroup.terminationFuture().await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        ClientHandler.shutdownRpcExecutor();

        if (raftPeers != null) {
            for (RaftPeer peer : raftPeers) {
                peer.close();
            }
        }

        try {
            if (logManager != null) logManager.close();
        } catch (IOException e) {
            logger.error("Error closing log manager", e);
        }

        try {
            if (offsetManager != null) offsetManager.close();
        } catch (IOException e) {
            logger.error("Error closing offset manager", e);
        }

        if (groupCoordinator != null) {
            groupCoordinator.close();
        }

        logger.info("Broker shutdown complete");
        if (metrics != null) {
            metrics.close();
        }
        running = false;
        isShutdownComplete = true;
    }

    public boolean isRunning() { return running; }
    public MessageStore getMessageStore() { return messageStore; }
    public int getPort() { return config.getPort(); }
    public RaftNode getRaftNode() { return raftNode; }

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
