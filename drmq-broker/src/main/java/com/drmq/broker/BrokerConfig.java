package com.drmq.broker;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Configuration for a DRMQ broker node, including Raft cluster membership.
 *
 * Usage:
 *   java -jar drmq-broker.jar --id broker1 --port 9092 --peers broker2:9093,broker3:9094 --data-dir ./data-1
 *
 * If no --peers are specified, the broker runs in single-node mode (no Raft).
 */
public class BrokerConfig {
    private final String nodeId;
    private final int port;
    private final String dataDir;
    private final List<PeerAddress> peers;
    private final boolean metricsEnabled;
    private final int metricsPort;
    private final String metricsPath;
    private long logSegmentBytes;
    private long logRetentionMs;
    private final long raftCompactThreshold;
    private final int maxDeliveries;
    private final String dlqTopicPrefix;
    private final boolean logSegmentFsync;
    private final String s3ArchiveBucket;
    private final String s3ArchiveRegion;
    private final String s3ArchiveEndpoint;

    public BrokerConfig(String nodeId, int port, String dataDir, List<PeerAddress> peers,
                        boolean metricsEnabled, int metricsPort, String metricsPath,
                        long logSegmentBytes, long logRetentionMs, long raftCompactThreshold,
                        int maxDeliveries, String dlqTopicPrefix) {
        this(nodeId, port, dataDir, peers, metricsEnabled, metricsPort, metricsPath,
             logSegmentBytes, logRetentionMs, raftCompactThreshold, maxDeliveries, dlqTopicPrefix, true, null, null, null);
    }

    public BrokerConfig(String nodeId, int port, String dataDir, List<PeerAddress> peers,
                        boolean metricsEnabled, int metricsPort, String metricsPath,
                        long logSegmentBytes, long logRetentionMs, long raftCompactThreshold,
                        int maxDeliveries, String dlqTopicPrefix, boolean logSegmentFsync,
                        String s3ArchiveBucket, String s3ArchiveRegion, String s3ArchiveEndpoint) {
        this.nodeId = nodeId;
        this.port = port;
        this.dataDir = dataDir;
        this.peers = peers != null ? peers : List.of();
        this.metricsEnabled = metricsEnabled;
        this.metricsPort = metricsPort;
        this.metricsPath = metricsPath != null ? metricsPath : "/metrics";
        this.logSegmentBytes = logSegmentBytes;
        this.logRetentionMs = logRetentionMs;
        this.raftCompactThreshold = raftCompactThreshold;
        this.maxDeliveries = maxDeliveries > 0 ? maxDeliveries : 5;
        this.dlqTopicPrefix = dlqTopicPrefix != null ? dlqTopicPrefix : "dlq.";
        this.logSegmentFsync = logSegmentFsync;
        this.s3ArchiveBucket = s3ArchiveBucket;
        this.s3ArchiveRegion = s3ArchiveRegion;
        this.s3ArchiveEndpoint = s3ArchiveEndpoint;
    }

    public BrokerConfig(String nodeId, int port, String dataDir, List<PeerAddress> peers) {
        this(nodeId, port, dataDir, peers, true, 9096, "/metrics", 
             100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", true, null, null, null);
    }

    /** Single-node config (backward compatible) */
    public BrokerConfig(int port, String dataDir) {
        this("standalone", port, dataDir, List.of(), true, 9096, "/metrics",
             100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", true, null, null, null);
    }

    public String getNodeId() { return nodeId; }
    public int getPort() { return port; }
    public String getDataDir() { return dataDir; }
    public List<PeerAddress> getPeers() { return peers; }
    public boolean isMetricsEnabled() { return metricsEnabled; }
    public int getMetricsPort() { return metricsPort; }
    public String getMetricsPath() { return metricsPath; }
    public long getLogSegmentBytes() { return logSegmentBytes; }
    public long getLogRetentionMs() { return logRetentionMs; }
    public long getRaftCompactThreshold() { return raftCompactThreshold; }
    public int getMaxDeliveries() { return maxDeliveries; }
    public String getDlqTopicPrefix() { return dlqTopicPrefix; }
    public boolean isLogSegmentFsync() { return logSegmentFsync; }
    public String getS3ArchiveBucket() { return s3ArchiveBucket; }
    public String getS3ArchiveRegion() { return s3ArchiveRegion; }
    public String getS3ArchiveEndpoint() { return s3ArchiveEndpoint; }

    public void setLogSegmentBytes(long logSegmentBytes) {
        if (logSegmentBytes <= 0) {
            throw new IllegalArgumentException("logSegmentBytes must be positive");
        }
        this.logSegmentBytes = logSegmentBytes;
    }

    public void setLogRetentionMs(long logRetentionMs) {
        if (logRetentionMs <= 0) {
            throw new IllegalArgumentException("logRetentionMs must be positive");
        }
        this.logRetentionMs = logRetentionMs;
    }

    /** True if this broker is part of a Raft cluster */
    public boolean isClusterMode() {
        return !peers.isEmpty();
    }

    /**
     * Parse CLI arguments into a BrokerConfig.
     *
     * Supported args:
     *   --id <nodeId>
     *   --port <port>
     *   --data-dir <path>
     *   --peers <id:host:port,id:host:port,...>
     *   --metrics-enabled <true|false>
     *   --metrics-disabled
     *   --metrics-port <port>
     *   --metrics-path </metrics>
     *   --log-segment-bytes <bytes>
     *   --log-retention-ms <ms>
     *   --raft-compact-threshold <count>
     *   --max-deliveries <count>
     *   --dlq-topic-prefix <prefix>
     *   --s3-archive-bucket <bucket>
     *   --s3-archive-region <region>
     *   --s3-archive-endpoint <url>
     */
    public static BrokerConfig fromArgs(String[] args) {
        String nodeId = "standalone";
        int port = BrokerServer.DEFAULT_PORT;
        String dataDir = BrokerServer.DEFAULT_DATA_DIR;
        List<PeerAddress> peers = new ArrayList<>();
        boolean metricsEnabled = true;
        int metricsPort = 9096;
        String metricsPath = "/metrics";
        long logSegmentBytes = 100 * 1024 * 1024L; // 100MB
        long logRetentionMs = 7L * 24 * 60 * 60 * 1000; // 7 days
        long raftCompactThreshold = 50000L; // Keep 50,000 entries to buffer followers during short outages
        int maxDeliveries = 5;
        String dlqTopicPrefix = "dlq.";
        boolean logSegmentFsync = true;
        String s3ArchiveBucket = null;
        String s3ArchiveRegion = "us-east-1";
        String s3ArchiveEndpoint = null;

        String configFilePath = null;
        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configFilePath = args[i + 1];
                break;
            }
        }

        if (configFilePath != null) {
            Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream(configFilePath)) {
                props.load(fis);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load config file: " + configFilePath, e);
            }
            if (props.containsKey("node.id")) nodeId = props.getProperty("node.id");
            if (props.containsKey("port")) port = Integer.parseInt(props.getProperty("port"));
            if (props.containsKey("data.dir")) dataDir = props.getProperty("data.dir");
            if (props.containsKey("peers")) {
                for (String peerStr : props.getProperty("peers").split(",")) {
                    if (!peerStr.isBlank()) peers.add(PeerAddress.parse(peerStr.trim()));
                }
            }
            if (props.containsKey("metrics.enabled")) metricsEnabled = Boolean.parseBoolean(props.getProperty("metrics.enabled"));
            if (props.containsKey("metrics.port")) metricsPort = Integer.parseInt(props.getProperty("metrics.port"));
            if (props.containsKey("metrics.path")) metricsPath = props.getProperty("metrics.path");
            if (props.containsKey("log.segment.bytes")) logSegmentBytes = Long.parseLong(props.getProperty("log.segment.bytes"));
            if (props.containsKey("log.retention.ms")) logRetentionMs = Long.parseLong(props.getProperty("log.retention.ms"));
            if (props.containsKey("raft.compact.threshold")) raftCompactThreshold = Long.parseLong(props.getProperty("raft.compact.threshold"));
            if (props.containsKey("max.deliveries")) maxDeliveries = Integer.parseInt(props.getProperty("max.deliveries"));
            if (props.containsKey("dlq.topic.prefix")) dlqTopicPrefix = props.getProperty("dlq.topic.prefix");
            if (props.containsKey("log.segment.fsync")) logSegmentFsync = Boolean.parseBoolean(props.getProperty("log.segment.fsync"));
            if (props.containsKey("s3.archive.bucket")) s3ArchiveBucket = props.getProperty("s3.archive.bucket");
            if (props.containsKey("s3.archive.region")) s3ArchiveRegion = props.getProperty("s3.archive.region");
            if (props.containsKey("s3.archive.endpoint")) s3ArchiveEndpoint = props.getProperty("s3.archive.endpoint");
        }

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> i++; // skip the value, already handled
                case "--id", "--node-id" -> nodeId = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data-dir" -> dataDir = args[++i];
                case "--peers" -> {
                    peers.clear(); // Override config file peers
                    String[] peerStrs = args[++i].split(",");
                    for (String peerStr : peerStrs) {
                        peers.add(PeerAddress.parse(peerStr));
                    }
                }
                case "--metrics-enabled" -> metricsEnabled = parseBooleanArg(args, ++i, "--metrics-enabled");
                case "--metrics-disabled" -> metricsEnabled = false;
                case "--metrics-port" -> metricsPort = parsePortArg(args, ++i, "--metrics-port");
                case "--metrics-path" -> metricsPath = parsePathArg(args, ++i, "--metrics-path");
                case "--log-segment-bytes" -> logSegmentBytes = parseLongArg(args, ++i, "--log-segment-bytes");
                case "--log-retention-ms" -> logRetentionMs = parseLongArg(args, ++i, "--log-retention-ms");
                case "--raft-compact-threshold" -> raftCompactThreshold = parseLongArg(args, ++i, "--raft-compact-threshold");
                case "--max-deliveries" -> maxDeliveries = (int) parseLongArg(args, ++i, "--max-deliveries");
                case "--dlq-topic-prefix" -> dlqTopicPrefix = parsePrefixArg(args, ++i, "--dlq-topic-prefix");
                case "--log-segment-fsync" -> logSegmentFsync = parseBooleanArg(args, ++i, "--log-segment-fsync");
                case "--s3-archive-bucket" -> s3ArchiveBucket = requireValue(args, ++i, "--s3-archive-bucket");
                case "--s3-archive-region" -> s3ArchiveRegion = requireValue(args, ++i, "--s3-archive-region");
                case "--s3-archive-endpoint" -> s3ArchiveEndpoint = requireValue(args, ++i, "--s3-archive-endpoint");
                default -> {
                    if (i == 0) {
                        try {
                            port = Integer.parseInt(args[i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Unknown argument: " + args[i]);
                        }
                    } else if (i == 1) {
                        dataDir = args[i];
                    }
                }
            }
        }

        return new BrokerConfig(nodeId, port, dataDir, peers, metricsEnabled, metricsPort, metricsPath,
                                logSegmentBytes, logRetentionMs, raftCompactThreshold,
                                maxDeliveries, dlqTopicPrefix, logSegmentFsync, s3ArchiveBucket, s3ArchiveRegion, s3ArchiveEndpoint);
    }

    private static boolean parseBooleanArg(String[] args, int index, String flag) {
        String value = requireValue(args, index, flag);
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(flag + " must be true or false, got: " + value);
    }

    private static int parsePortArg(String[] args, int index, String flag) {
        String value = requireValue(args, index, flag);
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException(flag + " must be between 1 and 65535, got: " + value);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " must be a valid integer port, got: " + value, e);
        }
    }

    private static long parseLongArg(String[] args, int index, String flag) {
        String value = requireValue(args, index, flag);
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(flag + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " must be a valid long integer, got: " + value, e);
        }
    }

    private static String parsePathArg(String[] args, int index, String flag) {
        String value = requireValue(args, index, flag);
        String normalized = value.trim();
        if (normalized.isBlank()) {
            throw new IllegalArgumentException(flag + " must not be empty");
        }
        if (normalized.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException(flag + " must not contain whitespace: " + value);
        }
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static String parsePrefixArg(String[] args, int index, String flag) {
        String value = requireValue(args, index, flag);
        String normalized = value.trim();
        if (normalized.isBlank()) {
            throw new IllegalArgumentException(flag + " must not be empty or whitespace");
        }
        return normalized;
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + flag);
        }
        return args[index];
    }

    /**
     * Represents a peer broker's address in the cluster.
     * Format: "peerId:host:port" (e.g., "broker2:localhost:9093")
     */
    public record PeerAddress(String id, String host, int port) {
        public static PeerAddress parse(String str) {
            String[] parts = str.split(":");
            if (parts.length == 3) {
                return new PeerAddress(parts[0], parts[1], Integer.parseInt(parts[2]));
            } else if (parts.length == 2) {
                // Legacy format: "host:port" — use host as id
                return new PeerAddress(parts[0], parts[0], Integer.parseInt(parts[1]));
            }
            throw new IllegalArgumentException("Invalid peer format: " + str + 
                    " (expected peerId:host:port or host:port)");
        }

        public String address() {
            return host + ":" + port;
        }

        @Override
        public String toString() {
            return id + "@" + host + ":" + port;
        }
    }
}
