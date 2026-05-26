package com.drmq.broker;

import java.util.ArrayList;
import java.util.List;

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

    public BrokerConfig(String nodeId, int port, String dataDir, List<PeerAddress> peers,
                        boolean metricsEnabled, int metricsPort, String metricsPath) {
        this.nodeId = nodeId;
        this.port = port;
        this.dataDir = dataDir;
        this.peers = peers != null ? peers : List.of();
        this.metricsEnabled = metricsEnabled;
        this.metricsPort = metricsPort;
        this.metricsPath = metricsPath != null ? metricsPath : "/metrics";
    }

    public BrokerConfig(String nodeId, int port, String dataDir, List<PeerAddress> peers) {
        this(nodeId, port, dataDir, peers, true, 9096, "/metrics");
    }

    /** Single-node config (backward compatible) */
    public BrokerConfig(int port, String dataDir) {
        this("standalone", port, dataDir, List.of(), true, 9096, "/metrics");
    }

    public String getNodeId() { return nodeId; }
    public int getPort() { return port; }
    public String getDataDir() { return dataDir; }
    public List<PeerAddress> getPeers() { return peers; }
    public boolean isMetricsEnabled() { return metricsEnabled; }
    public int getMetricsPort() { return metricsPort; }
    public String getMetricsPath() { return metricsPath; }

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
     */
    public static BrokerConfig fromArgs(String[] args) {
        String nodeId = "standalone";
        int port = BrokerServer.DEFAULT_PORT;
        String dataDir = BrokerServer.DEFAULT_DATA_DIR;
        List<PeerAddress> peers = new ArrayList<>();
        boolean metricsEnabled = true;
        int metricsPort = 9096;
        String metricsPath = "/metrics";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id" -> nodeId = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data-dir" -> dataDir = args[++i];
                case "--peers" -> {
                    String[] peerStrs = args[++i].split(",");
                    for (String peerStr : peerStrs) {
                        peers.add(PeerAddress.parse(peerStr));
                    }
                }
                case "--metrics-enabled" -> metricsEnabled = parseBooleanArg(args, ++i, "--metrics-enabled");
                case "--metrics-disabled" -> metricsEnabled = false;
                case "--metrics-port" -> metricsPort = parsePortArg(args, ++i, "--metrics-port");
                case "--metrics-path" -> metricsPath = parsePathArg(args, ++i, "--metrics-path");
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

        return new BrokerConfig(nodeId, port, dataDir, peers, metricsEnabled, metricsPort, metricsPath);
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
