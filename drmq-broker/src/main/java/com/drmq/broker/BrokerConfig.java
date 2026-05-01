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

    public BrokerConfig(String nodeId, int port, String dataDir, List<PeerAddress> peers) {
        this.nodeId = nodeId;
        this.port = port;
        this.dataDir = dataDir;
        this.peers = peers != null ? peers : List.of();
    }

    /** Single-node config (backward compatible) */
    public BrokerConfig(int port, String dataDir) {
        this("standalone", port, dataDir, List.of());
    }

    public String getNodeId() { return nodeId; }
    public int getPort() { return port; }
    public String getDataDir() { return dataDir; }
    public List<PeerAddress> getPeers() { return peers; }

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
     */
    public static BrokerConfig fromArgs(String[] args) {
        String nodeId = "standalone";
        int port = BrokerServer.DEFAULT_PORT;
        String dataDir = BrokerServer.DEFAULT_DATA_DIR;
        List<PeerAddress> peers = new ArrayList<>();

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
                default -> {
                    // Legacy support: first positional arg = port, second = dataDir
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

        return new BrokerConfig(nodeId, port, dataDir, peers);
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
