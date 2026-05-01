package com.drmq.client;

import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * DRMQ Producer client for sending messages to the broker.
 *
 * Supports bootstrap servers: provide multiple broker addresses so the producer
 * can automatically failover to another broker if the current one dies.
 *
 * Thread-safe: can be used by multiple threads to send messages concurrently.
 * Uses synchronous send with response waiting.
 */
public class DRMQProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DRMQProducer.class);
    private static final int MAX_RETRIES = 5;
    private static final long RECONNECT_DELAY_MS = 500;  // Brief pause between retries to allow leader election

    private String host;
    private int port;
    private final List<String[]> bootstrapServers;  // List of [host, port] pairs
    private int currentServerIndex = 0;
    private final Object sendLock = new Object();

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private volatile boolean connected = false;

    /**
     * Create a producer targeting a single broker.
     */
    public DRMQProducer(String host, int port) {
        this.host = host;
        this.port = port;
        this.bootstrapServers = new ArrayList<>();
        this.bootstrapServers.add(new String[]{host, String.valueOf(port)});
    }

    /**
     * Create a producer with multiple bootstrap servers for failover.
     * Format: "host1:port1,host2:port2,host3:port3"
     *
     * The producer will try each server in order when the current one fails.
     * If it connects to a follower, it will auto-redirect to the leader.
     */
    public DRMQProducer(String bootstrapServersStr) {
        this.bootstrapServers = new ArrayList<>();
        for (String server : bootstrapServersStr.split(",")) {
            String[] parts = server.trim().split(":");
            if (parts.length == 2) {
                bootstrapServers.add(parts);
            }
        }
        if (bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("No valid bootstrap servers: " + bootstrapServersStr);
        }
        this.host = bootstrapServers.get(0)[0];
        this.port = Integer.parseInt(bootstrapServers.get(0)[1]);
    }

    /**
     * Create a producer targeting localhost with default port.
     */
    public DRMQProducer() {
        this("localhost", 9092);
    }

    /**
     * Connect to the broker.
     */
    public void connect() throws IOException {
        if (connected) {
            return;
        }

        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        connected = true;

        logger.info("Connected to broker at {}:{}", host, port);
    }

    /**
     * Send a message to the specified topic.
     *
     * @param topic   The topic name
     * @param payload The message payload
     * @return The result containing the assigned offset or error
     */
    public SendResult send(String topic, byte[] payload) throws IOException {
        return send(topic, payload, null);
    }

    /**
     * Send a string message to the specified topic.
     */
    public SendResult send(String topic, String message) throws IOException {
        return send(topic, message.getBytes(StandardCharsets.UTF_8), null);
    }

    /**
     * Send a message with an optional key to the specified topic.
     *
     * @param topic   The topic name
     * @param payload The message payload
     * @param key     Optional message key (can be null)
     * @return The result containing the assigned offset or error
     */
    public SendResult send(String topic, byte[] payload, String key) throws IOException {
        // Build the produce request
        ProduceRequest.Builder requestBuilder = ProduceRequest.newBuilder()
                .setTopic(topic)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setTimestamp(System.currentTimeMillis());

        if (key != null && !key.isEmpty()) {
            requestBuilder.setKey(key);
        }

        ProduceRequest request = requestBuilder.build();

        // Wrap in envelope
        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_REQUEST)
                .setPayload(request.toByteString())
                .build();

        // Retry loop: handles broken connections, tries other bootstrap servers
        IOException lastException = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            if (!connected) {
                try {
                    connect();
                } catch (IOException e) {
                    logger.warn("Connection to {}:{} failed (attempt {}/{}): {}",
                            host, port, attempt + 1, MAX_RETRIES, e.getMessage());
                    lastException = e;
                    // Try the next bootstrap server
                    rotateToNextServer();
                    try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during reconnect", ie);
                    }
                    continue;
                }
            }

            try {
                // Send and receive (synchronized for thread safety)
                synchronized (sendLock) {
                    byte[] envelopeBytes = envelope.toByteArray();
                    out.writeInt(envelopeBytes.length);
                    out.write(envelopeBytes);
                    out.flush();

                    // Read response
                    int responseLength = in.readInt();
                    byte[] responseBytes = new byte[responseLength];
                    in.readFully(responseBytes);

                    MessageEnvelope responseEnvelope = MessageEnvelope.parseFrom(responseBytes);
                    ProduceResponse response = ProduceResponse.parseFrom(responseEnvelope.getPayload());

                    if (response.getSuccess()) {
                        logger.debug("Message sent: topic={}, offset={}", topic, response.getOffset());
                        return SendResult.success(response.getOffset());
                    } else {
                        String errorMsg = response.getErrorMessage();
                        // Check for leader redirection (Raft cluster mode)
                        if (errorMsg != null && errorMsg.startsWith("NOT_LEADER:")) {
                            String leaderAddr = errorMsg.substring("NOT_LEADER:".length());
                            if (!leaderAddr.equals("UNKNOWN")) {
                                logger.info("Redirected to leader: {}", leaderAddr);
                                return redirectToLeader(leaderAddr, topic, payload, key);
                            }
                        }
                        logger.warn("Message send failed: {}", errorMsg);
                        return SendResult.failure(errorMsg);
                    }
                }
            } catch (IOException e) {
                // Connection is broken (leader crashed, network issue, etc.)
                logger.warn("Connection lost to {}:{} (attempt {}/{}): {}",
                        host, port, attempt + 1, MAX_RETRIES, e.getMessage());
                lastException = e;
                closeConnection();
                // Try the next bootstrap server
                rotateToNextServer();
                // Brief pause to allow new leader election
                try { Thread.sleep(500); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during reconnect", ie);
                }
            }
        }
        throw new IOException("Failed after " + MAX_RETRIES + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
    }

    /**
     * Rotate to the next bootstrap server in the list.
     */
    private void rotateToNextServer() {
        if (bootstrapServers.size() <= 1) return;
        currentServerIndex = (currentServerIndex + 1) % bootstrapServers.size();
        String[] next = bootstrapServers.get(currentServerIndex);
        this.host = next[0];
        this.port = Integer.parseInt(next[1]);
        logger.info("Switching to next broker: {}:{}", host, port);
    }

    /**
     * Reconnect to the leader broker and retry the produce request.
     */
    private SendResult redirectToLeader(String leaderAddr, String topic, byte[] payload, String key) throws IOException {
        String[] parts = leaderAddr.split(":");
        if (parts.length != 2) {
            return SendResult.failure("Invalid leader address: " + leaderAddr);
        }

        // Close current connection
        closeConnection();

        // Reconnect to leader
        this.host = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.connected = false;

        logger.info("Reconnecting to leader at {}:{}", host, port);

        // Retry
        return send(topic, payload, key);
    }

    /**
     * Close the current connection without closing the producer.
     */
    private void closeConnection() {
        connected = false;
        try { if (in != null) in.close(); } catch (IOException ignored) {}
        try { if (out != null) out.close(); } catch (IOException ignored) {}
        try { if (socket != null && !socket.isClosed()) socket.close(); } catch (IOException ignored) {}
    }

    /**
     * Check if connected to the broker.
     */
    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed();
    }

    /**
     * Close the connection to the broker.
     */
    @Override
    public void close() {
        connected = false;

        try {
            if (in != null) in.close();
        } catch (IOException e) {
            logger.debug("Error closing input stream", e);
        }

        try {
            if (out != null) out.close();
        } catch (IOException e) {
            logger.debug("Error closing output stream", e);
        }

        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.debug("Error closing socket", e);
        }

        logger.info("Disconnected from broker");
    }

    /**
     * Result of a send operation.
     */
    public static class SendResult {
        private final boolean success;
        private final long offset;
        private final String errorMessage;

        private SendResult(boolean success, long offset, String errorMessage) {
            this.success = success;
            this.offset = offset;
            this.errorMessage = errorMessage;
        }

        public static SendResult success(long offset) {
            return new SendResult(true, offset, null);
        }

        public static SendResult failure(String errorMessage) {
            return new SendResult(false, -1, errorMessage);
        }

        public boolean isSuccess() {
            return success;
        }

        public long getOffset() {
            return offset;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            if (success) {
                return "SendResult{success=true, offset=" + offset + "}";
            } else {
                return "SendResult{success=false, error='" + errorMessage + "'}";
            }
        }
    }
}
