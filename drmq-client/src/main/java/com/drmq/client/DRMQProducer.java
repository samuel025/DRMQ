package com.drmq.client;

import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

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
        List<String[]> parsed = host != null && host.contains(",") ? parseBootstrapServers(host) : List.of();
        if (!parsed.isEmpty()) {
            this.bootstrapServers = new ArrayList<>(parsed);
            this.currentServerIndex = ThreadLocalRandom.current().nextInt(bootstrapServers.size());
            this.host = bootstrapServers.get(currentServerIndex)[0];
            this.port = Integer.parseInt(bootstrapServers.get(currentServerIndex)[1]);
            logger.warn("Comma-separated bootstrap list passed as host; using parsed servers and ignoring port {}", port);
        } else {
            this.host = host;
            this.port = port;
            this.bootstrapServers = new ArrayList<>();
            this.bootstrapServers.add(new String[]{host, String.valueOf(port)});
        }
    }

    /**
     * Create a producer with multiple bootstrap servers for failover.
     * Format: "host1:port1,host2:port2,host3:port3"
     *
     * The producer will try each server in order when the current one fails.
     * If it connects to a follower, it will auto-redirect to the leader.
     */
    public DRMQProducer(String bootstrapServersStr) {
        this.bootstrapServers = new ArrayList<>(parseBootstrapServers(bootstrapServersStr));
        if (bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("No valid bootstrap servers: " + bootstrapServersStr);
        }
        this.currentServerIndex = ThreadLocalRandom.current().nextInt(bootstrapServers.size());
        this.host = bootstrapServers.get(currentServerIndex)[0];
        this.port = Integer.parseInt(bootstrapServers.get(currentServerIndex)[1]);
    }

    private static List<String[]> parseBootstrapServers(String bootstrapServersStr) {
        List<String[]> parsed = new ArrayList<>();
        if (bootstrapServersStr == null || bootstrapServersStr.isBlank()) {
            return parsed;
        }
        for (String server : bootstrapServersStr.split(",")) {
            String[] parts = server.trim().split(":");
            if (parts.length == 2) {
                parsed.add(parts);
            }
        }
        return parsed;
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
        ensureConnectedWithRetry();
    }

    private void connectInternal() throws IOException {
        if (connected && socket != null && !socket.isClosed()) {
            return;
        }
        connected = false;
        if (socket != null && !socket.isClosed()) {
            closeConnection();
        }

        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        connected = true;

        logger.info("Connected to broker at {}:{}", host, port);
    }

    /**
     * Ensures connected state with automatic retries across all bootstrap servers.
     * Tries to connect to the current broker, and if that fails, cycles through
     * all bootstrap servers before giving up.
     */
    private void ensureConnectedWithRetry() throws IOException {
        if (connected && socket != null && !socket.isClosed()) {
            return;
        }
        if (socket != null && socket.isClosed()) {
            closeConnection();
        }

        IOException lastException = null;
        int totalAttempts = MAX_RETRIES * bootstrapServers.size();

        for (int attempt = 0; attempt < totalAttempts; attempt++) {
            try {
                connectInternal();
                return;  // Success
            } catch (IOException e) {
                logger.debug("Connection to {}:{} failed (attempt {}/{}): {}",
                        host, port, attempt + 1, totalAttempts, e.getMessage());
                lastException = e;
                closeConnection();
                rotateToNextServer();
                try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during connection retry", ie);
                }
            }
        }

        throw new IOException("Failed to connect after " + totalAttempts + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
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
        return sendWithRetry(topic, payload, key, MAX_RETRIES);
    }

    private SendResult sendWithRetry(String topic, byte[] payload, String key, int retriesLeft) throws IOException {
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

        IOException lastException = null;
        int totalAttempts = MAX_RETRIES * bootstrapServers.size();

        for (int attempt = 0; attempt < totalAttempts; attempt++) {
            try {
                ensureConnectedWithRetry();

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
                                try {
                                    redirectToLeader(leaderAddr);
                                    // Retry the send to the leader
                                    continue;
                                } catch (IOException e) {
                                    logger.warn("Failed to redirect to leader {}: {}", leaderAddr, e.getMessage());
                                    lastException = e;
                                    closeConnection();
                                    rotateToNextServer();
                                }
                            } else {
                                logger.info("Leader unknown, rotating to next bootstrap server");
                                lastException = new IOException("Leader unknown");
                                closeConnection();
                                rotateToNextServer();
                                try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    throw new IOException("Interrupted during leader-unknown retry", ie);
                                }
                                continue;
                            }
                        }
                        logger.warn("Message send failed: {}", errorMsg);
                        return SendResult.failure(errorMsg);
                    }
                }
            } catch (IOException e) {
                // Connection is broken (leader crashed, network issue, etc.)
                logger.warn("Connection lost to {}:{} (attempt {}/{}): {}",
                        host, port, attempt + 1, totalAttempts, e.getMessage());
                lastException = e;
                closeConnection();
                // Try the next bootstrap server
                rotateToNextServer();
                // Brief pause to allow new leader election
                try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during send retry", ie);
                }
            }
        }

        throw new IOException("Failed to send after " + totalAttempts + " attempts: " +
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

    private void syncServerIndexToCurrent() {
        for (int i = 0; i < bootstrapServers.size(); i++) {
            String[] server = bootstrapServers.get(i);
            if (server.length != 2) {
                continue;
            }
            try {
                int serverPort = Integer.parseInt(server[1]);
                if (server[0].equals(host) && serverPort == port) {
                    currentServerIndex = i;
                    return;
                }
            } catch (NumberFormatException ignored) {
            }
        }
    }

    /**
     * Reconnect to the leader broker.
     */
    private void redirectToLeader(String leaderAddr) throws IOException {
        String[] parts = leaderAddr.split(":");
        if (parts.length != 2) {
            throw new IOException("Invalid leader address: " + leaderAddr);
        }

        try {
            int leaderPort = Integer.parseInt(parts[1]);
            this.host = parts[0];
            this.port = leaderPort;
            syncServerIndexToCurrent();
        } catch (NumberFormatException e) {
            throw new IOException("Invalid port in leader address: " + leaderAddr, e);
        }

        closeConnection();
        logger.info("Redirected to leader at {}:{}", host, port);
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
