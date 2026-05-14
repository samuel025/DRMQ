package com.drmq.client;

import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * DRMQ Consumer client for reading messages from topics.
 *
 * Supports bootstrap servers: provide multiple broker addresses so the consumer
 * can automatically failover to another broker if the current one dies.
 *
 * Offsets are stored on the broker per consumer group.
 * On subscribe(), the consumer fetches its last committed offset from the
 * broker and resumes from there. Offsets are only auto-committed after poll()
 * when auto-commit is enabled.
 */
public class DRMQConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DRMQConsumer.class);
    private static final int DEFAULT_PORT = 9092;
    private static final int DEFAULT_MAX_MESSAGES = 100;
    private static final String DEFAULT_CONSUMER_GROUP = "default";
    private static final long DEFAULT_POLL_TIMEOUT_MS = 1000;
    private static final int MAX_RETRIES = 5;
    private static final long RECONNECT_DELAY_MS = 500;  // Brief pause between retries to allow leader election

    private String host;
    private int port;
    /**
     * Consumer Identifier used for offset tracking on the broker
     * Note: DRMQ does not support multiple consumers per consumer group. Each group name should be used by a single consumer instance to avoid offset conflicts.
     */
    private final String consumerGroup;
    private final List<String[]> bootstrapServers;
    private int currentServerIndex = 0;

    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private volatile boolean connected = false;

    private final Map<String, Long> topicOffsets = new HashMap<>();
    private final Object pollLock = new Object();
    private volatile boolean autoCommit = false;



    public DRMQConsumer() {
        this("localhost", DEFAULT_PORT, DEFAULT_CONSUMER_GROUP);
    }

    public DRMQConsumer(String consumerGroup) {
        this("localhost", DEFAULT_PORT, consumerGroup);
    }

    public DRMQConsumer(String host, int port) {
        this(host, port, DEFAULT_CONSUMER_GROUP);
    }

    public DRMQConsumer(String host, int port, String consumerGroup) {
        this.consumerGroup = consumerGroup;
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
     * Create a consumer with multiple bootstrap servers for failover.
     * Format: "host1:port1,host2:port2,host3:port3"
     */
    public DRMQConsumer(String bootstrapServersStr, String consumerGroup) {
        this.consumerGroup = consumerGroup;
        this.bootstrapServers = new ArrayList<>(parseBootstrapServers(bootstrapServersStr));
        if (bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("No valid bootstrap servers: " + bootstrapServersStr);
        }
        this.currentServerIndex = ThreadLocalRandom.current().nextInt(bootstrapServers.size());
        this.host = bootstrapServers.get(currentServerIndex)[0];
        this.port = Integer.parseInt(bootstrapServers.get(currentServerIndex)[1]);
    }

    /**
     * Enable or disable auto-commit after poll(). Default is false.
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public boolean isAutoCommit() {
        return autoCommit;
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
        inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        connected = true;

        logger.info("Connected to DRMQ broker at {}:{} as group '{}'", host, port, consumerGroup);
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
     * Close and reopen the connection, cycling to the next bootstrap server.
     */
    private void reconnect() throws IOException {
        closeConnection();
        rotateToNextServer();
        ensureConnectedWithRetry();
        for (Map.Entry<String, Long> entry : topicOffsets.entrySet()) {
            logger.info("Re-subscribing to topic '{}' at offset {} on new broker",
                    entry.getKey(), entry.getValue());
        }
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
     * Close the current connection without closing the consumer.
     */
    private void closeConnection() {
        connected = false;
        try { if (inputStream != null) inputStream.close(); } catch (IOException ignored) {}
        try { if (outputStream != null) outputStream.close(); } catch (IOException ignored) {}
        try { if (socket != null && !socket.isClosed()) socket.close(); } catch (IOException ignored) {}
    }

    /**
     * Redirect to the leader if the broker indicates NOT_LEADER.
     * Returns true if a reconnect attempt was made.
     */
    private boolean tryRedirectToLeader(String errorMessage) throws IOException {
        if (errorMessage == null || !errorMessage.startsWith("NOT_LEADER:")) {
            return false;
        }

        String leader = errorMessage.substring("NOT_LEADER:".length());
        if (!"UNKNOWN".equals(leader)) {
            String[] parts = leader.split(":");
            if (parts.length == 2) {
                try {
                    this.host = parts[0];
                    this.port = Integer.parseInt(parts[1]);
                    syncServerIndexToCurrent();
                    closeConnection();
                    ensureConnectedWithRetry();
                    logger.info("Redirected to leader {}:{}", host, port);
                    return true;
                } catch (NumberFormatException e) {
                    // Fall through to reconnect cycling.
                }
            }
        }

        reconnect();
        return true;
    }

    /**
     * Subscribe to a topic. Resumes from the broker-committed offset automatically.
     * If no offset has been committed yet, starts from offset 0.
     */
    public void subscribe(String topic) throws IOException {
        ensureConnectedWithRetry();
        long offset = fetchOffsetFromBroker(topic);
        topicOffsets.put(topic, offset);
        logger.info("Subscribed to topic '{}' from offset {} (group='{}')", topic, offset, consumerGroup);
    }

    /**
     * Subscribe to a topic, overriding to a specific offset.
     * Useful for replaying or skipping messages.
     */
    public void subscribe(String topic, long fromOffset) throws IOException {
        ensureConnectedWithRetry();
        topicOffsets.put(topic, fromOffset);
        logger.info("Subscribed to topic '{}' from explicit offset {} (group='{}')", topic, fromOffset, consumerGroup);
    }

    // -------------------------------------------------------------------------
    // Poll (auto-commits offset after fetching, with reconnect on failure)
    // -------------------------------------------------------------------------

    public List<ConsumedMessage> poll() throws IOException {
        return poll(DEFAULT_MAX_MESSAGES, DEFAULT_POLL_TIMEOUT_MS);
    }

    public List<ConsumedMessage> poll(int maxMessages) throws IOException {
        return poll(maxMessages, DEFAULT_POLL_TIMEOUT_MS);
    }

    /**
     * Poll for messages with a specific max count and long-poll timeout.
     * Automatically reconnects to another broker if the connection is lost.
     *
     * @param maxMessages maximum number of messages to fetch per topic
     * @param timeoutMs   how long the broker should wait for messages before
     *                    returning empty (0 = return immediately / short poll)
     */
    public List<ConsumedMessage> poll(int maxMessages, long timeoutMs) throws IOException {
        ensureConnectedWithRetry();

        synchronized (pollLock) {
            try {
                return doPoll(maxMessages, timeoutMs);
            } catch (IOException e) {
                // Connection broken — reconnect and retry once
                logger.warn("Poll failed ({}), reconnecting to another broker...", e.getMessage());
                reconnect();
                return doPoll(maxMessages, timeoutMs);
            }
        }
    }

    private List<ConsumedMessage> doPoll(int maxMessages, long timeoutMs) throws IOException {
        List<ConsumedMessage> allMessages = new ArrayList<>();

        for (Map.Entry<String, Long> entry : topicOffsets.entrySet()) {
            String topic = entry.getKey();
            long fromOffset = entry.getValue();

            List<ConsumedMessage> messages = fetchMessages(topic, fromOffset, maxMessages, timeoutMs);
            allMessages.addAll(messages);

            if (!messages.isEmpty()) {
                long nextOffset = messages.get(messages.size() - 1).offset() + 1;
                topicOffsets.put(topic, nextOffset);
                if (autoCommit) {
                    commitOffsetToBroker(topic, nextOffset);
                }
            }
        }

        return allMessages;
    }

    // -------------------------------------------------------------------------
    // Manual commit / offset management
    // -------------------------------------------------------------------------

    /**
     * Manually commit a specific offset to the broker.
     */
    public void commit(String topic, long offset) throws IOException {
        ensureConnectedWithRetry();
        topicOffsets.put(topic, offset);
        commitOffsetToBroker(topic, offset);
        logger.debug("Manually committed offset {} for topic '{}'", offset, topic);
    }

    public long getCurrentOffset(String topic) {
        return topicOffsets.getOrDefault(topic, 0L);
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    // -------------------------------------------------------------------------
    // Broker offset protocol
    // -------------------------------------------------------------------------

    /**
     * Ask the broker for the last committed offset for this group/topic.
     * Returns 0 if no offset has been committed yet.
     * Automatically retries with failover if the connection fails.
     */
    private long fetchOffsetFromBroker(String topic) throws IOException {
        return fetchOffsetFromBrokerWithRetry(topic, MAX_RETRIES);
    }

    private long fetchOffsetFromBrokerWithRetry(String topic, int retriesLeft) throws IOException {
        IOException lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                ensureConnectedWithRetry();
                return fetchOffsetFromBrokerInternal(topic);
            } catch (IOException e) {
                lastException = e;
                logger.warn("Failed to fetch offset for topic '{}' from {}:{} (attempt {}/{}): {}",
                        topic, host, port, attempt + 1, MAX_RETRIES, e.getMessage());
                closeConnection();
                rotateToNextServer();
                try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during offset fetch retry", ie);
                }
            }
        }

        logger.error("Failed to fetch offset for topic '{}' after {} attempts", topic, MAX_RETRIES);
        throw new IOException("Failed to fetch offset after " + MAX_RETRIES + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
    }

    private long fetchOffsetFromBrokerInternal(String topic) throws IOException {
        FetchOffsetRequest request = FetchOffsetRequest.newBuilder()
                .setConsumerGroup(consumerGroup)
                .setTopic(topic)
                .build();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.FETCH_OFFSET_REQUEST)
                .setPayload(request.toByteString())
                .build();

        sendEnvelope(envelope);

        MessageEnvelope responseEnvelope = receiveEnvelope();
        FetchOffsetResponse response = FetchOffsetResponse.parseFrom(responseEnvelope.getPayload());

        if (!response.getSuccess()) {
            if (tryRedirectToLeader(response.getErrorMessage())) {
                return fetchOffsetFromBrokerInternal(topic);
            }
            logger.warn("Failed to fetch offset from broker for topic '{}': {}", topic, response.getErrorMessage());
            return 0L;
        }

        long offset = response.getOffset();
        return offset < 0 ? 0L : offset;
    }

    /**
     * Push the current offset for this group/topic to the broker.
     * Automatically retries with failover if the connection fails.
     */
    private void commitOffsetToBroker(String topic, long offset) throws IOException {
        commitOffsetToBrokerWithRetry(topic, offset, MAX_RETRIES);
    }

    private void commitOffsetToBrokerWithRetry(String topic, long offset, int retriesLeft) throws IOException {
        IOException lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                ensureConnectedWithRetry();
                commitOffsetToBrokerInternal(topic, offset);
                return;
            } catch (IOException e) {
                lastException = e;
                logger.warn("Failed to commit offset {} for topic '{}' to {}:{} (attempt {}/{}): {}",
                        offset, topic, host, port, attempt + 1, MAX_RETRIES, e.getMessage());
                closeConnection();
                rotateToNextServer();
                try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during offset commit retry", ie);
                }
            }
        }

        logger.error("Failed to commit offset {} for topic '{}' after {} attempts", offset, topic, MAX_RETRIES);
        throw new IOException("Failed to commit offset after " + MAX_RETRIES + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
    }

    private void commitOffsetToBrokerInternal(String topic, long offset) throws IOException {
        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
                .setConsumerGroup(consumerGroup)
                .setTopic(topic)
                .setOffset(offset)
                .build();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.COMMIT_OFFSET_REQUEST)
                .setPayload(request.toByteString())
                .build();

        sendEnvelope(envelope);

        MessageEnvelope responseEnvelope = receiveEnvelope();
        CommitOffsetResponse response = CommitOffsetResponse.parseFrom(responseEnvelope.getPayload());

        if (!response.getSuccess()) {
            if (tryRedirectToLeader(response.getErrorMessage())) {
                commitOffsetToBrokerInternal(topic, offset);
                return;
            }
            logger.warn("Failed to commit offset {} for topic '{}': {}", offset, topic, response.getErrorMessage());
        } else {
            logger.debug("Committed offset {} for topic '{}' to broker", offset, topic);
        }
    }

    // -------------------------------------------------------------------------
    // Message fetching
    // -------------------------------------------------------------------------

    private List<ConsumedMessage> fetchMessages(String topic, long fromOffset, int maxMessages) throws IOException {
        return fetchMessages(topic, fromOffset, maxMessages, 0);
    }

    private List<ConsumedMessage> fetchMessages(String topic, long fromOffset, int maxMessages, long timeoutMs) throws IOException {
        return fetchMessagesWithRetry(topic, fromOffset, maxMessages, timeoutMs, MAX_RETRIES);
    }

    private List<ConsumedMessage> fetchMessagesWithRetry(String topic, long fromOffset, int maxMessages,
                                                          long timeoutMs, int retriesLeft) throws IOException {
        IOException lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                ensureConnectedWithRetry();
                return fetchMessagesInternal(topic, fromOffset, maxMessages, timeoutMs);
            } catch (IOException e) {
                lastException = e;
                logger.warn("Failed to fetch messages from topic '{}' at offset {} from {}:{} (attempt {}/{}): {}",
                        topic, fromOffset, host, port, attempt + 1, MAX_RETRIES, e.getMessage());
                closeConnection();
                rotateToNextServer();
                try { Thread.sleep(RECONNECT_DELAY_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during message fetch retry", ie);
                }
            }
        }

        logger.error("Failed to fetch messages from topic '{}' after {} attempts", topic, MAX_RETRIES);
        throw new IOException("Failed to fetch messages after " + MAX_RETRIES + " attempts: " +
                (lastException != null ? lastException.getMessage() : "unknown error"));
    }

    private List<ConsumedMessage> fetchMessagesInternal(String topic, long fromOffset, int maxMessages, long timeoutMs) throws IOException {
        ConsumeRequest request = ConsumeRequest.newBuilder()
                .setTopic(topic)
                .setFromOffset(fromOffset)
                .setMaxMessages(maxMessages)
                .setTimeoutMs(timeoutMs)
                .build();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.CONSUME_REQUEST)
                .setPayload(request.toByteString())
                .build();

        sendEnvelope(envelope);

        MessageEnvelope responseEnvelope = receiveEnvelope();
        ConsumeResponse response = ConsumeResponse.parseFrom(responseEnvelope.getPayload());

        if (!response.getSuccess()) {
            if (tryRedirectToLeader(response.getErrorMessage())) {
                return fetchMessagesInternal(topic, fromOffset, maxMessages, timeoutMs);
            }
            throw new IOException("Consume failed: " + response.getErrorMessage());
        }

        List<ConsumedMessage> messages = new ArrayList<>();
        for (StoredMessage msg : response.getMessagesList()) {
            messages.add(new ConsumedMessage(
                    msg.getOffset(),
                    msg.getTopic(),
                    msg.getPayload().toByteArray(),
                    msg.hasKey() ? msg.getKey() : null,
                    msg.getTimestamp(),
                    msg.getStoredAt()
            ));
        }

        logger.debug("Fetched {} messages from topic '{}' starting at offset {}", messages.size(), topic, fromOffset);
        return messages;
    }

    // -------------------------------------------------------------------------
    // Low-level transport helpers
    // -------------------------------------------------------------------------

    private void sendEnvelope(MessageEnvelope envelope) throws IOException {
        byte[] bytes = envelope.toByteArray();
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
        outputStream.flush();
    }

    private MessageEnvelope receiveEnvelope() throws IOException {
        int length = inputStream.readInt();
        byte[] bytes = new byte[length];
        inputStream.readFully(bytes);
        return MessageEnvelope.parseFrom(bytes);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        if (connected) {
            try {
                if (outputStream != null) outputStream.close();
                if (inputStream != null) inputStream.close();
                if (socket != null) socket.close();
            } finally {
                connected = false;
                logger.info("Disconnected from broker");
            }
        }
    }

    // -------------------------------------------------------------------------
    // ConsumedMessage record
    // -------------------------------------------------------------------------

    public record ConsumedMessage(
            long offset,
            String topic,
            byte[] payload,
            String key,
            long timestamp,
            long storedAt
    ) {
        public String payloadAsString() {
            return new String(payload);
        }
    }
}
