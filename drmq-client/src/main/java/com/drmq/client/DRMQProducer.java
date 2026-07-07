package com.drmq.client;

import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * DRMQ Producer client for sending messages to the broker.
 * Supports asynchronous client-side batching for high throughput.
 */
public class DRMQProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DRMQProducer.class);
    private static final int MAX_RETRIES = 5;
    private static final long RECONNECT_DELAY_MS = 500;
    private static final int BATCH_SIZE_BYTES = 16384; // 16KB
    private static final long LINGER_MS = 5;

    private String host;
    private int port;
    private final List<String[]> bootstrapServers;
    private int currentServerIndex = 0;
    private final Object sendLock = new Object();

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private volatile boolean connected = false;
    private volatile boolean running = true;

    private final ConcurrentLinkedQueue<PendingMessage> accumulator = new ConcurrentLinkedQueue<>();
    private final Thread senderThread;

    public DRMQProducer(String host, int port) {
        List<String[]> parsed = host != null && host.contains(",") ? parseBootstrapServers(host) : List.of();
        if (!parsed.isEmpty()) {
            this.bootstrapServers = new ArrayList<>(parsed);
            this.currentServerIndex = ThreadLocalRandom.current().nextInt(bootstrapServers.size());
            this.host = bootstrapServers.get(currentServerIndex)[0];
            this.port = Integer.parseInt(bootstrapServers.get(currentServerIndex)[1]);
        } else {
            this.host = host;
            this.port = port;
            this.bootstrapServers = new ArrayList<>();
            this.bootstrapServers.add(new String[]{host, String.valueOf(port)});
        }
        senderThread = new Thread(this::senderLoop, "drmq-producer-sender");
        senderThread.start();
    }

    public DRMQProducer(String bootstrapServersStr) {
        this.bootstrapServers = new ArrayList<>(parseBootstrapServers(bootstrapServersStr));
        if (bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("No valid bootstrap servers: " + bootstrapServersStr);
        }
        this.currentServerIndex = ThreadLocalRandom.current().nextInt(bootstrapServers.size());
        this.host = bootstrapServers.get(currentServerIndex)[0];
        this.port = Integer.parseInt(bootstrapServers.get(currentServerIndex)[1]);
        senderThread = new Thread(this::senderLoop, "drmq-producer-sender");
        senderThread.start();
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

    public DRMQProducer() {
        this("localhost", 9092);
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
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        connected = true;

        logger.info("Connected to broker at {}:{}", host, port);
    }

    private void ensureConnectedWithRetry() throws IOException {
        if (connected && socket != null && !socket.isClosed()) {
            return;
        }
        if (socket != null && socket.isClosed()) {
            closeConnection();
        }

        IOException lastException = null;
        int totalAttempts = MAX_RETRIES * Math.max(1, bootstrapServers.size());

        for (int attempt = 0; attempt < totalAttempts; attempt++) {
            try {
                connectInternal();
                return;
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

    public CompletableFuture<SendResult> send(String topic, byte[] payload) {
        return send(topic, payload, null);
    }

    public CompletableFuture<SendResult> send(String topic, String message) {
        return send(topic, message.getBytes(StandardCharsets.UTF_8), null);
    }

    public CompletableFuture<SendResult> send(String topic, byte[] payload, String key) {
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        if (payload.length > 10 * 1024 * 1024) { // 10MB sanity check
            future.completeExceptionally(new IllegalArgumentException("Payload too large"));
            return future;
        }
        accumulator.add(new PendingMessage(topic, payload, key, future));
        return future;
    }

    private void senderLoop() {
        while (running || !accumulator.isEmpty()) {
            try {
                if (accumulator.isEmpty()) {
                    Thread.sleep(1);
                    continue;
                }

                // Group messages by topic
                List<PendingMessage> currentBatch = new ArrayList<>();
                int currentBytes = 0;
                long firstMsgTime = System.currentTimeMillis();
                String currentTopic = null;

                while (!accumulator.isEmpty() && currentBytes < BATCH_SIZE_BYTES) {
                    PendingMessage peeked = accumulator.peek();
                    if (currentTopic == null) {
                        currentTopic = peeked.topic;
                    } else if (!currentTopic.equals(peeked.topic)) {
                        break; // Batches are per-topic
                    }

                    PendingMessage msg = accumulator.poll();
                    currentBatch.add(msg);
                    currentBytes += msg.payload.length;
                    
                    if (System.currentTimeMillis() - firstMsgTime >= LINGER_MS) {
                        break;
                    }
                }

                if (!currentBatch.isEmpty()) {
                    sendBatchWithRetry(currentTopic, currentBatch, MAX_RETRIES);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in sender loop", e);
            }
        }
    }

    private void sendBatchWithRetry(String topic, List<PendingMessage> batch, int retriesLeft) {
        ProduceBatchRequest.Builder requestBuilder = ProduceBatchRequest.newBuilder()
                .setTopic(topic);

        for (PendingMessage pm : batch) {
            ProduceBatchRequest.BatchEntry.Builder entryBuilder = ProduceBatchRequest.BatchEntry.newBuilder()
                    .setPayload(com.google.protobuf.ByteString.copyFrom(pm.payload))
                    .setClientTimestamp(pm.timestamp);
            if (pm.key != null) {
                entryBuilder.setKey(pm.key);
            }
            requestBuilder.addEntries(entryBuilder.build());
        }

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_BATCH_REQUEST)
                .setPayload(requestBuilder.build().toByteString())
                .build();

        IOException lastException = null;
        long deliveryTimeoutMs = 120_000; // 2 minutes max wait time for cluster healing
        long startMs = System.currentTimeMillis();
        long currentBackoffMs = 100;

        while (true) {
            if (System.currentTimeMillis() - startMs > deliveryTimeoutMs) {
                break; // Give up after 2 minutes
            }

            try {
                ensureConnectedWithRetry();
                
                synchronized (sendLock) {
                    byte[] envelopeBytes = envelope.toByteArray();
                    out.writeInt(envelopeBytes.length);
                    out.write(envelopeBytes);
                    out.flush();

                    int responseLength = in.readInt();
                    byte[] responseBytes = new byte[responseLength];
                    in.readFully(responseBytes);

                    MessageEnvelope responseEnvelope = MessageEnvelope.parseFrom(responseBytes);
                    ProduceBatchResponse response = ProduceBatchResponse.parseFrom(responseEnvelope.getPayload());

                    if (response.getSuccess()) {
                        long baseOffset = response.getBaseOffset();
                        for (int i = 0; i < batch.size(); i++) {
                            batch.get(i).future.complete(SendResult.success(baseOffset + i));
                        }
                        return; // Success
                    } else {
                        String errorMsg = response.getErrorMessage();
                        if (errorMsg != null && errorMsg.startsWith("NOT_LEADER:")) {
                            String leaderAddr = errorMsg.substring("NOT_LEADER:".length());
                            if (!leaderAddr.equals("UNKNOWN")) {
                                try {
                                    redirectToLeader(leaderAddr);
                                    continue; // Try again immediately on new leader
                                } catch (IOException e) {
                                    lastException = e;
                                    closeConnection();
                                    rotateToNextServer();
                                }
                            } else {
                                lastException = new IOException("Leader unknown");
                                closeConnection();
                                rotateToNextServer();
                            }
                        } else if (errorMsg != null && (
                                errorMsg.contains("timed out") || 
                                errorMsg.contains("Lost leadership") ||
                                errorMsg.contains("Raft batch proposal")
                        )) {
                            lastException = new IOException("Broker cluster error: " + errorMsg);
                            closeConnection();
                            rotateToNextServer();
                        } else {
                            // Fatal error from broker (e.g., bad payload), do not retry
                            for (PendingMessage pm : batch) {
                                pm.future.complete(SendResult.failure(errorMsg));
                            }
                            return;
                        }
                    }
                }
            } catch (IOException e) {
                lastException = e;
                closeConnection();
                rotateToNextServer();
            }

            // Exponential backoff before retrying
            try { Thread.sleep(currentBackoffMs); } catch (InterruptedException ignored) {}
            currentBackoffMs = Math.min(2000, currentBackoffMs * 2);
        }

        // Complete exceptionally if we exhausted retries
        for (PendingMessage pm : batch) {
            pm.future.completeExceptionally(new IOException("Failed to send batch: " + 
                (lastException != null ? lastException.getMessage() : "unknown error")));
        }
    }

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
            if (server.length != 2) continue;
            try {
                int serverPort = Integer.parseInt(server[1]);
                if (server[0].equals(host) && serverPort == port) {
                    currentServerIndex = i;
                    return;
                }
            } catch (NumberFormatException ignored) {}
        }
    }

    private void redirectToLeader(String leaderAddr) throws IOException {
        String[] parts = leaderAddr.split(":");
        if (parts.length != 2) {
            throw new IOException("Invalid leader address: " + leaderAddr);
        }
        try {
            this.port = Integer.parseInt(parts[1]);
            this.host = parts[0];
            syncServerIndexToCurrent();
        } catch (NumberFormatException e) {
            throw new IOException("Invalid port in leader address: " + leaderAddr, e);
        }
        closeConnection();
        logger.info("Redirected to leader at {}:{}", host, port);
    }

    private void closeConnection() {
        connected = false;
        try { if (in != null) in.close(); } catch (IOException ignored) {}
        try { if (out != null) out.close(); } catch (IOException ignored) {}
        try { if (socket != null && !socket.isClosed()) socket.close(); } catch (IOException ignored) {}
    }

    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed();
    }

    @Override
    public void close() {
        running = false;
        if (senderThread != null && senderThread.isAlive()) {
            try {
                senderThread.join(5000); // Wait up to 5 seconds to flush accumulator
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        closeConnection();
        logger.info("Disconnected from broker");
    }

    private static class PendingMessage {
        final String topic;
        final byte[] payload;
        final String key;
        final long timestamp;
        final CompletableFuture<SendResult> future;

        PendingMessage(String topic, byte[] payload, String key, CompletableFuture<SendResult> future) {
            this.topic = topic;
            this.payload = payload;
            this.key = key;
            this.timestamp = System.currentTimeMillis();
            this.future = future;
        }
    }

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
