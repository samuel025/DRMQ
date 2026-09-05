package com.drmq.client;

import com.drmq.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DRMQ Producer client for sending messages to the broker.
 */
public class DRMQProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DRMQProducer.class);
    private static final int MAX_RETRIES = 5;
    private static final long RECONNECT_DELAY_MS = 500;
    private static final long INFLIGHT_TIMEOUT_MS = 120_000;
    private int batchSizeBytes = 16384; // 16 KB default
    private long lingerMs = 5;
    private int maxInflight = 5;

    private String host;
    private int port;
    private final List<String[]> bootstrapServers;
    private int currentServerIndex = 0;
    private final Object writeLock = new Object();
    private final Object readLock = new Object();
    private final Object connectLock = new Object();

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private volatile boolean connected = false;
    private volatile boolean running = true;

    private final AtomicLong correlationCounter = new AtomicLong(0);
    private final ConcurrentHashMap<Long, InflightBatch> inflightBatches = new ConcurrentHashMap<>();
    private Semaphore inflightPermits;

    private static final int MAX_ACCUMULATOR_MESSAGES = 10000;
    private final java.util.concurrent.BlockingQueue<PendingMessage> accumulator = new java.util.concurrent.ArrayBlockingQueue<>(MAX_ACCUMULATOR_MESSAGES);
    private final java.util.concurrent.BlockingQueue<PendingAtomicMessage> atomicAccumulator = new java.util.concurrent.ArrayBlockingQueue<>(MAX_ACCUMULATOR_MESSAGES);
    
    private final java.util.concurrent.ConcurrentLinkedDeque<List<PendingMessage>> retryQueue = new java.util.concurrent.ConcurrentLinkedDeque<>();
    private final java.util.concurrent.ConcurrentLinkedDeque<List<PendingAtomicMessage>> atomicRetryQueue = new java.util.concurrent.ConcurrentLinkedDeque<>();
    
    private Thread senderThread;
    private Thread atomicSenderThread;
    private Thread readerThread;
    private Thread reaperThread;
    
    private static final long ACK_TIMEOUT_MS = 10_000;

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
        initThreads();
    }

    public DRMQProducer(String bootstrapServersStr) {
        this.bootstrapServers = new ArrayList<>(parseBootstrapServers(bootstrapServersStr));
        if (bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("No valid bootstrap servers: " + bootstrapServersStr);
        }
        this.currentServerIndex = ThreadLocalRandom.current().nextInt(bootstrapServers.size());
        this.host = bootstrapServers.get(currentServerIndex)[0];
        this.port = Integer.parseInt(bootstrapServers.get(currentServerIndex)[1]);
        initThreads();
    }

 
    private void initThreads() {
        this.inflightPermits = new Semaphore(maxInflight);
        senderThread = new Thread(this::senderLoop, "drmq-producer-sender");
        senderThread.start();
        atomicSenderThread = new Thread(this::atomicSenderLoop, "drmq-producer-atomic-sender");
        atomicSenderThread.start();
        readerThread = new Thread(this::readerLoop, "drmq-producer-reader");
        readerThread.setDaemon(true);
        readerThread.start();
        reaperThread = new Thread(this::reaperLoop, "drmq-producer-reaper");
        reaperThread.setDaemon(true);
        reaperThread.start();
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

    public void setBatchSizeBytes(int batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
    }

    public void setLingerMs(long lingerMs) {
        this.lingerMs = lingerMs;
    }

    public void setMaxInflight(int maxInflight) {
        if (maxInflight < 1) throw new IllegalArgumentException("maxInflight must be >= 1");
        this.maxInflight = maxInflight;
        this.inflightPermits = new Semaphore(maxInflight);
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
        socket.setTcpNoDelay(true);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
        connected = true;

        logger.info("Connected to broker at {}:{}", host, port);
    }

    private void ensureConnectedWithRetry() throws IOException {
        synchronized (connectLock) {
            if (isConnectedInternal()) return;

            IOException lastException = null;
            int totalAttempts = MAX_RETRIES * Math.max(1, bootstrapServers.size());

            for (int attempt = 0; attempt < totalAttempts; attempt++) {
                try {
                    connectInternal();
                    return;
                } catch (IOException e) {
                    lastException = handleConnectionFailure(attempt, totalAttempts, e);
                }
            }

            throw new IOException("Failed to connect after " + totalAttempts + " attempts: " +
                    (lastException != null ? lastException.getMessage() : "unknown error"));
        }
    }

    private boolean isConnectedInternal() {
        if (connected && socket != null && !socket.isClosed()) {
            return true;
        }
        if (socket != null && socket.isClosed()) {
            closeConnection();
        }
        return false;
    }

    private IOException handleConnectionFailure(int attempt, int totalAttempts, IOException e) throws IOException {
        logger.debug("Connection to {}:{} failed (attempt {}/{}): {}",
                host, port, attempt + 1, totalAttempts, e.getMessage());
        closeConnection();
        rotateToNextServer();
        try { 
            Thread.sleep(RECONNECT_DELAY_MS); 
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during connection retry", ie);
        }
        return e;
    }

    public CompletableFuture<SendResult> send(String topic, byte[] payload) {
        return send(topic, payload, null);
    }

    public CompletableFuture<SendResult> send(String topic, String message) {
        return send(topic, message.getBytes(StandardCharsets.UTF_8), null);
    }

    public CompletableFuture<SendResult> send(String topic, byte[] payload, String key) {
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        if (payload.length > 10 * 1024 * 1024) { 
            future.completeExceptionally(new IllegalArgumentException("Payload too large"));
            return future;
        }
        if (!accumulator.offer(new PendingMessage(topic, payload, key, future))) {
            future.completeExceptionally(new IllegalStateException("Producer accumulator is full (capacity: " + MAX_ACCUMULATOR_MESSAGES + "). Apply backpressure."));
        }
        return future;
    }

    private void senderLoop() {
        PendingMessage leftoverMsg = null;
        while (running || !accumulator.isEmpty() || leftoverMsg != null || !retryQueue.isEmpty()) { 
            try {
                if (processRetryQueue()) continue;

                PendingMessage firstMsg = leftoverMsg != null ? leftoverMsg : accumulator.poll(100, TimeUnit.MILLISECONDS);
                leftoverMsg = null;
                if (firstMsg == null) continue;

                List<PendingMessage> currentBatch = new ArrayList<>(1024);
                currentBatch.add(firstMsg);
                int currentBytes = firstMsg.payload.length;
                String currentTopic = firstMsg.topic;

                // Fast path: bulk-drain whatever is already in the accumulator
                if (currentBytes < batchSizeBytes) {
                    leftoverMsg = drainAccumulatorForBatch(currentBatch, currentBytes, currentTopic);
                    currentBytes = calculateBatchBytes(currentBatch);
                }

                // Slow path: if batch is still small after drain, linger-wait for more
                if (currentBytes < batchSizeBytes && leftoverMsg == null) {
                    leftoverMsg = lingerForMoreMessages(currentBatch, currentBytes, currentTopic, System.currentTimeMillis());
                }

                if (!currentBatch.isEmpty()) {
                    sendBatchFireAndForget(currentTopic, currentBatch);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in sender loop", e);
            }
        }
    }

    private boolean processRetryQueue() {
        List<PendingMessage> retryBatch = retryQueue.poll();
        if (retryBatch != null && !retryBatch.isEmpty()) {
            sendBatchFireAndForget(retryBatch.get(0).topic, retryBatch);
            return true;
        }
        return false;
    }

    private int calculateBatchBytes(List<PendingMessage> batch) {
        int bytes = 0;
        for (PendingMessage msg : batch) {
            bytes += msg.payload.length;
        }
        return bytes;
    }

    private PendingMessage drainAccumulatorForBatch(List<PendingMessage> currentBatch, int currentBytes, String currentTopic) {
        List<PendingMessage> bulk = new ArrayList<>(1024);
        accumulator.drainTo(bulk, 1024);
        for (int i = 0; i < bulk.size(); i++) {
            PendingMessage msg = bulk.get(i);
            if (!currentTopic.equals(msg.topic)) {
                requeueFromIndex(bulk, i + 1);
                return msg;
            }
            currentBatch.add(msg);
            currentBytes += msg.payload.length;
            if (currentBytes >= batchSizeBytes) {
                requeueFromIndex(bulk, i + 1);
                break;
            }
        }
        return null;
    }

    private void requeueFromIndex(List<PendingMessage> bulk, int startIndex) {
        for (int i = startIndex; i < bulk.size(); i++) {
            accumulator.offer(bulk.get(i));
        }
    }

    private PendingMessage lingerForMoreMessages(List<PendingMessage> currentBatch, int currentBytes, String currentTopic, long firstMsgTime) throws InterruptedException {
        while (currentBytes < batchSizeBytes) {
            long remaining = lingerMs - (System.currentTimeMillis() - firstMsgTime);
            if (remaining <= 0) break;

            PendingMessage msg = accumulator.poll(remaining, TimeUnit.MILLISECONDS);
            if (msg == null) break;
            
            if (!currentTopic.equals(msg.topic)) {
                return msg;
            }
            currentBatch.add(msg);
            currentBytes += msg.payload.length;
        }
        return null;
    }

    /**
     * Fire-and-forget batch send: acquires an inflight permit, writes the batch
     * to the socket, and registers it for async response matching by the reader thread.
     */
    private void sendBatchFireAndForget(String topic, List<PendingMessage> batch) {
        ProduceBatchRequest.Builder requestBuilder = ProduceBatchRequest.newBuilder()
                .setTopic(topic);

        for (PendingMessage pm : batch) {
            requestBuilder.addEntries(pm.batchEntry);
        }

        if (!acquireInflightPermitOrFail(batch.stream().map(pm -> pm.future).toList())) {
            return;
        }

        long corrId = correlationCounter.incrementAndGet();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_BATCH_REQUEST)
                .setPayload(requestBuilder.build().toByteString())
                .setCorrelationId(corrId)
                .build();

        inflightBatches.put(corrId, new InflightBatch(corrId, topic, batch, null, envelope));

        try {
            writeEnvelopeToSocket(envelope);
        } catch (IOException e) {
            handleSendFailure(corrId, e,
                    () -> retryQueue.addFirst(batch),
                    () -> failFutures(batch.stream().map(pm -> pm.future).toList(),
                            new IOException("Failed to send batch: " + e.getMessage(), e)));
        }
    }



    /**
     * Atomically sends messages to multiple topics in a single Raft entry.
     * Uses client-side batching to combine multiple atomic requests.
     */
    public CompletableFuture<Map<String, Long>> sendAtomic(Map<String, byte[]> topicMessages) {
        CompletableFuture<java.util.Map<String, Long>> future = new CompletableFuture<>();
        if (topicMessages.size() < 2) {
            future.completeExceptionally(new IllegalArgumentException("sendAtomic() requires at least 2 topics"));
            return future;
        }
        if (!atomicAccumulator.offer(new PendingAtomicMessage(topicMessages, future))) {
            future.completeExceptionally(new IllegalStateException("Atomic accumulator is full. Apply backpressure."));
        }
        return future;
    }

    private void atomicSenderLoop() {
        while (running || !atomicAccumulator.isEmpty() || !atomicRetryQueue.isEmpty()) {
            try {
                if (processAtomicRetryQueue()) continue;

                PendingAtomicMessage firstMsg = atomicAccumulator.poll(100, TimeUnit.MILLISECONDS);
                if (firstMsg == null) continue;

                List<PendingAtomicMessage> currentBatch = new ArrayList<>();
                currentBatch.add(firstMsg);
                int currentBytes = calculateAtomicMessageBytes(firstMsg);
                long firstMsgTime = System.currentTimeMillis();

                lingerForMoreAtomicMessages(currentBatch, currentBytes, firstMsgTime);

                if (!currentBatch.isEmpty()) {
                    sendAtomicBatchFireAndForget(currentBatch);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in atomic sender loop", e);
            }
        }
    }

    private boolean processAtomicRetryQueue() {
        List<PendingAtomicMessage> retryBatch = atomicRetryQueue.poll();
        if (retryBatch != null && !retryBatch.isEmpty()) {
            sendAtomicBatchFireAndForget(retryBatch);
            return true;
        }
        return false;
    }

    private int calculateAtomicMessageBytes(PendingAtomicMessage msg) {
        int bytes = 0;
        for (byte[] payload : msg.topicMessages.values()) {
            bytes += payload.length;
        }
        return bytes;
    }

    private void lingerForMoreAtomicMessages(List<PendingAtomicMessage> currentBatch, int currentBytes, long firstMsgTime) throws InterruptedException {
        while (currentBytes < batchSizeBytes) {
            long remaining = lingerMs - (System.currentTimeMillis() - firstMsgTime);
            if (remaining <= 0) break;

            PendingAtomicMessage msg = atomicAccumulator.poll(remaining, TimeUnit.MILLISECONDS);
            if (msg == null) break;
            currentBatch.add(msg);
            currentBytes += calculateAtomicMessageBytes(msg);
        }
    }

    private void sendAtomicBatchFireAndForget(List<PendingAtomicMessage> batch) {
        Map<Integer, java.util.Map<String, Integer>> relativeIndices = new java.util.HashMap<>();
        AtomicProduceRequest request = buildAtomicRequest(batch, relativeIndices);

        if (!acquireInflightPermitOrFail(batch.stream().map(pm -> pm.future).toList())) {
            return;
        }

        long corrId = correlationCounter.incrementAndGet();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.ATOMIC_PRODUCE_REQUEST)
                .setPayload(request.toByteString())
                .setCorrelationId(corrId)
                .build();

        inflightBatches.put(corrId, new InflightBatch(corrId, null, null, 
                new AtomicInflightData(batch, relativeIndices), envelope));

        try {
            writeEnvelopeToSocket(envelope);
        } catch (IOException e) {
            handleSendFailure(corrId, e,
                    () -> atomicRetryQueue.addFirst(batch),
                    () -> failFutures(batch.stream().map(pm -> pm.future).toList(),
                            new IOException("Failed to send atomic batch: " + e.getMessage(), e)));
        }
    }

    /**
     * Builds the protobuf AtomicProduceRequest from a batch of pending atomic messages.
     * Populates relativeIndicesOut with per-request topic→entry-index mappings for
     * resolving final offsets from the broker's base offsets.
     */
    private AtomicProduceRequest buildAtomicRequest(
            List<PendingAtomicMessage> batch,
            Map<Integer, Map<String, Integer>> relativeIndicesOut) {

        AtomicProduceRequest.Builder reqBuilder = AtomicProduceRequest.newBuilder();
        Map<String, AtomicBatchTopicSlice.Builder> sliceBuilders = new HashMap<>();

        for (int i = 0; i < batch.size(); i++) {
            PendingAtomicMessage pm = batch.get(i);
            Map<String, Integer> requestIndices = new HashMap<>();
            relativeIndicesOut.put(i, requestIndices);

            for (Map.Entry<String, byte[]> entry : pm.topicMessages.entrySet()) {
                String topic = entry.getKey();
                byte[] payload = entry.getValue();

                AtomicBatchTopicSlice.Builder sliceBuilder = sliceBuilders.computeIfAbsent(topic, 
                        k -> AtomicBatchTopicSlice.newBuilder().setTopic(k));
                
                int currentIndex = sliceBuilder.getEntriesCount();
                requestIndices.put(topic, currentIndex);

                sliceBuilder.addEntries(ProduceBatchRequest.BatchEntry.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                        .setClientTimestamp(pm.timestamp)
                        .build());
            }
        }

        for (AtomicBatchTopicSlice.Builder sb : sliceBuilders.values()) {
            reqBuilder.addSlices(sb.build());
        }
        return reqBuilder.build();
    }


    /**
     * Acquires an inflight permit, blocking up to INFLIGHT_TIMEOUT_MS.
     * On failure or interruption, completes all futures exceptionally and returns false.
     */
    private boolean acquireInflightPermitOrFail(List<? extends CompletableFuture<?>> futures) {
        try {
            if (!inflightPermits.tryAcquire(INFLIGHT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                failFutures(futures, new IOException("Inflight timeout: too many unacknowledged batches"));
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failFutures(futures, new IOException("Interrupted waiting for inflight permit"));
            return false;
        }
    }

    /**
     * Connects (if needed) and writes an envelope to the socket under the write lock.
     */
    private void writeEnvelopeToSocket(MessageEnvelope envelope) throws IOException {
        ensureConnectedWithRetry();
        synchronized (writeLock) {
            byte[] envelopeBytes = envelope.toByteArray();
            out.writeInt(envelopeBytes.length);
            out.write(envelopeBytes);
            out.flush();
        }
    }

    /**
     * Handles a send failure: removes the inflight batch, releases the permit,
     * closes the connection, then either retries or fails depending on whether
     * the producer is still running.
     */
    private void handleSendFailure(long corrId, IOException cause,
                                   Runnable retryAction, Runnable failAction) {
        InflightBatch removed = inflightBatches.remove(corrId);
        if (removed != null) {
            inflightPermits.release();
            closeConnection();
            if (running) {
                retryAction.run();
            } else {
                failAction.run();
            }
        }
    }

    /**
     * Completes all futures in the list exceptionally with the given cause.
     */
    @SuppressWarnings("rawtypes")
    private void failFutures(List<? extends CompletableFuture> futures, Exception cause) {
        for (CompletableFuture<?> f : futures) {
            f.completeExceptionally(cause);
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

    /**
     * Extracts the leader address from a NOT_LEADER error message.
     * The broker sends error messages in the format "NOT_LEADER:host:port".
     * @return the "host:port" string, or null if the format is unrecognised.
     */
    private String extractLeaderAddress(String errorMsg) {
        if (errorMsg == null) return null;
        String prefix = "NOT_LEADER:";
        int idx = errorMsg.indexOf(prefix);
        if (idx == -1) return null;
        String addr = errorMsg.substring(idx + prefix.length()).trim();
        if (addr.isEmpty() || "UNKNOWN".equals(addr)) return null;
        String[] parts = addr.split(":");
        if (parts.length != 2) return null;
        try {
            Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return null;
        }
        return addr;
    }

    private void closeConnection() {
        connected = false;
        try { if (in != null) in.close(); } catch (IOException ignored) {}
        try { if (out != null) out.close(); } catch (IOException ignored) {}
        try { if (socket != null && !socket.isClosed()) socket.close(); } catch (IOException ignored) {}
        in = null;
        out = null;
        socket = null;
    }

    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed();
    }

    @Override
    public void close() {
        running = false;
        synchronized (connectLock) {
            closeConnection();
        }
        joinThread(atomicSenderThread, 5000);
        joinThread(senderThread, 5000);
        joinThread(readerThread, 3000);
        if (reaperThread != null && reaperThread.isAlive()) {
            reaperThread.interrupt();
        }
        joinThread(reaperThread, 3000);
        // Fail any remaining inflight batches
        failAllInflight(new IOException("Producer closing"));
        logger.info("Disconnected from broker");
    }

    /**
     * Joins a thread with the given timeout, restoring the interrupt flag if interrupted.
     */
    private void joinThread(Thread thread, long timeoutMs) {
        if (thread != null && thread.isAlive()) {
            try {
                thread.join(timeoutMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void reaperLoop() {
        while (running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!connected) continue;

            long now = System.currentTimeMillis();
            boolean hasStuckBatch = false;
            for (InflightBatch batch : inflightBatches.values()) {
                if (now - batch.sentAtMs > ACK_TIMEOUT_MS) {
                    hasStuckBatch = true;
                    break;
                }
            }

            if (hasStuckBatch) {
                logger.warn("Inflight batch timed out after {} ms. Force closing connection to trigger retry.", ACK_TIMEOUT_MS);
                synchronized (connectLock) {
                    closeConnection();
                    rotateToNextServer();
                }
            }
        }
    }


    /**
     * Shared reader thread that reads responses from the socket and matches them
     * to inflight batches by correlation_id. Handles both regular batch and
     * atomic batch responses.
     */
    private void readerLoop() {
        while (running || !inflightBatches.isEmpty()) {
            try {
                if (checkAndHandleLostConnection()) {
                    Thread.sleep(50);
                    continue;
                }

                byte[] responseBytes = readResponseBytes();
                if (responseBytes == null) continue;

                processResponseEnvelope(responseBytes);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running) {
                    logger.error("Error in reader loop", e);
                }
            }
        }
    }

    private boolean checkAndHandleLostConnection() {
        if (!connected || socket == null || socket.isClosed() || in == null) {
            if (!inflightBatches.isEmpty()) {
                failAllInflight(new IOException("Connection lost"));
            }
            return true;
        }
        return false;
    }

    private byte[] readResponseBytes() {
        DataInputStream localIn = in;
        if (localIn == null) return null;
        try {
            int responseLength = localIn.readInt();
            byte[] responseBytes = new byte[responseLength];
            localIn.readFully(responseBytes);
            return responseBytes;
        } catch (IOException e) {
            if (!running && inflightBatches.isEmpty()) return null;
            if (running) {
                logger.debug("Reader: connection lost, failing inflight batches: {}", e.getMessage());
                synchronized (connectLock) {
                    closeConnection();
                    rotateToNextServer();
                }
                failAllInflight(e);
            }
            return null;
        }
    }

    private void processResponseEnvelope(byte[] responseBytes) throws com.google.protobuf.InvalidProtocolBufferException {
        MessageEnvelope responseEnvelope = MessageEnvelope.parseFrom(responseBytes);
        long corrId = responseEnvelope.getCorrelationId();

        InflightBatch batch = inflightBatches.remove(corrId);
        if (batch == null) {
            logger.warn("Received response for unknown correlation ID: {}", corrId);
            return;
        }

        inflightPermits.release();

        if (responseEnvelope.getType() == MessageType.PRODUCE_BATCH_RESPONSE) {
            processProduceBatchResponse(responseEnvelope, batch);
        } else if (responseEnvelope.getType() == MessageType.ATOMIC_PRODUCE_RESPONSE) {
            processAtomicProduceResponse(responseEnvelope, batch);
        } else {
            logger.warn("Unexpected response type: {}", responseEnvelope.getType());
            failInflightBatch(batch, new IOException("Unexpected response type: " + responseEnvelope.getType()));
        }
    }

    private void processProduceBatchResponse(MessageEnvelope responseEnvelope, InflightBatch batch) {
        try {
            ProduceBatchResponse response = ProduceBatchResponse.parseFrom(responseEnvelope.getPayload());

            if (response.getSuccess()) {
                handleSuccessfulBatchResponse(response, batch);
            } else {
                handleFailedBatchResponse(response, batch);
            }
        } catch (Exception e) {
            failInflightBatch(batch, e);
        }
    }

    private void handleSuccessfulBatchResponse(ProduceBatchResponse response, InflightBatch batch) {
        long baseOffset = response.getBaseOffset();
        if (batch.regularBatch != null) {
            for (int i = 0; i < batch.regularBatch.size(); i++) {
                batch.regularBatch.get(i).future.complete(SendResult.success(baseOffset + i));
            }
        }
    }

    private void handleFailedBatchResponse(ProduceBatchResponse response, InflightBatch batch) {
        ErrorCode errorCode = response.getErrorCode();
        String errorMsg = response.getErrorMessage();
               
        if (isNotLeaderError(errorCode, errorMsg) && running) {
            handleNotLeaderRedirect(errorMsg);
            if (batch.regularBatch != null) {
                retryQueue.addFirst(batch.regularBatch);
            }
        } else {
            if (batch.regularBatch != null) {
                for (PendingMessage pm : batch.regularBatch) {
                    pm.future.completeExceptionally(new IOException(
                            errorCode == ErrorCode.NOT_LEADER ? "NOT_LEADER" : errorMsg));
                }
            }
        }
    }

    private void handleNotLeaderRedirect(String errorMsg) {
        String leaderAddr = extractLeaderAddress(errorMsg);
        if (leaderAddr != null) {
            try {
                redirectToLeader(leaderAddr);
                return;
            } catch (IOException e) {
                logger.warn("Failed to redirect to leader {}: {}", leaderAddr, e.getMessage());
            }
        }
        rotateToNextServer();
        closeConnection();
    }

    private void processAtomicProduceResponse(MessageEnvelope responseEnvelope, InflightBatch batch) {
        try {
            AtomicProduceResponse response = AtomicProduceResponse.parseFrom(responseEnvelope.getPayload());
            AtomicInflightData atomicData = batch.atomicData;
            if (atomicData == null) return;

            if (response.getSuccess()) {
                handleSuccessfulAtomicResponse(response, atomicData);
            } else {
                handleFailedAtomicResponse(response, atomicData);
            }
        } catch (Exception e) {
            failInflightBatch(batch, e);
        }
    }

    private void handleSuccessfulAtomicResponse(AtomicProduceResponse response, AtomicInflightData atomicData) {
        java.util.Map<String, Long> baseOffsets = response.getBaseOffsetsMap();
        for (int i = 0; i < atomicData.batch.size(); i++) {
            PendingAtomicMessage pm = atomicData.batch.get(i);
            Map<String, Integer> reqIndices = atomicData.relativeIndices.get(i);
            Map<String, Long> finalOffsets = new java.util.HashMap<>();

            for (String topic : pm.topicMessages.keySet()) {
                long base = baseOffsets.getOrDefault(topic, -1L);
                if (base != -1L) {
                    finalOffsets.put(topic, base + reqIndices.get(topic));
                }
            }
            pm.future.complete(finalOffsets);
        }
    }

    private void handleFailedAtomicResponse(AtomicProduceResponse response, AtomicInflightData atomicData) {
        ErrorCode errorCode = response.getErrorCode();
        String errorMsg = response.getErrorMessage();
        
        if (isNotLeaderError(errorCode, errorMsg) && running) {
            handleNotLeaderRedirect(errorMsg);
            atomicRetryQueue.addFirst(atomicData.batch);
        } else {
            for (PendingAtomicMessage pm : atomicData.batch) {
                pm.future.completeExceptionally(new IOException("Failed to send atomic batch: " + errorMsg));
            }
        }
    }

    /**
     * Checks whether an error response indicates the broker is not the Raft leader.
     */
    private boolean isNotLeaderError(ErrorCode errorCode, String errorMsg) {
        return errorCode == ErrorCode.NOT_LEADER || (errorMsg != null && errorMsg.contains("NOT_LEADER"));
    }

    private void failInflightBatch(InflightBatch batch, Exception cause) {
        IOException wrapped = cause instanceof IOException ? (IOException) cause : new IOException(cause);
        if (batch.regularBatch != null) {
            for (PendingMessage pm : batch.regularBatch) {
                pm.future.completeExceptionally(wrapped);
            }
        }
        if (batch.atomicData != null) {
            for (PendingAtomicMessage pm : batch.atomicData.batch) {
                pm.future.completeExceptionally(wrapped);
            }
        }
    }

    private void failAllInflight(Exception cause) {
        List<InflightBatch> failed = new ArrayList<>(inflightBatches.values());
        inflightBatches.clear();
        for (InflightBatch batch : failed) {
            inflightPermits.release();
            if (running) {
                if (batch.regularBatch != null) {
                    retryQueue.add(batch.regularBatch);
                } else if (batch.atomicData != null) {
                    atomicRetryQueue.add(batch.atomicData.batch);
                }
            } else {
                failInflightBatch(batch, cause);
            }
        }
    }


    /** Tracks a batch that has been sent but not yet acknowledged. */
    private static class InflightBatch {
        final long correlationId;
        final String topic;
        final List<PendingMessage> regularBatch;
        final AtomicInflightData atomicData;
        final MessageEnvelope envelope;
        final long sentAtMs;

        InflightBatch(long correlationId, String topic, List<PendingMessage> regularBatch,
                      AtomicInflightData atomicData, MessageEnvelope envelope) {
            this.correlationId = correlationId;
            this.topic = topic;
            this.regularBatch = regularBatch;
            this.atomicData = atomicData;
            this.envelope = envelope;
            this.sentAtMs = System.currentTimeMillis();
        }
    }

    /** Data needed to process an atomic batch response. */
    private static class AtomicInflightData {
        final List<PendingAtomicMessage> batch;
        final java.util.Map<Integer, java.util.Map<String, Integer>> relativeIndices;

        AtomicInflightData(List<PendingAtomicMessage> batch,
                           java.util.Map<Integer, java.util.Map<String, Integer>> relativeIndices) {
            this.batch = batch;
            this.relativeIndices = relativeIndices;
        }
    }

    private static class PendingMessage {
        final String topic;
        final byte[] payload;
        final String key;
        final long timestamp;
        final CompletableFuture<SendResult> future;
        final ProduceBatchRequest.BatchEntry batchEntry;

        PendingMessage(String topic, byte[] payload, String key, CompletableFuture<SendResult> future) {
            this.topic = topic;
            this.payload = payload;
            this.key = key;
            this.timestamp = System.currentTimeMillis();
            this.future = future;

            ProduceBatchRequest.BatchEntry.Builder builder = ProduceBatchRequest.BatchEntry.newBuilder()
                    .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                    .setClientTimestamp(this.timestamp);
            if (key != null) {
                builder.setKey(key);
            }
            this.batchEntry = builder.build();
        }
    }

    private static class PendingAtomicMessage {
        final java.util.Map<String, byte[]> topicMessages;
        final CompletableFuture<java.util.Map<String, Long>> future;
        final long timestamp;

        PendingAtomicMessage(java.util.Map<String, byte[]> topicMessages, CompletableFuture<java.util.Map<String, Long>> future) {
            this.topicMessages = topicMessages;
            this.future = future;
            this.timestamp = System.currentTimeMillis();
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
