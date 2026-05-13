package com.drmq.broker;

import com.drmq.broker.raft.RaftNode;
import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Handles a single client connection to the broker.
 *
 * Wire protocol: [4-byte big-endian length][MessageEnvelope protobuf bytes]
 * Both client traffic (produce/consume) and Raft peer RPCs (RequestVote,
 * AppendEntries) use this same framing on the same TCP port.
 *
 * The handler reads one envelope at a time, dispatches to the appropriate
 * handler based on MessageType, and writes back a response envelope.
 */
public class ClientHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket socket;
    private final MessageStore messageStore;
    private final OffsetManager offsetManager;
    private final RaftNode raftNode;  // null in single-node mode
    private final Set<ClientHandler> ownerSet;  // for self-removal on disconnect
    private volatile boolean running = true;
    
  
    private static final ExecutorService rpcExecutor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            r -> {
                Thread t = new Thread(r, "raft-rpc-handler");
                t.setDaemon(true);
                return t;
            });

    public ClientHandler(Socket socket, MessageStore messageStore, OffsetManager offsetManager,
                         RaftNode raftNode, Set<ClientHandler> ownerSet) {
        this.socket = socket;
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.raftNode = raftNode;
        this.ownerSet = ownerSet;
    }

    public ClientHandler(Socket socket, MessageStore messageStore, OffsetManager offsetManager, RaftNode raftNode) {
        this(socket, messageStore, offsetManager, raftNode, null);
    }

    /** Backward-compatible constructor for single-node mode */
    public ClientHandler(Socket socket, MessageStore messageStore, OffsetManager offsetManager) {
        this(socket, messageStore, offsetManager, null);
    }

    @Override
    public void run() {
        String clientAddress = socket.getRemoteSocketAddress().toString();
        logger.info("Client connected: {}", clientAddress);
        BrokerMetrics.get().recordConnectionOpened();

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            while (running && !socket.isClosed()) {
                try {
                    int length = in.readInt();
                    if (length <= 0 || length > 10 * 1024 * 1024) { // Max 10MB message
                        logger.warn("Invalid message length: {}", length);
                        break;
                    }

                    byte[] envelopeBytes = new byte[length];
                    in.readFully(envelopeBytes);

                    MessageEnvelope envelope = MessageEnvelope.parseFrom(envelopeBytes);
                    MessageEnvelope response = handleMessage(envelope);
                    byte[] responseBytes = response.toByteArray();
                    out.writeInt(responseBytes.length);
                    out.write(responseBytes);
                    out.flush();

                } catch (EOFException e) {
                    logger.info("Client disconnected: {}", clientAddress);
                    break;
                }
            }

        } catch (IOException e) {
            if (running) {
                logger.error("Error handling client {}: {}", clientAddress, e.getMessage());
            }
        } finally {
            if (ownerSet != null) {
                ownerSet.remove(this);
            }
            closeSocket();
            BrokerMetrics.get().recordConnectionClosed();
        }
    }

    /**
     * Dispatch incoming message to appropriate handler based on type.
     */
    private MessageEnvelope handleMessage(MessageEnvelope envelope) throws IOException {
        return switch (envelope.getType()) {
            case PRODUCE_REQUEST -> handleProduceRequest(envelope);
            case CONSUME_REQUEST -> handleConsumeRequest(envelope);
            case COMMIT_OFFSET_REQUEST -> handleCommitOffsetRequest(envelope);
            case FETCH_OFFSET_REQUEST -> handleFetchOffsetRequest(envelope);
            case REQUEST_VOTE_REQUEST -> handleRequestVoteRequest(envelope);
            case APPEND_ENTRIES_REQUEST -> handleAppendEntriesRequest(envelope);
            default -> createErrorResponse("Unknown message type: " + envelope.getType());
        };
    }

    /**
     * Handle a produce request - store the message and return the assigned offset.
     */
    private MessageEnvelope handleProduceRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        long payloadBytes = 0;
        try {
            ProduceRequest request = ProduceRequest.parseFrom(envelope.getPayload());

            String topic = request.getTopic();
            byte[] payload = request.getPayload().toByteArray();
            payloadBytes = payload.length;
            String key = request.hasKey() ? request.getKey() : null;
            long timestamp = request.getTimestamp();

            long offset;
            if (raftNode != null) {
                if (!raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    return createProduceErrorResponse("NOT_LEADER:" +
                            (leaderAddr != null ? leaderAddr : "UNKNOWN"));
                }
                offset = raftNode.propose(topic, payload, key, timestamp);
            } else {
                offset = messageStore.append(topic, payload, key, timestamp);
            }

            logger.debug("Produced message: topic={}, offset={}", topic, offset);

            // Build success response
            ProduceResponse response = ProduceResponse.newBuilder()
                    .setSuccess(true)
                    .setOffset(offset)
                    .build();

                BrokerMetrics.get().recordRequest("produce", true,
                    System.nanoTime() - startNanos, payloadBytes, 1);

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.PRODUCE_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing produce request", e);
                BrokerMetrics.get().recordRequest("produce", false,
                    System.nanoTime() - startNanos, payloadBytes, 1);
            return createProduceErrorResponse(e.getMessage());
        }
    }

    /**
     * Handle a consume request - fetch messages from the specified offset.
     * If timeout_ms > 0, uses long-polling: waits efficiently via wait()/notifyAll()
     * until messages arrive or the timeout expires.
     */
    private MessageEnvelope handleConsumeRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        try {
            ConsumeRequest request = ConsumeRequest.parseFrom(envelope.getPayload());

            String topic      = request.getTopic();
            long fromOffset   = request.getFromOffset();
            int maxMessages   = request.getMaxMessages();
            long timeoutMs    = request.getTimeoutMs(); // 0 = short poll

            // Fetch messages — uses efficient wait/notify for long-polling (no thread sleep-loop)
            var messages = (timeoutMs > 0)
                    ? messageStore.waitForMessages(topic, fromOffset, maxMessages, timeoutMs)
                    : messageStore.getMessages(topic, fromOffset, maxMessages);

            logger.debug("Consumed {} messages: topic={}, fromOffset={}, longPoll={}",
                    messages.size(), topic, fromOffset, timeoutMs > 0);

            ConsumeResponse response = ConsumeResponse.newBuilder()
                    .setSuccess(true)
                    .addAllMessages(messages)
                    .build();

                BrokerMetrics.get().recordRequest("consume", true,
                    System.nanoTime() - startNanos, estimatePayloadBytes(messages), messages.size());

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.CONSUME_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing consume request", e);
                BrokerMetrics.get().recordRequest("consume", false,
                    System.nanoTime() - startNanos, 0, 0);
            return createConsumeErrorResponse(e.getMessage());
        }
    }

    /**
     * Create an error response envelope for produce requests.
     */
    private MessageEnvelope createProduceErrorResponse(String errorMessage) {
        ProduceResponse response = ProduceResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    /**
     * Create an error response envelope for consume requests.
     */
    private MessageEnvelope createConsumeErrorResponse(String errorMessage) {
        ConsumeResponse response = ConsumeResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.CONSUME_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    /**
     * Handle a commit offset request - store the consumer group offset on the broker.
     *
     * In cluster mode, offset commits are routed through Raft consensus to ensure
     * they survive leader failover. In single-node mode, offsets are persisted locally.
     */
    private MessageEnvelope handleCommitOffsetRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        try {
            CommitOffsetRequest request = CommitOffsetRequest.parseFrom(envelope.getPayload());

            String group = request.getConsumerGroup();
            String topic = request.getTopic();
            long offset  = request.getOffset();

            if (raftNode != null) {
                // Cluster mode: replicate offset commit through Raft consensus
                if (!raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("NOT_LEADER:" +
                                    (leaderAddr != null ? leaderAddr : "UNKNOWN"))
                            .build();
                    return MessageEnvelope.newBuilder()
                            .setType(MessageType.COMMIT_OFFSET_RESPONSE)
                            .setPayload(response.toByteString())
                            .build();
                }
                raftNode.proposeOffsetCommit(group, topic, offset);
            } else {
                offsetManager.commit(group, topic, offset);
            }

            logger.debug("Committed offset: group={}, topic={}, offset={}", group, topic, offset);

            CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                    .setSuccess(true)
                    .build();

                BrokerMetrics.get().recordRequest("commit_offset", true,
                    System.nanoTime() - startNanos, 0, 0);

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.COMMIT_OFFSET_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error committing offset", e);
                BrokerMetrics.get().recordRequest("commit_offset", false,
                    System.nanoTime() - startNanos, 0, 0);
            CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "Unknown error")
                    .build();
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.COMMIT_OFFSET_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        }
    }

    /**
     * Handle a fetch offset request - return the committed offset for a consumer group.
     */
    private MessageEnvelope handleFetchOffsetRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        try {
            FetchOffsetRequest request = FetchOffsetRequest.parseFrom(envelope.getPayload());

            String group = request.getConsumerGroup();
            String topic = request.getTopic();
            long offset  = offsetManager.fetch(group, topic);

            logger.debug("Fetched offset: group={}, topic={}, offset={}", group, topic, offset);

            FetchOffsetResponse response = FetchOffsetResponse.newBuilder()
                    .setSuccess(true)
                    .setOffset(offset)
                    .build();

                BrokerMetrics.get().recordRequest("fetch_offset", true,
                    System.nanoTime() - startNanos, 0, 0);

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.FETCH_OFFSET_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error fetching offset", e);
                BrokerMetrics.get().recordRequest("fetch_offset", false,
                    System.nanoTime() - startNanos, 0, 0);
            FetchOffsetResponse response = FetchOffsetResponse.newBuilder()
                    .setSuccess(false)
                    .setOffset(-1)
                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "Unknown error")
                    .build();
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.FETCH_OFFSET_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        }
    }

    /**
     * Create a generic error response envelope (deprecated - use specific error methods).
     */
    @Deprecated
    private MessageEnvelope createErrorResponse(String errorMessage) {
        return createProduceErrorResponse(errorMessage);
    }

    // ===========================
    //  Raft RPC handlers
    // ===========================

    /**
     * Handle an incoming RequestVote RPC from a Raft candidate.
     */
    private MessageEnvelope handleRequestVoteRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        if (raftNode == null) {
            BrokerMetrics.get().recordRaftRpc("request_vote", false,
                    System.nanoTime() - startNanos);
            return createErrorResponse("Raft not enabled on this broker");
        }
        RequestVoteRequest request = RequestVoteRequest.parseFrom(envelope.getPayload());
        
        // Process synchronously but in a dedicated executor to prevent blocking
        // the ClientHandler thread while holding RaftNode.lock
        try {
            RequestVoteResponse response = CompletableFuture.supplyAsync(
                    () -> raftNode.handleRequestVote(request),
                    rpcExecutor
            ).get(10, TimeUnit.SECONDS);
            
            BrokerMetrics.get().recordRaftRpc("request_vote", true,
                    System.nanoTime() - startNanos);
            
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.REQUEST_VOTE_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        } catch (TimeoutException e) {
            logger.error("RequestVote handler timed out");
            BrokerMetrics.get().recordRaftRpc("request_vote", false,
                    System.nanoTime() - startNanos);
            return createErrorResponse("RequestVote timeout");
        } catch (Exception e) {
            logger.error("RequestVote handler failed: {}", e.getMessage());
            BrokerMetrics.get().recordRaftRpc("request_vote", false,
                    System.nanoTime() - startNanos);
            return createErrorResponse("RequestVote failed: " + e.getMessage());
        }
    }

    /**
     * Handle an incoming AppendEntries RPC from the Raft leader.
     */
    private MessageEnvelope handleAppendEntriesRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        if (raftNode == null) {
            BrokerMetrics.get().recordRaftRpc("append_entries", false,
                    System.nanoTime() - startNanos);
            return createErrorResponse("Raft not enabled on this broker");
        }
        
        AppendEntriesRequest request = AppendEntriesRequest.parseFrom(envelope.getPayload());
        try {
            AppendEntriesResponse response = CompletableFuture.supplyAsync(
                    () -> raftNode.handleAppendEntries(request),
                    rpcExecutor
            ).get(10, TimeUnit.SECONDS);
            
            BrokerMetrics.get().recordRaftRpc("append_entries", response.getSuccess(),
                    System.nanoTime() - startNanos);
            
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.APPEND_ENTRIES_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        } catch (TimeoutException e) {
            logger.error("AppendEntries handler timed out");
            BrokerMetrics.get().recordRaftRpc("append_entries", false,
                    System.nanoTime() - startNanos);
            return createErrorResponse("AppendEntries timeout");
        } catch (Exception e) {
            logger.error("AppendEntries handler failed: {}", e.getMessage());
            BrokerMetrics.get().recordRaftRpc("append_entries", false,
                    System.nanoTime() - startNanos);
            return createErrorResponse("AppendEntries failed: " + e.getMessage());
        }
    }

    /**
     * Stop processing and close the connection.
     */
    public void stop() {
        running = false;
        closeSocket();
    }

    private void closeSocket() {
        try {
            if (!socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.debug("Error closing socket", e);
        }
    }

    private static long estimatePayloadBytes(java.util.List<StoredMessage> messages) {
        long total = 0;
        for (StoredMessage message : messages) {
            total += message.getPayload().size();
        }
        return total;
    }
}
