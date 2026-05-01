package com.drmq.broker;

import com.drmq.broker.raft.RaftNode;
import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

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
    private volatile boolean running = true;

    public ClientHandler(Socket socket, MessageStore messageStore, OffsetManager offsetManager, RaftNode raftNode) {
        this.socket = socket;
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.raftNode = raftNode;
    }

    /** Backward-compatible constructor for single-node mode */
    public ClientHandler(Socket socket, MessageStore messageStore, OffsetManager offsetManager) {
        this(socket, messageStore, offsetManager, null);
    }

    @Override
    public void run() {
        String clientAddress = socket.getRemoteSocketAddress().toString();
        logger.info("Client connected: {}", clientAddress);

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            while (running && !socket.isClosed()) {
                try {
                    // Read message length prefix (4 bytes, big-endian)
                    // This tells us exactly how many bytes to read for the envelope
                    int length = in.readInt();
                    if (length <= 0 || length > 10 * 1024 * 1024) { // Max 10MB message
                        logger.warn("Invalid message length: {}", length);
                        break;
                    }

                    // Read the envelope
                    byte[] envelopeBytes = new byte[length];
                    in.readFully(envelopeBytes);

                    MessageEnvelope envelope = MessageEnvelope.parseFrom(envelopeBytes);
                    MessageEnvelope response = handleMessage(envelope);

                    // Send response
                    byte[] responseBytes = response.toByteArray();
                    out.writeInt(responseBytes.length);
                    out.write(responseBytes);
                    out.flush();

                } catch (EOFException e) {
                    // Client disconnected normally
                    logger.info("Client disconnected: {}", clientAddress);
                    break;
                }
            }

        } catch (IOException e) {
            if (running) {
                logger.error("Error handling client {}: {}", clientAddress, e.getMessage());
            }
        } finally {
            closeSocket();
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
            // Raft RPCs
            case REQUEST_VOTE_REQUEST -> handleRequestVoteRequest(envelope);
            case APPEND_ENTRIES_REQUEST -> handleAppendEntriesRequest(envelope);
            default -> createErrorResponse("Unknown message type: " + envelope.getType());
        };
    }

    /**
     * Handle a produce request - store the message and return the assigned offset.
     */
    private MessageEnvelope handleProduceRequest(MessageEnvelope envelope) throws IOException {
        try {
            ProduceRequest request = ProduceRequest.parseFrom(envelope.getPayload());

            String topic = request.getTopic();
            byte[] payload = request.getPayload().toByteArray();
            String key = request.hasKey() ? request.getKey() : null;
            long timestamp = request.getTimestamp();

            long offset;
            if (raftNode != null) {
                // Cluster mode: route through Raft consensus
                if (!raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    return createProduceErrorResponse("NOT_LEADER:" +
                            (leaderAddr != null ? leaderAddr : "UNKNOWN"));
                }
                // Leader: propose via Raft — blocks until committed to majority
                raftNode.propose(topic, payload, key, timestamp);
                // After Raft commit, the entry is applied to MessageStore.
                // Use the MessageStore's current offset as the user-facing offset.
                offset = messageStore.getCurrentOffset() - 1;
            } else {
                // Single-node mode: write directly to MessageStore (existing behavior)
                offset = messageStore.append(topic, payload, key, timestamp);
            }

            logger.debug("Produced message: topic={}, offset={}", topic, offset);

            // Build success response
            ProduceResponse response = ProduceResponse.newBuilder()
                    .setSuccess(true)
                    .setOffset(offset)
                    .build();

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.PRODUCE_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing produce request", e);
            return createProduceErrorResponse(e.getMessage());
        }
    }

    /**
     * Handle a consume request - fetch messages from the specified offset.
     * If timeout_ms > 0, uses long-polling: waits efficiently via wait()/notifyAll()
     * until messages arrive or the timeout expires.
     */
    private MessageEnvelope handleConsumeRequest(MessageEnvelope envelope) throws IOException {
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

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.CONSUME_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing consume request", e);
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
                // Leader: propose offset commit via Raft — blocks until committed to majority
                raftNode.proposeOffsetCommit(group, topic, offset);
            } else {
                // Single-node mode: persist locally
                offsetManager.commit(group, topic, offset);
            }

            logger.debug("Committed offset: group={}, topic={}, offset={}", group, topic, offset);

            CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                    .setSuccess(true)
                    .build();

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.COMMIT_OFFSET_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error committing offset", e);
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

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.FETCH_OFFSET_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error fetching offset", e);
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
        if (raftNode == null) {
            return createErrorResponse("Raft not enabled on this broker");
        }
        RequestVoteRequest request = RequestVoteRequest.parseFrom(envelope.getPayload());
        RequestVoteResponse response = raftNode.handleRequestVote(request);

        return MessageEnvelope.newBuilder()
                .setType(MessageType.REQUEST_VOTE_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    /**
     * Handle an incoming AppendEntries RPC from the Raft leader.
     */
    private MessageEnvelope handleAppendEntriesRequest(MessageEnvelope envelope) throws IOException {
        if (raftNode == null) {
            return createErrorResponse("Raft not enabled on this broker");
        }
        AppendEntriesRequest request = AppendEntriesRequest.parseFrom(envelope.getPayload());
        AppendEntriesResponse response = raftNode.handleAppendEntries(request);

        return MessageEnvelope.newBuilder()
                .setType(MessageType.APPEND_ENTRIES_RESPONSE)
                .setPayload(response.toByteString())
                .build();
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
}
