package com.drmq.broker.raft;

import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerMetrics;
import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * Manages a TCP connection to a single remote Raft peer.
 */
public class RaftPeer {
    private static final Logger logger = LoggerFactory.getLogger(RaftPeer.class);
    private static final int CONNECT_TIMEOUT_MS = 1000;
    private static final int READ_TIMEOUT_MS = 2000;
    private static final long LOG_RATE_LIMIT_MS = 1000;  
    private final PeerAddress address;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final Object lock = new Object();
    private long lastRequestVoteFailureLogTime = 0;
    private long lastPreVoteFailureLogTime = 0;
    private long lastAppendEntriesFailureLogTime = 0;

    public RaftPeer(PeerAddress address) {
        this.address = address;
    }

    /**
     * Ensure we have an active connection, reconnecting if needed.
     */
    private void ensureConnected() throws IOException {
        if (socket != null && !socket.isClosed() && socket.isConnected()) {
            return;
        }

        close(); // Clean up any partial state
        socket = new Socket();
        socket.connect(new java.net.InetSocketAddress(address.host(), address.port()), CONNECT_TIMEOUT_MS);
        socket.setSoTimeout(READ_TIMEOUT_MS);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    /**
     * Send a RequestVote RPC and wait for the response.
     */
    public RequestVoteResponse sendRequestVote(RequestVoteRequest request) {
        synchronized (lock) {
            long startNanos = System.nanoTime();
            try {
                ensureConnected();

                MessageEnvelope envelope = MessageEnvelope.newBuilder()
                        .setType(MessageType.REQUEST_VOTE_REQUEST)
                        .setPayload(request.toByteString())
                        .build();

                sendEnvelope(envelope);
                MessageEnvelope response = receiveEnvelope();
                requireResponseType(response, MessageType.REQUEST_VOTE_RESPONSE, "RequestVote");
                RequestVoteResponse parsed = RequestVoteResponse.parseFrom(response.getPayload());
                BrokerMetrics.get().recordRaftRpc("request_vote", true,
                        System.nanoTime() - startNanos);
                return parsed;

            } catch (Exception e) {
                close();
                long now = System.currentTimeMillis();
                if ((now - lastRequestVoteFailureLogTime) >= LOG_RATE_LIMIT_MS) {
                    logger.debug("RequestVote to {} failed: {}", address, e.getMessage());
                    lastRequestVoteFailureLogTime = now;
                }
                BrokerMetrics.get().recordRaftRpc("request_vote", false,
                        System.nanoTime() - startNanos);
                throw new RuntimeException("RequestVote RPC failed", e);
            }
        }
    }

    /**
     * Send a PreVote RPC and wait for the response.
     * PreVote checks if this candidate could win an election without incrementing the term.
     */
    public PreVoteResponse sendPreVote(PreVoteRequest request) {
        synchronized (lock) {
            long startNanos = System.nanoTime();
            try {
                ensureConnected();

                MessageEnvelope envelope = MessageEnvelope.newBuilder()
                        .setType(MessageType.PRE_VOTE_REQUEST)
                        .setPayload(request.toByteString())
                        .build();

                sendEnvelope(envelope);
                MessageEnvelope response = receiveEnvelope();
                requireResponseType(response, MessageType.PRE_VOTE_RESPONSE, "PreVote");
                PreVoteResponse parsed = PreVoteResponse.parseFrom(response.getPayload());
                BrokerMetrics.get().recordRaftRpc("pre_vote", true,
                        System.nanoTime() - startNanos);
                return parsed;

            } catch (Exception e) {
                close();
                long now = System.currentTimeMillis();
                if ((now - lastPreVoteFailureLogTime) >= LOG_RATE_LIMIT_MS) {
                    logger.debug("PreVote to {} failed: {}", address, e.getMessage());
                    lastPreVoteFailureLogTime = now;
                }
                BrokerMetrics.get().recordRaftRpc("pre_vote", false,
                        System.nanoTime() - startNanos);
                throw new RuntimeException("PreVote RPC failed", e);
            }
        }
    }

    /**
     * Send an AppendEntries RPC and wait for the response.
     */
    public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
        synchronized (lock) {
            long startNanos = System.nanoTime();
            try {
                ensureConnected();

                MessageEnvelope envelope = MessageEnvelope.newBuilder()
                        .setType(MessageType.APPEND_ENTRIES_REQUEST)
                        .setPayload(request.toByteString())
                        .build();

                sendEnvelope(envelope);
                MessageEnvelope response = receiveEnvelope();
                requireResponseType(response, MessageType.APPEND_ENTRIES_RESPONSE, "AppendEntries");
                AppendEntriesResponse parsed = AppendEntriesResponse.parseFrom(response.getPayload());
                BrokerMetrics.get().recordRaftRpc("append_entries", true,
                    System.nanoTime() - startNanos);
                return parsed;

            } catch (Exception e) {
                close();
                long now = System.currentTimeMillis();
                if ((now - lastAppendEntriesFailureLogTime) >= LOG_RATE_LIMIT_MS) {
                    logger.debug("AppendEntries to {} failed: {}", address, e.getMessage());
                    lastAppendEntriesFailureLogTime = now;
                }
                BrokerMetrics.get().recordRaftRpc("append_entries", false,
                        System.nanoTime() - startNanos);
                throw new RuntimeException("AppendEntries RPC failed", e);
            }
        }
    }

    /**
     * Send a RequestTopicOffsets RPC and wait for the response.
     */
    public RequestTopicOffsetsResponse sendRequestTopicOffsets(RequestTopicOffsetsRequest request) {
        synchronized (lock) {
            long startNanos = System.nanoTime();
            try {
                ensureConnected();
                socket.setSoTimeout(10000); // 10 seconds for offset resolution

                MessageEnvelope envelope = MessageEnvelope.newBuilder()
                        .setType(MessageType.REQUEST_TOPIC_OFFSETS_REQUEST)
                        .setPayload(request.toByteString())
                        .build();

                sendEnvelope(envelope);
                MessageEnvelope response = receiveEnvelope();
                
                socket.setSoTimeout(READ_TIMEOUT_MS);
                requireResponseType(response, MessageType.REQUEST_TOPIC_OFFSETS_RESPONSE, "RequestTopicOffsets");
                RequestTopicOffsetsResponse parsed = RequestTopicOffsetsResponse.parseFrom(response.getPayload());
                BrokerMetrics.get().recordRaftRpc("request_topic_offsets", true,
                    System.nanoTime() - startNanos);
                return parsed;

            } catch (Exception e) {
                close();
                logger.debug("RequestTopicOffsets to {} failed: {}", address, e.getMessage());
                BrokerMetrics.get().recordRaftRpc("request_topic_offsets", false,
                        System.nanoTime() - startNanos);
                throw new RuntimeException("RequestTopicOffsets RPC failed", e);
            }
        }
    }

    /**
     * Send an IncrementalSnapshotChunk RPC and wait for the response.
     */
    public IncrementalSnapshotChunkResponse sendIncrementalSnapshotChunk(IncrementalSnapshotChunk request) {
        synchronized (lock) {
            long startNanos = System.nanoTime();
            try {
                ensureConnected();
                socket.setSoTimeout(30000); // 30 seconds to allow for chunk flush (mappedBuffer.force())

                MessageEnvelope envelope = MessageEnvelope.newBuilder()
                        .setType(MessageType.INCREMENTAL_SNAPSHOT_CHUNK)
                        .setPayload(request.toByteString())
                        .build();

                sendEnvelope(envelope);
                MessageEnvelope response = receiveEnvelope();
                
                socket.setSoTimeout(READ_TIMEOUT_MS);
                requireResponseType(response, MessageType.INCREMENTAL_SNAPSHOT_CHUNK_RESPONSE, "IncrementalSnapshotChunk");
                IncrementalSnapshotChunkResponse parsed = IncrementalSnapshotChunkResponse.parseFrom(response.getPayload());
                BrokerMetrics.get().recordRaftRpc("incremental_snapshot_chunk", true,
                    System.nanoTime() - startNanos);
                return parsed;

            } catch (Exception e) {
                close();
                logger.debug("IncrementalSnapshotChunk to {} failed: {}", address, e.getMessage());
                BrokerMetrics.get().recordRaftRpc("incremental_snapshot_chunk", false,
                        System.nanoTime() - startNanos);
                throw new RuntimeException("IncrementalSnapshotChunk RPC failed", e);
            }
        }
    }

    /**
     * Send an IncrementalSnapshotDoneRequest RPC and wait for the response.
     */
    public IncrementalSnapshotDoneResponse sendIncrementalSnapshotDone(IncrementalSnapshotDoneRequest request) {
        synchronized (lock) {
            long startNanos = System.nanoTime();
            try {
                ensureConnected();
                socket.setSoTimeout(300000); // 5 minutes to allow for large MessageStore reload

                MessageEnvelope envelope = MessageEnvelope.newBuilder()
                        .setType(MessageType.INCREMENTAL_SNAPSHOT_DONE_REQUEST)
                        .setPayload(request.toByteString())
                        .build();

                sendEnvelope(envelope);
                MessageEnvelope response = receiveEnvelope();
                
                socket.setSoTimeout(READ_TIMEOUT_MS);
                
                requireResponseType(response, MessageType.INCREMENTAL_SNAPSHOT_DONE_RESPONSE, "IncrementalSnapshotDoneRequest");
                IncrementalSnapshotDoneResponse parsed = IncrementalSnapshotDoneResponse.parseFrom(response.getPayload());
                BrokerMetrics.get().recordRaftRpc("incremental_snapshot_done", true,
                    System.nanoTime() - startNanos);
                return parsed;

            } catch (Exception e) {
                close();
                logger.debug("IncrementalSnapshotDone to {} failed: {}", address, e.getMessage());
                BrokerMetrics.get().recordRaftRpc("incremental_snapshot_done", false,
                        System.nanoTime() - startNanos);
                throw new RuntimeException("IncrementalSnapshotDone RPC failed", e);
            }
        }
    }

    /**
     * Send a length-prefixed envelope.
     */
    private void sendEnvelope(MessageEnvelope envelope) throws IOException {
        byte[] data = envelope.toByteArray();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    /**
     * Receive a length-prefixed envelope.
     */
    private MessageEnvelope receiveEnvelope() throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > 10 * 1024 * 1024) {
            throw new IOException("Invalid response length: " + length);
        }
        byte[] data = new byte[length];
        in.readFully(data);
        return MessageEnvelope.parseFrom(data);
    }

    private static void requireResponseType(MessageEnvelope response, MessageType expectedType,
                                            String rpcName) throws IOException {
        if (response.getType() != expectedType) {
            throw new IOException(rpcName + " expected " + expectedType + " but received " + response.getType());
        }
    }

    /**
     * Close the connection to the peer.
     */
    public void close() {
        try {
            if (in != null) in.close();
        } catch (IOException ignored) {}
        try {
            if (out != null) out.close();
        } catch (IOException ignored) {}
        try {
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException ignored) {}
        socket = null;
        in = null;
        out = null;
    }

    public PeerAddress getAddress() { return address; }
}



