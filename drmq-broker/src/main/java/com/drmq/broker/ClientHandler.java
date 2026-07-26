package com.drmq.broker;

import com.drmq.broker.raft.RaftNode;
import com.drmq.broker.ClusterEventBuffer;
import com.drmq.protocol.DRMQProtocol.*;
import com.drmq.protocol.DRMQProtocol.ErrorCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;


public class ClientHandler extends SimpleChannelInboundHandler<io.netty.buffer.ByteBuf> {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    private static final int MAX_BATCH_MESSAGES = 10000;
    private static final int MAX_PAYLOAD_BYTES = 10 * 1024 * 1024; // 10MB

    private final MessageStore messageStore;
    private final OffsetManager offsetManager;
    private final RaftNode raftNode;  
    private final ChannelGroup activeChannels;
    private final ConsumerGroupCoordinator groupCoordinator;

    private static final int RPC_THREAD_COUNT = Math.max(4, Runtime.getRuntime().availableProcessors());
    private static final int RPC_QUEUE_CAPACITY = 1000;

    private final ThreadPoolExecutor rpcExecutor;

    public ClientHandler(MessageStore messageStore, OffsetManager offsetManager,
                         RaftNode raftNode, ChannelGroup activeChannels,
                         ConsumerGroupCoordinator groupCoordinator, ThreadPoolExecutor rpcExecutor) {
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.raftNode = raftNode;
        this.activeChannels = activeChannels;
        this.groupCoordinator = groupCoordinator;
        this.rpcExecutor = rpcExecutor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (activeChannels != null) {
            activeChannels.add(ctx.channel());
        }
        BrokerMetrics.get().recordConnectionOpened();
        logger.info("Client connected: {}", ctx.channel().remoteAddress());
        ClusterEventBuffer.emitConnection(String.format("Client connected from %s", ctx.channel().remoteAddress()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        BrokerMetrics.get().recordConnectionClosed();
        logger.info("Client disconnected: {}", ctx.channel().remoteAddress());
        ClusterEventBuffer.emitConnection(String.format("Client disconnected: %s", ctx.channel().remoteAddress()));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, io.netty.buffer.ByteBuf msg) throws Exception {
        java.nio.ByteBuffer nioBuffer = msg.nioBuffer();
        com.google.protobuf.CodedInputStream input = com.google.protobuf.CodedInputStream.newInstance(nioBuffer);
        MessageEnvelope envelope = MessageEnvelope.parseFrom(input);
        
        handleMessage(envelope).thenAccept(response -> {
            int size = response.getSerializedSize();
            io.netty.buffer.ByteBuf outBuf = ctx.alloc().directBuffer(size);
            try {
                com.google.protobuf.CodedOutputStream output = com.google.protobuf.CodedOutputStream.newInstance(outBuf.nioBuffer(0, size));
                response.writeTo(output);
                output.flush();
                outBuf.writerIndex(size);
                ctx.writeAndFlush(outBuf);
            } catch (Exception e) {
                outBuf.release();
                logger.error("Failed to serialize response", e);
            }
        }).exceptionally(e -> {
            logger.error("Unhandled error processing request", e);
            return null;
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof java.net.SocketException || cause instanceof java.io.IOException) {
            logger.debug("Client disconnected with IO error: {}", cause.getMessage());
        } else {
            logger.error("Error handling client {}: {}", ctx.channel().remoteAddress(), cause.getMessage(), cause);
        }
        ctx.close();
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleMessage(MessageEnvelope envelope) throws IOException {
        return switch (envelope.getType()) {
            case PRODUCE_REQUEST -> handleProduceRequest(envelope);
            case PRODUCE_BATCH_REQUEST -> handleProduceBatchRequest(envelope);
            case CONSUME_REQUEST -> java.util.concurrent.CompletableFuture.completedFuture(handleConsumeRequest(envelope));
            case COMMIT_OFFSET_REQUEST -> handleCommitOffsetRequest(envelope);
            case FETCH_OFFSET_REQUEST -> java.util.concurrent.CompletableFuture.completedFuture(handleFetchOffsetRequest(envelope));
            case NACK_REQUEST -> java.util.concurrent.CompletableFuture.completedFuture(handleNackRequest(envelope));
            case REQUEST_VOTE_REQUEST -> handleRequestVoteRequest(envelope);
            case PRE_VOTE_REQUEST -> handlePreVoteRequest(envelope);
            case APPEND_ENTRIES_REQUEST -> handleAppendEntriesRequest(envelope);
            case REQUEST_TOPIC_OFFSETS_REQUEST -> handleRequestTopicOffsetsRequest(envelope);
            case INCREMENTAL_SNAPSHOT_CHUNK -> handleIncrementalSnapshotChunk(envelope);
            case INCREMENTAL_SNAPSHOT_DONE_REQUEST -> handleIncrementalSnapshotDoneRequest(envelope);
            case SEARCH_OFFSET_BY_TIME_REQUEST -> java.util.concurrent.CompletableFuture.completedFuture(handleSearchOffsetByTimeRequest(envelope));
            case ATOMIC_PRODUCE_REQUEST -> handleAtomicProduceRequest(envelope);
            default -> java.util.concurrent.CompletableFuture.completedFuture(createErrorResponse("Unknown message type: " + envelope.getType()));
        };
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleProduceRequest(MessageEnvelope envelope) {
        long startNanos = System.nanoTime();
        try {
            ProduceRequest request = ProduceRequest.parseFrom(envelope.getPayload());

            String topic = request.getTopic();
            byte[] payload = request.getPayload().toByteArray();
            long finalPayloadBytes = payload.length;
            String key = request.hasKey() ? request.getKey() : null;
            long timestamp = request.getTimestamp();

            java.util.concurrent.CompletableFuture<Long> offsetFuture;
            if (raftNode != null) {
                if (!raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    return java.util.concurrent.CompletableFuture.completedFuture(createProduceErrorResponse("NOT_LEADER:" +
                            (leaderAddr != null ? leaderAddr : "UNKNOWN"), ErrorCode.NOT_LEADER));
                }
                offsetFuture = raftNode.proposeAsync(topic, payload, key, timestamp);
            } else {
                offsetFuture = java.util.concurrent.CompletableFuture.completedFuture(messageStore.append(topic, payload, key, timestamp));
            }

            return offsetFuture.thenApply(offset -> {
                logger.debug("Produced message: topic={}, offset={}", topic, offset);

                ProduceResponse response = ProduceResponse.newBuilder()
                        .setSuccess(true)
                        .setOffset(offset)
                        .build();

                BrokerMetrics.get().recordRequest("produce", true,
                    System.nanoTime() - startNanos, finalPayloadBytes, 1);

                return MessageEnvelope.newBuilder()
                        .setType(MessageType.PRODUCE_RESPONSE)
                        .setPayload(response.toByteString())
                        .build();
            }).exceptionally(e -> {
                logger.error("Error processing produce request", e);
                BrokerMetrics.get().recordRequest("produce", false,
                    System.nanoTime() - startNanos, finalPayloadBytes, 1);
                return createProduceErrorResponse(e.getMessage(), ErrorCode.UNKNOWN_ERROR);
            });

        } catch (Exception e) {
            logger.error("Error processing produce request", e);
            BrokerMetrics.get().recordRequest("produce", false,
                System.nanoTime() - startNanos, 0, 1);
            return java.util.concurrent.CompletableFuture.completedFuture(createProduceErrorResponse(e.getMessage(), ErrorCode.UNKNOWN_ERROR));
        }
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleProduceBatchRequest(MessageEnvelope envelope) {
        long startNanos = System.nanoTime();
        long payloadBytes = 0;
        int count = 0;
        try {
            ProduceBatchRequest request = ProduceBatchRequest.parseFrom(envelope.getPayload());

            String topic = request.getTopic();
            count = request.getEntriesCount();
            final int finalBatchCount = count;

            if (finalBatchCount == 0) {
                return java.util.concurrent.CompletableFuture.completedFuture(createProduceBatchErrorResponse("Batch must contain at least one message", ErrorCode.UNKNOWN_ERROR));
            }
            if (finalBatchCount > MAX_BATCH_MESSAGES) {
                return java.util.concurrent.CompletableFuture.completedFuture(createProduceBatchErrorResponse("Batch exceeds maximum message count of " + MAX_BATCH_MESSAGES, ErrorCode.UNKNOWN_ERROR));
            }

            for (var entry : request.getEntriesList()) {
                payloadBytes += entry.getPayload().size();
            }
            final long finalPayloadBytes = payloadBytes;

            if (finalPayloadBytes > MAX_PAYLOAD_BYTES) {
                return java.util.concurrent.CompletableFuture.completedFuture(createProduceBatchErrorResponse("Batch payload exceeds maximum size of " + MAX_PAYLOAD_BYTES + " bytes", ErrorCode.UNKNOWN_ERROR));
            }

            java.util.concurrent.CompletableFuture<Long> offsetFuture;
            if (raftNode != null) {
                if (!raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    return java.util.concurrent.CompletableFuture.completedFuture(createProduceBatchErrorResponse("NOT_LEADER:" +
                            (leaderAddr != null ? leaderAddr : "UNKNOWN"), ErrorCode.NOT_LEADER));
                }
                offsetFuture = raftNode.proposeBatchAsync(topic, request.getEntriesList());
            } else {
                offsetFuture = java.util.concurrent.CompletableFuture.completedFuture(messageStore.appendBatch(topic, request.getEntriesList()));
            }

            return offsetFuture.thenApply(baseOffset -> {
                logger.debug("Produced batch: topic={}, baseOffset={}, count={}", topic, baseOffset, finalBatchCount);

                ProduceBatchResponse response = ProduceBatchResponse.newBuilder()
                        .setSuccess(true)
                        .setBaseOffset(baseOffset)
                        .setCount(finalBatchCount)
                        .build();

                BrokerMetrics.get().recordRequest("produce_batch", true,
                    System.nanoTime() - startNanos, finalPayloadBytes, finalBatchCount);

                return MessageEnvelope.newBuilder()
                        .setType(MessageType.PRODUCE_BATCH_RESPONSE)
                        .setPayload(response.toByteString())
                        .build();
            }).exceptionally(e -> {
                logger.error("Error processing produce batch request", e);
                BrokerMetrics.get().recordRequest("produce_batch", false,
                    System.nanoTime() - startNanos, finalPayloadBytes, finalBatchCount);
                return createProduceBatchErrorResponse(e.getMessage(), ErrorCode.UNKNOWN_ERROR);
            });

        } catch (Exception e) {
            logger.error("Error processing produce batch request", e);
            BrokerMetrics.get().recordRequest("produce_batch", false,
                System.nanoTime() - startNanos, payloadBytes, count);
            return java.util.concurrent.CompletableFuture.completedFuture(createProduceBatchErrorResponse(e.getMessage(), ErrorCode.UNKNOWN_ERROR));
        }
    }

    private MessageEnvelope createProduceBatchErrorResponse(String errorMessage, ErrorCode errorCode) {
        ProduceBatchResponse response = ProduceBatchResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .setErrorCode(errorCode)
                .build();
        return MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_BATCH_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleAtomicProduceRequest(MessageEnvelope envelope) {
        long startNanos = System.nanoTime();
        long payloadBytes = 0;
        int count = 0;
        try {
            com.drmq.protocol.DRMQProtocol.AtomicProduceRequest request = com.drmq.protocol.DRMQProtocol.AtomicProduceRequest.parseFrom(envelope.getPayload());

            for (var slice : request.getSlicesList()) {
                count += slice.getEntriesCount();
                for (var entry : slice.getEntriesList()) {
                    payloadBytes += entry.getPayload().size();
                }
            }
            final int finalBatchCount = count;
            final long finalPayloadBytes = payloadBytes;

            if (finalBatchCount == 0) {
                return java.util.concurrent.CompletableFuture.completedFuture(createAtomicProduceErrorResponse("Atomic batch must contain at least one message", ErrorCode.UNKNOWN_ERROR));
            }
            if (finalPayloadBytes > MAX_PAYLOAD_BYTES) {
                return java.util.concurrent.CompletableFuture.completedFuture(createAtomicProduceErrorResponse("Batch payload exceeds maximum size of " + MAX_PAYLOAD_BYTES + " bytes", ErrorCode.UNKNOWN_ERROR));
            }

            java.util.concurrent.CompletableFuture<java.util.Map<String, Long>> offsetsFuture;
            if (raftNode != null) {
                if (!raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    return java.util.concurrent.CompletableFuture.completedFuture(createAtomicProduceErrorResponse("NOT_LEADER:" +
                            (leaderAddr != null ? leaderAddr : "UNKNOWN"), ErrorCode.NOT_LEADER));
                }
                offsetsFuture = raftNode.proposeAtomicBatchAsync(request.getSlicesList());
            } else {
                offsetsFuture = java.util.concurrent.CompletableFuture.completedFuture(messageStore.appendAtomicBatch(request.getSlicesList()));
            }

            return offsetsFuture.thenApply(offsets -> {
                logger.debug("Produced atomic batch: topics={}, count={}", offsets.keySet(), finalBatchCount);

                com.drmq.protocol.DRMQProtocol.AtomicProduceResponse response = com.drmq.protocol.DRMQProtocol.AtomicProduceResponse.newBuilder()
                        .setSuccess(true)
                        .putAllBaseOffsets(offsets)
                        .build();

                BrokerMetrics.get().recordRequest("atomic_produce", true,
                    System.nanoTime() - startNanos, finalPayloadBytes, finalBatchCount);

                return MessageEnvelope.newBuilder()
                        .setType(MessageType.ATOMIC_PRODUCE_RESPONSE)
                        .setPayload(response.toByteString())
                        .build();
            }).exceptionally(e -> {
                logger.error("Error processing atomic produce request", e);
                BrokerMetrics.get().recordRequest("atomic_produce", false,
                    System.nanoTime() - startNanos, finalPayloadBytes, finalBatchCount);
                return createAtomicProduceErrorResponse(e.getMessage(), ErrorCode.UNKNOWN_ERROR);
            });

        } catch (Exception e) {
            logger.error("Error processing atomic produce request", e);
            BrokerMetrics.get().recordRequest("atomic_produce", false,
                System.nanoTime() - startNanos, payloadBytes, count);
            return java.util.concurrent.CompletableFuture.completedFuture(createAtomicProduceErrorResponse(e.getMessage(), ErrorCode.UNKNOWN_ERROR));
        }
    }

    private MessageEnvelope createAtomicProduceErrorResponse(String errorMessage, ErrorCode errorCode) {
        com.drmq.protocol.DRMQProtocol.AtomicProduceResponse response = com.drmq.protocol.DRMQProtocol.AtomicProduceResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .setErrorCode(errorCode)
                .build();
        return MessageEnvelope.newBuilder()
                .setType(MessageType.ATOMIC_PRODUCE_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    private MessageEnvelope handleConsumeRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        try {
            ConsumeRequest request = ConsumeRequest.parseFrom(envelope.getPayload());

            String topic      = request.getTopic();
            int maxMessages   = request.getMaxMessages();
            long timeoutMs    = request.getTimeoutMs();

            List<StoredMessage> messages;

            // Group-aware consumption: broker coordinates offset dispatch
            if (request.hasConsumerGroup() && request.hasConsumerId() && groupCoordinator != null) {
                if (raftNode != null && !raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    return createConsumeErrorResponse("NOT_LEADER:" +
                            (leaderAddr != null ? leaderAddr : "UNKNOWN"));
                }
                String group = request.getConsumerGroup();
                String consumerId = request.getConsumerId();
                messages = groupCoordinator.acquireMessages(group, topic, consumerId, maxMessages, timeoutMs);
                logger.debug("Group-consume {} messages: group={}, consumer={}, topic={}",
                        messages.size(), group, consumerId, topic);
            } else {
                // Single mode: client-driven offset consumption
                long fromOffset = request.getFromOffset();
                messages = (timeoutMs > 0)
                        ? messageStore.waitForMessages(topic, fromOffset, maxMessages, timeoutMs)
                        : messageStore.getMessages(topic, fromOffset, maxMessages);
                logger.debug("Consumed {} messages: topic={}, fromOffset={}, longPoll={}",
                        messages.size(), topic, fromOffset, timeoutMs > 0);
            }

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

    private MessageEnvelope createProduceErrorResponse(String errorMessage, ErrorCode errorCode) {
        ProduceResponse response = ProduceResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .setErrorCode(errorCode)
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

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

    private MessageEnvelope createCommitOffsetErrorResponse(String errorMessage) {
        CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.COMMIT_OFFSET_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    private MessageEnvelope createFetchOffsetErrorResponse(String errorMessage) {
        FetchOffsetResponse response = FetchOffsetResponse.newBuilder()
                .setSuccess(false)
                .setOffset(-1)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.FETCH_OFFSET_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    private MessageEnvelope handleNackRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        try {
            NackRequest request = NackRequest.parseFrom(envelope.getPayload());

            String group = request.getConsumerGroup();
            String topic = request.getTopic();
            long offset = request.getOffset();
            String consumerId = request.hasConsumerId() ? request.getConsumerId() : group;

            if (groupCoordinator == null) {
                BrokerMetrics.get().recordRequest("nack", false,
                        System.nanoTime() - startNanos, 0, 0);
                return createNackErrorResponse("Consumer group coordination not available");
            }

            if (raftNode != null && !raftNode.isLeader()) {
                String leaderAddr = raftNode.getLeaderAddress();
                BrokerMetrics.get().recordRequest("nack", false,
                        System.nanoTime() - startNanos, 0, 0);
                return createNackErrorResponse("NOT_LEADER:" +
                        (leaderAddr != null ? leaderAddr : "UNKNOWN"));
            }

            boolean routedToDlq = groupCoordinator.nackOffset(group, topic, consumerId, offset);

            logger.info("NACK processed: group={}, topic={}, offset={}, routedToDlq={}",
                    group, topic, offset, routedToDlq);

            NackResponse response = NackResponse.newBuilder()
                    .setSuccess(true)
                    .setRoutedToDlq(routedToDlq)
                    .build();

            BrokerMetrics.get().recordRequest("nack", true,
                    System.nanoTime() - startNanos, 0, 0);

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.NACK_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing NACK request", e);
            BrokerMetrics.get().recordRequest("nack", false,
                    System.nanoTime() - startNanos, 0, 0);
            return createNackErrorResponse(e.getMessage());
        }
    }

    private MessageEnvelope createNackErrorResponse(String errorMessage) {
        NackResponse response = NackResponse.newBuilder()
                .setSuccess(false)
                .setRoutedToDlq(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.NACK_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleCommitOffsetRequest(MessageEnvelope envelope) {
        long startNanos = System.nanoTime();
        try {
            CommitOffsetRequest request = CommitOffsetRequest.parseFrom(envelope.getPayload());

            String group = request.getConsumerGroup();
            String topic = request.getTopic();
            long offset  = request.getOffset();

            java.util.concurrent.CompletableFuture<Void> commitFuture = new java.util.concurrent.CompletableFuture<>();

            // Route through the coordinator if this group is actively coordinated
            if (groupCoordinator != null && groupCoordinator.isGroupActive(group, topic)) {
                String consumerId = request.hasConsumerId() ? request.getConsumerId() : group;
                groupCoordinator.commitOffset(group, topic, consumerId, offset);
                commitFuture.complete(null);
            } else if (raftNode != null) {
                if (!raftNode.isLeader()) {
                    String leaderAddr = raftNode.getLeaderAddress();
                    CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("NOT_LEADER:" +
                                    (leaderAddr != null ? leaderAddr : "UNKNOWN"))
                            .build();
                    return java.util.concurrent.CompletableFuture.completedFuture(MessageEnvelope.newBuilder()
                            .setType(MessageType.COMMIT_OFFSET_RESPONSE)
                            .setPayload(response.toByteString())
                            .build());
                }
                commitFuture = raftNode.proposeOffsetCommitAsync(group, topic, offset).thenApply(idx -> null);
            } else {
                offsetManager.commit(group, topic, offset);
                commitFuture.complete(null);
            }

            return commitFuture.thenApply(v -> {
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
            }).exceptionally(e -> {
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
            });

        } catch (Exception e) {
            logger.error("Error committing offset", e);
            BrokerMetrics.get().recordRequest("commit_offset", false,
                System.nanoTime() - startNanos, 0, 0);
            CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "Unknown error")
                    .build();
            return java.util.concurrent.CompletableFuture.completedFuture(MessageEnvelope.newBuilder()
                    .setType(MessageType.COMMIT_OFFSET_RESPONSE)
                    .setPayload(response.toByteString())
                    .build());
        }
    }

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

    private MessageEnvelope handleSearchOffsetByTimeRequest(MessageEnvelope envelope) throws IOException {
        long startNanos = System.nanoTime();
        try {
            SearchOffsetByTimeRequest request = SearchOffsetByTimeRequest.parseFrom(envelope.getPayload());
            long offset = messageStore.findOffsetByTimestamp(request.getTopic(), request.getTimestamp());
            
            SearchOffsetByTimeResponse response = SearchOffsetByTimeResponse.newBuilder()
                    .setSuccess(true)
                    .setOffset(offset)
                    .build();
                    
            BrokerMetrics.get().recordRequest("search_offset_by_time", true,
                System.nanoTime() - startNanos, envelope.getSerializedSize(), response.getSerializedSize());
                    
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.SEARCH_OFFSET_BY_TIME_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        } catch (Exception e) {
            logger.error("Error handling SEARCH_OFFSET_BY_TIME request", e);
            BrokerMetrics.get().recordRequest("search_offset_by_time", false,
                System.nanoTime() - startNanos, envelope.getSerializedSize(), 0);
            
            SearchOffsetByTimeResponse response = SearchOffsetByTimeResponse.newBuilder()
                    .setSuccess(false)
                    .setOffset(-1)
                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "Unknown error")
                    .build();
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.SEARCH_OFFSET_BY_TIME_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        }
    }

    @Deprecated
    private MessageEnvelope createErrorResponse(String errorMessage) {
        return createErrorResponse(errorMessage, MessageType.PRODUCE_RESPONSE);
    }

    private MessageEnvelope createErrorResponse(String errorMessage, MessageType messageType) {
        return switch (messageType) {
            case REQUEST_VOTE_RESPONSE -> createRequestVoteErrorResponse();
            case PRE_VOTE_RESPONSE -> createPreVoteErrorResponse();
            case APPEND_ENTRIES_RESPONSE -> createAppendEntriesErrorResponse();
            case PRODUCE_RESPONSE -> createProduceErrorResponse(errorMessage, ErrorCode.UNKNOWN_ERROR);
            case CONSUME_RESPONSE -> createConsumeErrorResponse(errorMessage);
            case COMMIT_OFFSET_RESPONSE -> createCommitOffsetErrorResponse(errorMessage);
            case FETCH_OFFSET_RESPONSE -> createFetchOffsetErrorResponse(errorMessage);
            case NACK_RESPONSE -> createNackErrorResponse(errorMessage);
            case ATOMIC_PRODUCE_RESPONSE -> createAtomicProduceErrorResponse(errorMessage, ErrorCode.UNKNOWN_ERROR);
            default -> createProduceErrorResponse(errorMessage, ErrorCode.UNKNOWN_ERROR);
        };
    }

    private MessageEnvelope createRequestVoteErrorResponse() {
        RequestVoteResponse response = RequestVoteResponse.newBuilder()
                .setTerm(0)
                .setVoteGranted(false)
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.REQUEST_VOTE_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    private MessageEnvelope createAppendEntriesErrorResponse() {
        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                .setTerm(0)
                .setSuccess(false)
                .setMatchIndex(0)
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.APPEND_ENTRIES_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }



    private MessageEnvelope createPreVoteErrorResponse() {
        PreVoteResponse response = PreVoteResponse.newBuilder()
                .setTerm(0)
                .setVoteGranted(false)
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.PRE_VOTE_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleRequestVoteRequest(MessageEnvelope envelope) {
        long startNanos = System.nanoTime();
        if (raftNode == null) {
            BrokerMetrics.get().recordRaftRpc("request_vote", false,
                    System.nanoTime() - startNanos);
            return java.util.concurrent.CompletableFuture.completedFuture(createRequestVoteErrorResponse());
        }
        
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                RequestVoteRequest request = RequestVoteRequest.parseFrom(envelope.getPayload());
                RequestVoteResponse response = raftNode.handleRequestVote(request);

                BrokerMetrics.get().recordRaftRpc("request_vote", true,
                        System.nanoTime() - startNanos);

                return MessageEnvelope.newBuilder()
                        .setType(MessageType.REQUEST_VOTE_RESPONSE)
                        .setPayload(response.toByteString())
                        .build();
            } catch (Exception e) {
                logger.error("RequestVote handler failed: {}", e.getMessage());
                BrokerMetrics.get().recordRaftRpc("request_vote", false,
                        System.nanoTime() - startNanos);
                return createRequestVoteErrorResponse();
            }
        }, rpcExecutor);
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handlePreVoteRequest(MessageEnvelope envelope) {
        long startNanos = System.nanoTime();
        if (raftNode == null) {
            BrokerMetrics.get().recordRaftRpc("pre_vote", false,
                    System.nanoTime() - startNanos);
            return java.util.concurrent.CompletableFuture.completedFuture(createPreVoteErrorResponse());
        }
        
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                PreVoteRequest request = PreVoteRequest.parseFrom(envelope.getPayload());
                PreVoteResponse response = raftNode.handlePreVote(request);

                BrokerMetrics.get().recordRaftRpc("pre_vote", true,
                        System.nanoTime() - startNanos);

                return MessageEnvelope.newBuilder()
                        .setType(MessageType.PRE_VOTE_RESPONSE)
                        .setPayload(response.toByteString())
                        .build();
            } catch (Exception e) {
                logger.error("PreVote handler failed: {}", e.getMessage());
                BrokerMetrics.get().recordRaftRpc("pre_vote", false,
                        System.nanoTime() - startNanos);
                return createPreVoteErrorResponse();
            }
        }, rpcExecutor);
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleAppendEntriesRequest(MessageEnvelope envelope) {
        long startNanos = System.nanoTime();
        if (raftNode == null) {
            BrokerMetrics.get().recordRaftRpc("append_entries", false,
                    System.nanoTime() - startNanos);
            return java.util.concurrent.CompletableFuture.completedFuture(createAppendEntriesErrorResponse());
        }

        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                AppendEntriesRequest request = AppendEntriesRequest.parseFrom(envelope.getPayload());
                AppendEntriesResponse response = raftNode.handleAppendEntries(request);

                BrokerMetrics.get().recordRaftRpc("append_entries", response.getSuccess(),
                        System.nanoTime() - startNanos);

                return MessageEnvelope.newBuilder()
                        .setType(MessageType.APPEND_ENTRIES_RESPONSE)
                        .setPayload(response.toByteString())
                        .build();
            } catch (Exception e) {
                logger.error("AppendEntries handler failed: {}", e.getMessage());
                BrokerMetrics.get().recordRaftRpc("append_entries", false,
                        System.nanoTime() - startNanos);
                return createAppendEntriesErrorResponse();
            }
        }, rpcExecutor);
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleRequestTopicOffsetsRequest(MessageEnvelope envelope) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                RequestTopicOffsetsRequest request = RequestTopicOffsetsRequest.parseFrom(envelope.getPayload());
                java.util.Map<String, Long> offsets = messageStore.getTopicMaxOffsets();
                
                logger.info("Follower generating topic offsets for Tier 2 Incremental Sync ({} topics found)", offsets.size());
                
                RequestTopicOffsetsResponse response = RequestTopicOffsetsResponse.newBuilder()
                        .setTerm(raftNode != null ? raftNode.getCurrentTerm() : 0)
                        .putAllTopicOffsets(offsets)
                        .build();
                        
                return MessageEnvelope.newBuilder()
                        .setType(MessageType.REQUEST_TOPIC_OFFSETS_RESPONSE)
                        .setPayload(response.toByteString())
                        .build();
            } catch (Exception e) {
                logger.error("Error handling request topic offsets", e);
                return createErrorResponse(e.getMessage());
            }
        }, rpcExecutor);
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleIncrementalSnapshotChunk(MessageEnvelope envelope) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            boolean success = false;
            try {
                IncrementalSnapshotChunk request = IncrementalSnapshotChunk.parseFrom(envelope.getPayload());
                if (raftNode != null) {
                    success = raftNode.handleIncrementalSnapshotChunk(request).getSuccess();
                }
            } catch (Exception e) {
                logger.error("Error handling chunk", e);
            }
            IncrementalSnapshotChunkResponse response = IncrementalSnapshotChunkResponse.newBuilder()
                    .setTerm(raftNode != null ? raftNode.getCurrentTerm() : 0)
                    .setSuccess(success)
                    .build();
                    
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.INCREMENTAL_SNAPSHOT_CHUNK_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        }, rpcExecutor);
    }

    private java.util.concurrent.CompletableFuture<MessageEnvelope> handleIncrementalSnapshotDoneRequest(MessageEnvelope envelope) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            boolean success = false;
            try {
                IncrementalSnapshotDoneRequest request = IncrementalSnapshotDoneRequest.parseFrom(envelope.getPayload());
                if (raftNode != null) {
                    success = raftNode.handleIncrementalSnapshotDone(request).getSuccess();
                }
            } catch (Exception e) {
                logger.error("Error handling snapshot done", e);
            }
            IncrementalSnapshotDoneResponse response = IncrementalSnapshotDoneResponse.newBuilder()
                    .setTerm(raftNode != null ? raftNode.getCurrentTerm() : 0)
                    .setSuccess(success)
                    .build();
                    
            return MessageEnvelope.newBuilder()
                    .setType(MessageType.INCREMENTAL_SNAPSHOT_DONE_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();
        }, rpcExecutor);
    }

    private static long estimatePayloadBytes(java.util.List<StoredMessage> messages) {
        long total = 0;
        for (StoredMessage message : messages) {
            total += message.getPayload().size();
        }
        return total;
    }
}
