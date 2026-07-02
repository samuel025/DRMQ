package com.drmq.broker;

import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.drmq.broker.raft.RaftNode;

/**
 * Coordinates message consumption across multiple consumers within a consumer group.
 *
 * <h3>Design: Broker-Side Offset Leasing</h3>

 *
 * <h3>Key Concepts</h3>
 * <ul>
 *   <li><b>dispatchOffset</b> — the next offset to hand out; always ≥ committedOffset</li>
 *   <li><b>committedOffset</b> — the highest contiguously committed offset (persisted for crash recovery)</li>
 *   <li><b>Lease</b> — a range of offsets assigned to a specific consumer, with an expiry time</li>
 * </ul>
 */
public class ConsumerGroupCoordinator implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupCoordinator.class);

    static final long DEFAULT_LEASE_TIMEOUT_MS = 30_000;

    private static final long LEASE_CHECK_INTERVAL_MS = 5_000;

    private final MessageStore messageStore;
    private final OffsetManager offsetManager;
    private final RaftNode raftNode;
    private final long leaseTimeoutMs;
    private final int maxDeliveries;
    private final String dlqTopicPrefix;

    /** group/topic → GroupTopicState */
    private final ConcurrentHashMap<String, GroupTopicState> groupStates = new ConcurrentHashMap<>();

    private final ScheduledExecutorService leaseReaper;

    public ConsumerGroupCoordinator(MessageStore messageStore, OffsetManager offsetManager) {
        this(messageStore, offsetManager, null, DEFAULT_LEASE_TIMEOUT_MS, 5, "dlq.");
    }

    public ConsumerGroupCoordinator(MessageStore messageStore, OffsetManager offsetManager, long leaseTimeoutMs) {
        this(messageStore, offsetManager, null, leaseTimeoutMs, 5, "dlq.");
    }

    public ConsumerGroupCoordinator(MessageStore messageStore, OffsetManager offsetManager,
                                     RaftNode raftNode, long leaseTimeoutMs) {
        this(messageStore, offsetManager, raftNode, leaseTimeoutMs, 5, "dlq.");
    }

    public ConsumerGroupCoordinator(MessageStore messageStore, OffsetManager offsetManager,
                                     RaftNode raftNode, long leaseTimeoutMs,
                                     int maxDeliveries, String dlqTopicPrefix) {
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.raftNode = raftNode;
        this.leaseTimeoutMs = leaseTimeoutMs;
        this.maxDeliveries = maxDeliveries > 0 ? maxDeliveries : 5;
        this.dlqTopicPrefix = dlqTopicPrefix != null ? dlqTopicPrefix : "dlq.";

        this.leaseReaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lease-reaper");
            t.setDaemon(true);
            return t;
        });
        this.leaseReaper.scheduleAtFixedRate(
                this::expireLeases, LEASE_CHECK_INTERVAL_MS, LEASE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Acquire a batch of messages for a consumer within a group.
     * The broker assigns the next available offset range and creates a lease.
     *
     * @param group      the consumer group name
     * @param topic      the topic to consume from
     * @param consumerId the unique consumer instance ID
     * @param maxMessages maximum messages to return
     * @param timeoutMs  long-poll timeout (0 = return immediately)
     * @return the messages assigned to this consumer (may be empty)
     */
    public List<StoredMessage> acquireMessages(String group, String topic, String consumerId, int maxMessages, long timeoutMs) {
        String key = groupKey(group, topic);
        GroupTopicState state = groupStates.computeIfAbsent(key, k -> {
            long committed = offsetManager.fetch(group, topic);
            long startOffset = committed >= 0 ? committed : 0;
            logger.info("Initializing group state: group={}, topic={}, startOffset={}", group, topic, startOffset);
            return new GroupTopicState(startOffset);
        });

        long fromOffset;
        state.lock.lock();
        try {
            fromOffset = state.dispatchOffset;
        } finally {
            state.lock.unlock();
        }
        List<StoredMessage> messages = (timeoutMs > 0)
                ? messageStore.waitForMessages(topic, fromOffset, maxMessages, timeoutMs)
                : messageStore.getMessages(topic, fromOffset, maxMessages);

        if (messages.isEmpty()) {
            return messages;
        }

        state.lock.lock();
        try {
            if (state.dispatchOffset > fromOffset) {
                return Collections.emptyList();
            }
            state.members.add(consumerId);
            long lastOffset = messages.get(messages.size() - 1).getOffset();
            long leaseEnd = lastOffset + 1; 
            
            Lease existingLease = state.activeLeases.get(consumerId);
            long leaseStart = (existingLease != null) ? existingLease.fromOffset : fromOffset;
            
            Lease lease = new Lease(consumerId, leaseStart, leaseEnd,
                    System.currentTimeMillis() + leaseTimeoutMs);
            state.activeLeases.put(consumerId, lease);
            state.dispatchOffset = leaseEnd;

            logger.debug("Leased offsets [{}, {}) to consumer {} in group={}, topic={}",
                    leaseStart, leaseEnd, consumerId, group, topic);

            return messages;

        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Commit an offset for a consumer within a group.
     * Clears the consumer's active lease and advances the group's committed offset
     * if all prior leases have also been committed.
     *
     * @param group      the consumer group name
     * @param topic      the topic
     * @param consumerId the consumer instance ID
     * @param offset     the next offset to read (last processed + 1)
     */
    public void commitOffset(String group, String topic, String consumerId, long offset) {
        String key = groupKey(group, topic);
        GroupTopicState state = groupStates.get(key);

        if (state == null) {
            offsetManager.commit(group, topic, offset);
            return;
        }

        state.lock.lock();
        try {
            Lease lease = state.activeLeases.remove(consumerId);

            if (lease != null) {
                state.committedRanges.add(new CommittedRange(lease.fromOffset, offset));
                state.members.remove(consumerId);
                logger.debug("Consumer {} committed offset {} for group={}, topic={} (lease [{}, {}))",
                        consumerId, offset, group, topic, lease.fromOffset, lease.toOffset);
            }

            advanceCommittedOffset(state, group, topic);

        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Advance the group's committed offset by merging contiguous committed ranges.
     * Only commits to the OffsetManager when the offset actually advances.
     */
    private void advanceCommittedOffset(GroupTopicState state, String group, String topic) {
        state.committedRanges.sort(Comparator.comparingLong(r -> r.fromOffset));

        long current = state.committedOffset;
        Iterator<CommittedRange> it = state.committedRanges.iterator();

        while (it.hasNext()) {
            CommittedRange range = it.next();
            if (range.fromOffset <= current) {
                if (range.toOffset > current) {
                    current = range.toOffset;
                }
                it.remove(); 
            } else {
                break;
            }
        }

        if (current > state.committedOffset) {
            state.committedOffset = current;
            offsetManager.commit(group, topic, current);
            if (raftNode != null && raftNode.isLeader()) {
                try {
                    raftNode.proposeOffsetCommit(group, topic, current);
                } catch (Exception e) {
                    logger.warn("Failed to replicate committed offset via Raft: group={}, topic={}, offset={}: {}",
                            group, topic, current, e.getMessage());
                }
            }

            logger.debug("Advanced committed offset to {} for group={}, topic={}", current, group, topic);
        }
    }

    /**
     * Periodic task: expire leases that have not been committed in time.
     * Increments delivery counts and routes to DLQ if max deliveries exceeded,
     * otherwise rewinds the dispatch offset so the expired range can be re-dispatched.
     */
    void expireLeases() {
        long now = System.currentTimeMillis();

        for (Map.Entry<String, GroupTopicState> entry : groupStates.entrySet()) {
            GroupTopicState state = entry.getValue();
            String[] parts = parseGroupKey(entry.getKey());
            String group = parts[0];
            String topic = parts[1];

            state.lock.lock();
            try {
                Iterator<Map.Entry<String, Lease>> it = state.activeLeases.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, Lease> leaseEntry = it.next();
                    Lease lease = leaseEntry.getValue();

                    if (now >= lease.expiresAt) {
                        long badOffset = lease.fromOffset;
                        int attempts = state.deliveryCounts.merge(badOffset, 1, Integer::sum);

                        logger.warn("Lease expired for consumer {} in {}: offsets [{}, {}). Attempt {} of {}.",
                                leaseEntry.getKey(), entry.getKey(), lease.fromOffset, lease.toOffset,
                                attempts, maxDeliveries);

                        if (attempts >= maxDeliveries) {
                            routeToDlq(state, group, topic, badOffset);
                        } else {
                            if (lease.fromOffset < state.dispatchOffset) {
                                state.dispatchOffset = lease.fromOffset;
                            }
                        }
                        state.members.remove(leaseEntry.getKey());
                        it.remove();
                    }
                }
            } finally {
                state.lock.unlock();
            }
        }
    }


    public boolean isGroupActive(String group, String topic) {
        String key = groupKey(group, topic);
        GroupTopicState state = groupStates.get(key);
        return state != null && !state.members.isEmpty();
    }

    /**
     * Get the number of active consumers in a group for a specific topic.
     */
    public int getConsumerCount(String group, String topic) {
        GroupTopicState state = groupStates.get(groupKey(group, topic));
        return state != null ? state.members.size() : 0;
    }

    /**
     * Get the number of active leases across all groups.
     */
    public int getActiveLeasesCount() {
        int total = 0;
        for (GroupTopicState state : groupStates.values()) {
            total += state.activeLeases.size();
        }
        return total;
    }

    // ---- Dead-Letter Queue (DLQ) Support ----

    /**
     * Explicitly reject (NACK) a message offset for a consumer within a group.
     * Increments the delivery count for the offset. If the count exceeds
     * {@code maxDeliveries}, the message is routed to the DLQ topic and the
     * consumer group advances past it. Otherwise, the offset is rewound for
     * immediate redelivery.
     *
     * @param group      the consumer group name
     * @param topic      the topic
     * @param consumerId the consumer instance ID
     * @param offset     the offset of the message being rejected
     * @return true if the message was routed to the DLQ, false if it was requeued
     */
    public boolean nackOffset(String group, String topic, String consumerId, long offset) {
        String key = groupKey(group, topic);
        GroupTopicState state = groupStates.get(key);

        if (state == null) {
            logger.warn("NACK for unknown group state: group={}, topic={}, offset={}", group, topic, offset);
            return false;
        }

        state.lock.lock();
        try {
            // Remove the consumer's active lease
            Lease lease = state.activeLeases.remove(consumerId);
            if (lease != null) {
                state.members.remove(consumerId);
            }

            int attempts = state.deliveryCounts.merge(offset, 1, Integer::sum);
            logger.info("NACK received: group={}, topic={}, consumer={}, offset={}, attempt {} of {}",
                    group, topic, consumerId, offset, attempts, maxDeliveries);

            if (attempts >= maxDeliveries) {
                routeToDlq(state, group, topic, offset);
                return true;
            } else {
                // Rewind for redelivery
                if (offset < state.dispatchOffset) {
                    state.dispatchOffset = offset;
                }
                return false;
            }
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Route a poison message to the Dead-Letter Queue topic.
     * Fetches the original message, produces it to the DLQ topic, then
     * advances the consumer group past the bad offset.
     *
     * Must be called while holding state.lock.
     */
    private void routeToDlq(GroupTopicState state, String group, String topic, long badOffset) {
        String dlqTopic = dlqTopicPrefix + group + "." + topic;

        try {
            List<StoredMessage> messages = messageStore.getMessages(topic, badOffset, 1);
            if (!messages.isEmpty()) {
                StoredMessage original = messages.get(0);
                byte[] payload = original.getPayload().toByteArray();
                String key = original.hasKey() ? original.getKey() : null;

                if (raftNode != null && raftNode.isLeader()) {
                    raftNode.propose(dlqTopic, payload, key, original.getTimestamp());
                } else {
                    messageStore.append(dlqTopic, payload, key, original.getTimestamp());
                }

                logger.warn("Routed poison message to DLQ: offset={} from topic={} -> {}",
                        badOffset, topic, dlqTopic);
            } else {
                logger.error("Failed to fetch poison message for DLQ routing: topic={}, offset={}",
                        topic, badOffset);
            }
        } catch (IOException e) {
            logger.error("Failed to route message to DLQ topic {}: {}", dlqTopic, e.getMessage(), e);
        }

        // Advance past the bad offset regardless — don't let a DLQ write failure block progress
        long nextOffset = badOffset + 1;
        state.committedRanges.add(new CommittedRange(badOffset, nextOffset));
        advanceCommittedOffset(state, group, topic);
        if (state.dispatchOffset <= badOffset) {
            state.dispatchOffset = nextOffset;
        }

        // Clean up the delivery counter for this offset
        state.deliveryCounts.remove(badOffset);
    }

    /**
     * Build the DLQ topic name for a given group and topic.
     */
    public String getDlqTopicName(String group, String topic) {
        return dlqTopicPrefix + group + "." + topic;
    }

    /** Parse a "group/topic" key back into its components. */
    private static String[] parseGroupKey(String key) {
        int idx = key.indexOf('/');
        return new String[]{ key.substring(0, idx), key.substring(idx + 1) };
    }

    @Override
    public void close() {
        leaseReaper.shutdownNow();
        try {
            leaseReaper.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String groupKey(String group, String topic) {
        return group + "/" + topic;
    }


    /**
     * Per (group, topic) coordination state.
     */
    private static class GroupTopicState {
        final ReentrantLock lock = new ReentrantLock();

        long dispatchOffset;

        long committedOffset;

        /** Active leases: consumerId → Lease */
        final Map<String, Lease> activeLeases = new LinkedHashMap<>();

        /** Ranges that have been committed but not yet merged into committedOffset. */
        final List<CommittedRange> committedRanges = new ArrayList<>();

        /** All consumer IDs currently participating in this group/topic. */
        final Set<String> members = new LinkedHashSet<>();

        /** Delivery attempt counts per offset for DLQ tracking. */
        final Map<Long, Integer> deliveryCounts = new HashMap<>();

        GroupTopicState(long startOffset) {
            this.dispatchOffset = startOffset;
            this.committedOffset = startOffset;
        }
    }

    /**
     * A lease grants a consumer exclusive access to a range of offsets.
     */
    private static class Lease {
        final String consumerId;
        final long fromOffset;    
        final long toOffset;     
        final long expiresAt;     

        Lease(String consumerId, long fromOffset, long toOffset, long expiresAt) {
            this.consumerId = consumerId;
            this.fromOffset = fromOffset;
            this.toOffset = toOffset;
            this.expiresAt = expiresAt;
        }
    }

    /**
     * A range of offsets that a consumer has successfully committed.
     */
    private static class CommittedRange {
        final long fromOffset;  
        final long toOffset;    

        CommittedRange(long fromOffset, long toOffset) {
            this.fromOffset = fromOffset;
            this.toOffset = toOffset;
        }
    }
}
