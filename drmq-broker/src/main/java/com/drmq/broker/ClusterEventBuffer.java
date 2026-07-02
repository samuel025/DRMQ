package com.drmq.broker;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe ring buffer of recent cluster events for telemetry.
 *
 * Components across the broker (Raft, ClientHandler, SnapshotManager, etc.)
 * call the static {@code emit*} helpers to record events. The telemetry
 * WebSocket server drains the buffer into the JSON payload each tick.
 *
 * Max capacity: 50 events. Oldest events are evicted on overflow.
 */
public final class ClusterEventBuffer {

    private static final int MAX_EVENTS = 50;
    private static final ClusterEventBuffer INSTANCE = new ClusterEventBuffer();

    private final ConcurrentLinkedDeque<Event> events = new ConcurrentLinkedDeque<>();
    private final AtomicLong idCounter = new AtomicLong(0);

    /** Event severity levels (must match frontend ClusterEvent.severity). */
    public enum Severity { info, warn, error }

    /** Event types (must match frontend ClusterEvent.type). */
    public enum EventType { REPLICATION, ELECTION, ERROR, SNAPSHOT, CONNECTION }

    /** Immutable event record. */
    public record Event(
        String id,
        long timestamp,
        EventType type,
        String message,
        String nodeId,
        Severity severity
    ) {}

    private ClusterEventBuffer() {}

    public static ClusterEventBuffer get() {
        return INSTANCE;
    }

    // ── Core emit ───────────────────────────────────────────────────────────

    /**
     * Record a new event. Thread-safe.
     */
    public void emit(EventType type, Severity severity, String message, String nodeId) {
        Event evt = new Event(
            "evt-" + idCounter.incrementAndGet(),
            System.currentTimeMillis(),
            type,
            message,
            nodeId,
            severity
        );
        events.addFirst(evt);
        // Evict overflow from tail
        while (events.size() > MAX_EVENTS) {
            events.pollLast();
        }
    }

    public void emit(EventType type, Severity severity, String message) {
        emit(type, severity, message, null);
    }

    // ── Convenience helpers ─────────────────────────────────────────────────

    /** Replication event: AppendEntries replicated, follower caught up, etc. */
    public static void emitReplication(String message, String nodeId) {
        INSTANCE.emit(EventType.REPLICATION, Severity.info, message, nodeId);
    }

    /** Election event: term change, vote, leader elected. */
    public static void emitElection(String message) {
        INSTANCE.emit(EventType.ELECTION, Severity.warn, message);
    }

    /** Snapshot event: snapshot created, installed, log compacted. */
    public static void emitSnapshot(String message, String nodeId) {
        INSTANCE.emit(EventType.SNAPSHOT, Severity.info, message, nodeId);
    }

    /** Connection event: producer/consumer connected or disconnected. */
    public static void emitConnection(String message) {
        INSTANCE.emit(EventType.CONNECTION, Severity.info, message);
    }

    /** Error event: produce timeout, replication stall, etc. */
    public static void emitError(String message, String nodeId) {
        INSTANCE.emit(EventType.ERROR, Severity.error, message, nodeId);
    }

    public static void emitError(String message) {
        INSTANCE.emit(EventType.ERROR, Severity.error, message, null);
    }

    // ── Serialisation ───────────────────────────────────────────────────────

    /**
     * Serialise current events to a Gson {@link JsonArray} for the telemetry payload.
     */
    public JsonArray toJsonArray() {
        JsonArray arr = new JsonArray();
        for (Event e : events) {
            JsonObject obj = new JsonObject();
            obj.addProperty("id", e.id());
            obj.addProperty("timestamp", e.timestamp());
            obj.addProperty("type", e.type().name());
            obj.addProperty("message", e.message());
            if (e.nodeId() != null) {
                obj.addProperty("nodeId", e.nodeId());
            }
            obj.addProperty("severity", e.severity().name());
            arr.add(obj);
        }
        return arr;
    }
}
