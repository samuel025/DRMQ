package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.persistence.LogSegment;
import com.drmq.broker.raft.RaftNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class BrokerMetrics implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BrokerMetrics.class);

    private static final BrokerMetrics NOOP = new BrokerMetrics(false, null, null, 0, "/metrics");
    private static volatile BrokerMetrics INSTANCE = NOOP;

    private final boolean enabled;
    private final PrometheusMeterRegistry prometheusRegistry;
    private final MeterRegistry registry;
    private final int metricsPort;
    private final String metricsPath;
    private final String summaryPath;
    private final long startNanos;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();

    private HttpServer httpServer;

    private BrokerMetrics(boolean enabled, PrometheusMeterRegistry prometheusRegistry,
                          MeterRegistry registry, int metricsPort, String metricsPath) {
        this.enabled = enabled;
        this.prometheusRegistry = prometheusRegistry;
        this.registry = registry;
        this.metricsPort = metricsPort;
        this.metricsPath = normalizePath(metricsPath);
        this.summaryPath = this.metricsPath + "/summary";
        this.startNanos = System.nanoTime();
    }

    public static BrokerMetrics init(BrokerConfig config) {
        if (config.isMetricsEnabled()) {
            PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            prometheus.config().commonTags(
                    "node_id", config.getNodeId(),
                    "broker_port", String.valueOf(config.getPort())
            );
            INSTANCE = new BrokerMetrics(true, prometheus, prometheus,
                    config.getMetricsPort(), config.getMetricsPath());
        } else {
            INSTANCE = NOOP;
        }
        return INSTANCE;
    }

    public static BrokerMetrics get() {
        return INSTANCE;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void start() {
        if (!enabled || prometheusRegistry == null) {
            return;
        }
        int port = metricsPort;
        while (port < metricsPort + 10) {
            try {
                httpServer = HttpServer.create(new InetSocketAddress(port), 0);
                httpServer.createContext(metricsPath, this::handleScrape);
                httpServer.createContext(summaryPath, this::handleSummary);
                httpServer.setExecutor(Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "metrics-http");
                    t.setDaemon(true);
                    return t;
                }));
                httpServer.start();
                logger.info("Metrics endpoint started on :{}{}", port, metricsPath);
                logger.info("Metrics summary endpoint started on :{}{}", port, summaryPath);
                return;
            } catch (BindException e) {
                port += 1;
            } catch (IOException e) {
                logger.error("Failed to start metrics endpoint", e);
                return;
            }
        }
        logger.error("Failed to start metrics endpoint after {} attempts", 10);
    }

    public void registerBroker(Set<ClientHandler> activeHandlers, MessageStore messageStore,
                               OffsetManager offsetManager, LogManager logManager, RaftNode raftNode) {
        if (!enabled || registry == null) {
            return;
        }
        Gauge.builder("drmq.broker.active_handlers", activeHandlers, Set::size)
                .register(registry);
        Gauge.builder("drmq.broker.topics", messageStore, MessageStore::getTopicCount)
                .register(registry);
        Gauge.builder("drmq.broker.cache.total_messages", messageStore, MessageStore::getCachedMessageCount)
                .register(registry);
        Gauge.builder("drmq.broker.global_offset", messageStore, MessageStore::getCurrentOffset)
                .register(registry);
        Gauge.builder("drmq.broker.offset_entries", offsetManager, OffsetManager::getOffsetEntryCount)
                .register(registry);
        Gauge.builder("drmq.broker.log.segments", logManager, LogManager::getOpenSegmentCount)
                .register(registry);

        if (raftNode != null) {
            Gauge.builder("drmq.broker.raft.commit_index", raftNode, RaftNode::getCommitIndex)
                    .register(registry);
            Gauge.builder("drmq.broker.raft.last_applied", raftNode, RaftNode::getLastApplied)
                    .register(registry);
            Gauge.builder("drmq.broker.raft.is_leader", raftNode, node -> node.isLeader() ? 1 : 0)
                    .register(registry);
        }
    }

    public void registerLogSegment(String topic, LogSegment segment) {
        if (!enabled || registry == null || segment == null) {
            return;
        }
        Gauge.builder("drmq.broker.log.segment.size_bytes", segment, LogSegment::getSize)
                .tags(Tags.of("topic", topic))
                .register(registry);
    }

    public void recordConnectionOpened() {
        incrementCounter("drmq.broker.connection.opened", Tags.empty());
    }

    public void recordConnectionClosed() {
        incrementCounter("drmq.broker.connection.closed", Tags.empty());
    }

    public void recordRequest(String type, boolean success, long durationNanos, long bytes, long records) {
        if (!enabled) {
            return;
        }
        Tags typeTag = Tags.of("type", type);
        Tags outcomeTag = Tags.of("type", type, "outcome", success ? "success" : "error");

        incrementCounter("drmq.broker.request.total", outcomeTag);
        recordTimer("drmq.broker.request.latency", typeTag, durationNanos);

        if (bytes > 0) {
            incrementCounter("drmq.broker.request.bytes", typeTag, bytes);
        }
        if (records > 0) {
            incrementCounter("drmq.broker.request.records", typeTag, records);
        }
        if (!success) {
            incrementCounter("drmq.broker.request.errors", typeTag);
        }
    }

    public void recordLogAppend(long bytes, long durationNanos) {
        if (!enabled) {
            return;
        }
        incrementCounter("drmq.broker.log.append.bytes", Tags.empty(), bytes);
        recordTimer("drmq.broker.log.append.latency", Tags.empty(), durationNanos);
    }

    public void recordLogRead(long bytes, long durationNanos) {
        if (!enabled) {
            return;
        }
        incrementCounter("drmq.broker.log.read.bytes", Tags.empty(), bytes);
        recordTimer("drmq.broker.log.read.latency", Tags.empty(), durationNanos);
    }

    public void recordOffsetPersist(long durationNanos, boolean success) {
        if (!enabled) {
            return;
        }
        Tags outcomeTag = Tags.of("outcome", success ? "success" : "error");
        incrementCounter("drmq.broker.offset.persist.total", outcomeTag);
        recordTimer("drmq.broker.offset.persist.latency", outcomeTag, durationNanos);
    }

    public void recordRaftRpc(String type, boolean success, long durationNanos) {
        if (!enabled) {
            return;
        }
        Tags typeTag = Tags.of("type", type);
        Tags outcomeTag = Tags.of("type", type, "outcome", success ? "success" : "error");
        incrementCounter("drmq.broker.raft.rpc.total", outcomeTag);
        recordTimer("drmq.broker.raft.rpc.latency", typeTag, durationNanos);
    }

    public void recordRaftElection(boolean won, long durationNanos) {
        if (!enabled) {
            return;
        }
        Tags outcomeTag = Tags.of("outcome", won ? "won" : "lost");
        incrementCounter("drmq.broker.raft.election.total", outcomeTag);
        recordTimer("drmq.broker.raft.election.duration", outcomeTag, durationNanos);
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
        if (prometheusRegistry != null) {
            prometheusRegistry.close();
        }
    }

    private void handleScrape(HttpExchange exchange) throws IOException {
        String response = prometheusRegistry.scrape();
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    private void handleSummary(HttpExchange exchange) throws IOException {
        if (!enabled || registry == null) {
            sendJson(exchange, "{\"enabled\":false}");
            return;
        }

        StringBuilder json = new StringBuilder(512);
        json.append('{');
        appendJsonField(json, "enabled", "true", true);
        appendJsonField(json, "active_handlers", gaugeValue("drmq.broker.active_handlers"), true);
        appendJsonField(json, "topics", gaugeValue("drmq.broker.topics"), true);
        appendJsonField(json, "cache_total_messages", gaugeValue("drmq.broker.cache.total_messages"), true);
        appendJsonField(json, "global_offset", gaugeValue("drmq.broker.global_offset"), true);
        appendJsonField(json, "offset_entries", gaugeValue("drmq.broker.offset_entries"), true);
        appendJsonField(json, "log_segments", gaugeValue("drmq.broker.log.segments"), true);
        appendJsonField(json, "raft_is_leader", gaugeValue("drmq.broker.raft.is_leader"), true);
        appendJsonField(json, "raft_commit_index", gaugeValue("drmq.broker.raft.commit_index"), true);
        appendJsonField(json, "raft_last_applied", gaugeValue("drmq.broker.raft.last_applied"), true);

        json.append("\"requests\":{");
        appendJsonMetricObject(json, "produce", "drmq.broker.request.total", Tags.of("type", "produce", "outcome", "success"));
        json.append(',');
        appendJsonMetricObject(json, "consume", "drmq.broker.request.total", Tags.of("type", "consume", "outcome", "success"));
        json.append(',');
        appendJsonMetricObject(json, "commit_offset", "drmq.broker.request.total", Tags.of("type", "commit_offset", "outcome", "success"));
        json.append(',');
        appendJsonMetricObject(json, "fetch_offset", "drmq.broker.request.total", Tags.of("type", "fetch_offset", "outcome", "success"));
        json.append('}');

        double elapsedSeconds = elapsedSeconds();
        json.append(",\"throughput\":{");
        appendJsonThroughput(json, "produce",
            counterValue("drmq.broker.request.bytes", Tags.of("type", "produce")),
            counterValue("drmq.broker.request.records", Tags.of("type", "produce")),
            elapsedSeconds);
        json.append(',');
        appendJsonThroughput(json, "consume",
            counterValue("drmq.broker.request.bytes", Tags.of("type", "consume")),
            counterValue("drmq.broker.request.records", Tags.of("type", "consume")),
            elapsedSeconds);
        json.append('}');

        json.append(",\"latency\":{");
        appendJsonLatency(json, "produce", timerMeanSeconds("drmq.broker.request.latency", Tags.of("type", "produce")));
        json.append(',');
        appendJsonLatency(json, "consume", timerMeanSeconds("drmq.broker.request.latency", Tags.of("type", "consume")));
        json.append('}');

        json.append('}');
        sendJson(exchange, json.toString());
    }

    private void sendJson(HttpExchange exchange, String payload) throws IOException {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    private String gaugeValue(String name) {
        if (registry == null) {
            return "null";
        }
        Double value = registry.find(name).gauge() != null ? registry.find(name).gauge().value() : null;
        return value != null ? String.valueOf(value) : "null";
    }

    private void appendJsonField(StringBuilder json, String key, String value, boolean withComma) {
        json.append('\"').append(key).append("\":").append(value);
        if (withComma) {
            json.append(',');
        }
    }

    private void appendJsonMetricObject(StringBuilder json, String key, String counterName, Tags tags) {
        double total = counterValue(counterName, tags);
        json.append('\"').append(key).append("\":{");
        json.append("\"success_total\":").append(total);
        json.append('}');
    }

    private void appendJsonThroughput(StringBuilder json, String key, double bytesTotal,
                                      double recordsTotal, double elapsedSeconds) {
        double safeSeconds = elapsedSeconds > 0 ? elapsedSeconds : 1.0;
        json.append('\"').append(key).append("\":{");
        json.append("\"bytes_total\":").append(bytesTotal).append(',');
        json.append("\"records_total\":").append(recordsTotal).append(',');
        json.append("\"bytes_per_sec\":").append(bytesTotal / safeSeconds).append(',');
        json.append("\"records_per_sec\":").append(recordsTotal / safeSeconds);
        json.append('}');
    }

    private void appendJsonLatency(StringBuilder json, String key, double meanSeconds) {
        json.append('\"').append(key).append("\":{");
        json.append("\"mean_seconds\":").append(meanSeconds);
        json.append('}');
    }

    private double counterValue(String name, Tags tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        return counter != null ? counter.count() : 0.0;
    }

    private double timerMeanSeconds(String name, Tags tags) {
        Timer timer = registry.find(name).tags(tags).timer();
        if (timer == null || timer.count() == 0) {
            return 0.0;
        }
        return timer.totalTime(TimeUnit.SECONDS) / timer.count();
    }

    private double elapsedSeconds() {
        return (System.nanoTime() - startNanos) / 1_000_000_000.0;
    }

    private void incrementCounter(String name, Tags tags) {
        Counter counter = counter(name, tags);
        if (counter != null) {
            counter.increment();
        }
    }

    private void incrementCounter(String name, Tags tags, double amount) {
        Counter counter = counter(name, tags);
        if (counter != null) {
            counter.increment(amount);
        }
    }

    private void recordTimer(String name, Tags tags, long durationNanos) {
        Timer timer = timer(name, tags);
        if (timer != null) {
            timer.record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }

    private Counter counter(String name, Tags tags) {
        if (!enabled || registry == null) {
            return null;
        }
        String key = key(name, tags);
        return counters.computeIfAbsent(key, k -> Counter.builder(name).tags(tags).register(registry));
    }

    private Timer timer(String name, Tags tags) {
        if (!enabled || registry == null) {
            return null;
        }
        String key = key(name, tags);
        return timers.computeIfAbsent(key, k -> Timer.builder(name).tags(tags).register(registry));
    }

    private static String key(String name, Tags tags) {
        StringBuilder builder = new StringBuilder(name);
        for (Tag tag : tags) {
            builder.append("|").append(tag.getKey()).append("=").append(tag.getValue());
        }
        return builder.toString();
    }

    private static String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/metrics";
        }
        return path.startsWith("/") ? path : "/" + path;
    }
}
