package com.drmq.broker;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Lightweight HTTP server for administrative REST APIs.
 */
public class AdminHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(AdminHttpServer.class);
    private final HttpServer server;
    private final MessageStore messageStore;
    private final OffsetManager offsetManager;
    private final ConsumerGroupCoordinator groupCoordinator;
    private final Gson gson = new Gson();

    public AdminHttpServer(int port, MessageStore messageStore, OffsetManager offsetManager, ConsumerGroupCoordinator groupCoordinator) throws IOException {
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.groupCoordinator = groupCoordinator;
        
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        
        this.server.createContext("/api/topics", this::handleTopics);
        this.server.createContext("/api/consumers", this::handleConsumers);
        this.server.createContext("/api/messages", this::handleMessages);
        
        // CORS and standard executor
        this.server.setExecutor(null); 
    }

    public void start() {
        server.start();
        logger.info("Admin HTTP Server started on port {}", server.getAddress().getPort());
    }

    public void stop() {
        server.stop(1);
        logger.info("Admin HTTP Server stopped.");
    }

    private void handleTopics(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        JsonArray topicsArray = new JsonArray();
        List<String> topics = messageStore.getTopics();
        
        for (String topic : topics) {
            JsonObject obj = new JsonObject();
            obj.addProperty("name", topic);
            obj.addProperty("messageCount", messageStore.getMessageCount(topic));
            // The global offset is global, not per topic. 
            // We'll just return the message count for now.
            topicsArray.add(obj);
        }

        sendJsonResponse(exchange, 200, gson.toJson(topicsArray));
    }

    private void handleConsumers(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        JsonArray groupsArray = new JsonArray();
        java.util.Map<String, Long> allOffsets = offsetManager.getAllOffsets();
        
        // Group by consumer group name
        java.util.Map<String, JsonArray> groupsMap = new java.util.HashMap<>();
        
        for (java.util.Map.Entry<String, Long> entry : allOffsets.entrySet()) {
            String[] parts = entry.getKey().split("/");
            if (parts.length != 2) continue;
            
            String groupName = parts[0];
            String topicName = parts[1];
            long committedOffset = entry.getValue();
            
            // Calculate lag using true topic head offset rather than message count
            // Since DRMQ uses global offsets, messageCount does not correlate to the offset values.
            long headOffset = messageStore.getHeadOffset(topicName);
            long lag = 0;
            if (headOffset >= 0) {
                long effectiveCommitted = Math.max(0, committedOffset); // -1 means none committed
                lag = Math.max(0, (headOffset + 1) - effectiveCommitted);
            }
            
            JsonObject topicObj = new JsonObject();
            topicObj.addProperty("topic", topicName);
            topicObj.addProperty("headOffset", headOffset);
            topicObj.addProperty("committedOffset", committedOffset);
            topicObj.addProperty("lag", lag);
            topicObj.addProperty("activeMembers", groupCoordinator.getConsumerCount(groupName, topicName));
            
            groupsMap.computeIfAbsent(groupName, k -> new JsonArray()).add(topicObj);
        }
        
        for (java.util.Map.Entry<String, JsonArray> entry : groupsMap.entrySet()) {
            JsonObject groupObj = new JsonObject();
            groupObj.addProperty("groupId", entry.getKey());
            groupObj.add("topics", entry.getValue());
            groupsArray.add(groupObj);
        }

        sendJsonResponse(exchange, 200, gson.toJson(groupsArray));
    }

    private void handleMessages(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        try {
            String query = exchange.getRequestURI().getQuery();
            if (query == null) {
                sendJsonResponse(exchange, 400, "{\"error\":\"Missing query parameters\"}");
                return;
            }

            String topic = null;
            long offset = 0;
            int limit = 10;

            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length == 2) {
                    if ("topic".equals(pair[0])) topic = pair[1];
                    else if ("offset".equals(pair[0])) offset = Long.parseLong(pair[1]);
                    else if ("limit".equals(pair[0])) limit = Integer.parseInt(pair[1]);
                }
            }

            if (topic == null) {
                sendJsonResponse(exchange, 400, "{\"error\":\"Missing 'topic' parameter\"}");
                return;
            }

            // Limit bounds to avoid OOM
            limit = Math.min(100, Math.max(1, limit));

            List<com.drmq.protocol.DRMQProtocol.StoredMessage> messages = messageStore.getMessages(topic, offset, limit);
            JsonArray msgsArray = new JsonArray();

            for (com.drmq.protocol.DRMQProtocol.StoredMessage msg : messages) {
                JsonObject obj = new JsonObject();
                obj.addProperty("offset", msg.getOffset());
                obj.addProperty("timestamp", msg.getTimestamp());
                obj.addProperty("storedAt", msg.getStoredAt());
                if (msg.hasKey()) {
                    obj.addProperty("key", msg.getKey());
                }
                obj.addProperty("payload", msg.getPayload().toStringUtf8());
                msgsArray.add(obj);
            }

            sendJsonResponse(exchange, 200, gson.toJson(msgsArray));
        } catch (Exception e) {
            logger.error("Error handling messages request", e);
            sendJsonResponse(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    private void addCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, Authorization");
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] bytes = response.getBytes("UTF-8");
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
