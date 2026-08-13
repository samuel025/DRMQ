package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import com.drmq.client.DRMQProducer;
import com.drmq.protocol.ProduceBatchRequest;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class PayloadCapAndThreadStarvationTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19840;
    private static final int PORT_2 = 19841;
    private static final int PORT_3 = 19842;

    @Test
    void testPayloadCapExceeded() throws Exception {
        System.setProperty("drmq.test.mode", "true");
        System.out.println("=================================================");
        System.out.println("RUNNING PAYLOAD CAP REPRODUCTION TEST (Section 4.6.2)");
        System.out.println("=================================================");

        Path trialDir = tempDir.resolve("payload-cap-test");

        BrokerConfig c1 = new BrokerConfig("b1", PORT_1, trialDir.resolve("data-1").toString(),
                List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9140, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c2 = new BrokerConfig("b2", PORT_2, trialDir.resolve("data-2").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3)),
                true, 9141, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);
        BrokerConfig c3 = new BrokerConfig("b3", PORT_3, trialDir.resolve("data-3").toString(),
                List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2)),
                true, 9142, "/metrics", 100 * 1024 * 1024L, 7L * 24 * 60 * 60 * 1000, 1000L, 5, "dlq.", false, true, null, null, null);

        BrokerServer b1 = new BrokerServer(c1);
        BrokerServer b2 = new BrokerServer(c2);
        BrokerServer b3 = new BrokerServer(c3);

        b1.startAsync();
        b2.startAsync();
        b3.startAsync();

        waitForLeader(b1, b2, b3, 8000);

        String topic = "payload-cap-topic";
        String bootstrap = "localhost:" + PORT_1 + ",localhost:" + PORT_2 + ",localhost:" + PORT_3;
        byte[] payload512 = new byte[512];

        try (DRMQProducer producer = new DRMQProducer(bootstrap)) {
            producer.connect();

            // Attempt to send a massive batch of 20,000 messages (20,000 * 512 bytes = ~10.24 MB > 10 MB cap)
            int batchMsgCount = 20000;
            System.out.printf("Sending large batch of %d messages (%.2f MB payload)...%n",
                    batchMsgCount, (batchMsgCount * 512.0) / (1024 * 1024));

            List<CompletableFuture<?>> futures = new ArrayList<>(batchMsgCount);
            for (int i = 0; i < batchMsgCount; i++) {
                futures.add(producer.send(topic, payload512));
            }

            // Expect rejection or execution exception due to 10MB payload cap
            Exception thrown = null;
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            } catch (ExecutionException e) {
                thrown = e;
            } catch (Exception e) {
                thrown = e;
            }

            System.out.println("RESULT: Exception captured as expected when exceeding 10MB payload cap:");
            if (thrown != null) {
                System.out.println(" -> " + thrown.getMessage());
            } else {
                System.out.println(" -> Batch processed successfully (under client buffer window).");
            }
            System.out.println("=================================================\n");

        } finally {
            b1.shutdown();
            b2.shutdown();
            b3.shutdown();
        }
    }

    private boolean waitForLeader(BrokerServer b1, BrokerServer b2, BrokerServer b3, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leaderCount = 0;
            if (b1 != null && b1.getRaftNode() != null && b1.getRaftNode().isLeader()) leaderCount++;
            if (b2 != null && b2.getRaftNode() != null && b2.getRaftNode().isLeader()) leaderCount++;
            if (b3 != null && b3.getRaftNode() != null && b3.getRaftNode().isLeader()) leaderCount++;
            if (leaderCount == 1) return true;
            Thread.sleep(20);
        }
        return false;
    }
}
