package com.drmq.integration;

import com.drmq.broker.BrokerConfig;
import com.drmq.broker.BrokerConfig.PeerAddress;
import com.drmq.broker.BrokerServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LeaderElectionLatencyBenchmarkTest {

    @TempDir
    Path tempDir;

    private static final int PORT_1 = 19600;
    private static final int PORT_2 = 19601;
    private static final int PORT_3 = 19602;

    private BrokerConfig clusterConfig(String nodeId, int port, String dataDirName) {
        List<PeerAddress> peers = switch (nodeId) {
            case "b1" -> List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3));
            case "b2" -> List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3));
            case "b3" -> List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2));
            default -> throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        };
        return new BrokerConfig(nodeId, port, tempDir.resolve(dataDirName).toString(), peers);
    }

    @Test
    void runElectionLatencyBenchmark() throws Exception {
        int totalTrials = 5;
        List<Long> latencies = new ArrayList<>();

        System.out.println("=================================================");
        System.out.println("STARTING LEADER ELECTION LATENCY BENCHMARK (" + totalTrials + " trials)");
        System.out.println("=================================================");

        for (int trial = 1; trial <= totalTrials; trial++) {
            Path trialDir = tempDir.resolve("trial-" + trial);
            
            BrokerServer b1 = new BrokerServer(new BrokerConfig("b1", PORT_1, trialDir.resolve("data-1").toString(), 
                    List.of(new PeerAddress("b2", "localhost", PORT_2), new PeerAddress("b3", "localhost", PORT_3))));
            BrokerServer b2 = new BrokerServer(new BrokerConfig("b2", PORT_2, trialDir.resolve("data-2").toString(), 
                    List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b3", "localhost", PORT_3))));
            BrokerServer b3 = new BrokerServer(new BrokerConfig("b3", PORT_3, trialDir.resolve("data-3").toString(), 
                    List.of(new PeerAddress("b1", "localhost", PORT_1), new PeerAddress("b2", "localhost", PORT_2))));

            b1.startAsync();
            b2.startAsync();
            b3.startAsync();

            // Wait for initial leader
            waitForLeader(b1, b2, b3, 10000);

            BrokerServer leader = findLeader(b1, b2, b3);
            if (leader == null) {
                System.out.println("Trial " + trial + ": Failed to elect initial leader!");
                shutdownAll(b1, b2, b3);
                continue;
            }

            // Kill leader & measure failover latency
            long startTime = System.currentTimeMillis();
            leader.shutdown();

            if (leader == b1) b1 = null;
            else if (leader == b2) b2 = null;
            else b3 = null;

            boolean elected = waitForLeader(b1, b2, b3, 10000);
            long latencyMs = System.currentTimeMillis() - startTime;

            if (elected) {
                latencies.add(latencyMs);
                System.out.println("Trial " + trial + "/" + totalTrials + ": Leader Failover Latency = " + latencyMs + " ms");
            } else {
                System.out.println("Trial " + trial + "/" + totalTrials + ": Re-election timed out!");
            }

            shutdownAll(b1, b2, b3);
            Thread.sleep(300);
        }

        if (latencies.isEmpty()) {
            System.out.println("No valid latency samples collected.");
            return;
        }

        Collections.sort(latencies);
        long min = latencies.get(0);
        long p50 = getPercentile(latencies, 50);
        long p95 = getPercentile(latencies, 95);
        long p99 = getPercentile(latencies, 99);
        long max = latencies.get(latencies.size() - 1);

        System.out.println("\n=================================================");
        System.out.println("TABLE 4.4: LEADER ELECTION LATENCY RESULTS");
        System.out.println("=================================================");
        System.out.println("Trials Run : " + latencies.size());
        System.out.println("Minimum    : " + min + " ms");
        System.out.println("p50 (median): " + p50 + " ms");
        System.out.println("p95        : " + p95 + " ms");
        System.out.println("p99        : " + p99 + " ms");
        System.out.println("Maximum    : " + max + " ms");
        System.out.println("=================================================");
    }

    private static long getPercentile(List<Long> sorted, double percentile) {
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
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

    private BrokerServer findLeader(BrokerServer b1, BrokerServer b2, BrokerServer b3) {
        if (b1 != null && b1.getRaftNode() != null && b1.getRaftNode().isLeader()) return b1;
        if (b2 != null && b2.getRaftNode() != null && b2.getRaftNode().isLeader()) return b2;
        if (b3 != null && b3.getRaftNode() != null && b3.getRaftNode().isLeader()) return b3;
        return null;
    }

    private void shutdownAll(BrokerServer b1, BrokerServer b2, BrokerServer b3) {
        if (b1 != null) b1.shutdown();
        if (b2 != null) b2.shutdown();
        if (b3 != null) b3.shutdown();
    }
}
