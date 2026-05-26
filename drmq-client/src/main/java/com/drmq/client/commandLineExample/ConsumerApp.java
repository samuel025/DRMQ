package com.drmq.client.commandLineExample;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import com.drmq.client.DRMQConsumer;

/**
 * Interactive Consumer CLI - allows subscribing and polling via command line.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp"
 *   mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp" -Dexec.args="localhost:9092,localhost:9093,localhost:9094"
 *   mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp" -Dexec.args="localhost:9092,localhost:9093,localhost:9094 my-service"
 */
public class ConsumerApp {
    public static void main(String[] args) {
        // First arg: bootstrap servers (default: localhost:9092)
        // Second arg: consumer group (default: "default")
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String consumerGroup = args.length > 1 ? args[1] : "default";

        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║     DRMQ Interactive Consumer CLI      ║");
        System.out.println("╚════════════════════════════════════════╝");
        System.out.printf( "  Consumer Group: %-23s\n", "[" + consumerGroup + "]");
        System.out.printf( "  Servers: %-30s\n\n", bootstrapServers);

        try (DRMQConsumer consumer = new DRMQConsumer(bootstrapServers, consumerGroup);
             Scanner scanner = new Scanner(System.in)) {

                consumer.connect();
                System.out.println("✓ Connected to broker");
            System.out.printf( "✓ Consumer ID: %s\n", consumer.getConsumerId());
            System.out.printf( "✓ Group mode: %s (broker coordinates offsets across consumers)\n",
                    consumer.isGroupMode() ? "on" : "off");
                System.out.printf("✓ Auto-commit: %s (use 'autocommit on|off')\n\n",
                    consumer.isAutoCommit() ? "on" : "off");

            printHelp();

            // Interactive loop
            while (true) {
                System.out.printf("consumer[%s]> ", consumerGroup);
                String input = scanner.nextLine().trim();

                if (input.isEmpty()) continue;

                String[] parts = input.split("\\s+");
                String command = parts[0].toLowerCase();

                switch (command) {
                    case "exit", "quit" -> {
                        System.out.println("\n✓ Goodbye!");
                        return;
                    }

                    case "help" -> printHelp();

                    case "subscribe", "sub" -> {
                        if (parts.length < 2) {
                            System.out.println("❌ Usage: subscribe <topic> [offset]");
                            System.out.println("   Example: subscribe orders");
                            System.out.println("   Example: subscribe payments 5\n");
                            continue;
                        }

                        String topic = parts[1];

                        try {
                            if (parts.length >= 3) {
                                // Explicit offset override
                                long offset;
                                try {
                                    offset = Long.parseLong(parts[2]);
                                } catch (NumberFormatException e) {
                                    System.out.println("❌ Invalid offset: " + parts[2] + "\n");
                                    continue;
                                }
                                consumer.subscribe(topic, offset);
                                System.out.printf("✓ Subscribed to [%s] from explicit offset %d\n\n", topic, offset);
                            } else {
                                // Let broker decide the starting offset
                                consumer.subscribe(topic);
                                System.out.printf("✓ Subscribed to [%s] (resuming from broker offset %d)\n\n",
                                        topic, consumer.getCurrentOffset(topic));
                            }
                        } catch (IOException e) {
                            System.out.printf("❌ Subscribe failed: %s\n\n", e.getMessage());
                        }
                    }

                    case "poll" -> {
                        int maxMessages = 100;
                        long timeoutMs = 1000; // default: 1s long-poll

                        if (parts.length >= 2) {
                            try {
                                maxMessages = Integer.parseInt(parts[1]);
                            } catch (NumberFormatException e) {
                                System.out.println("❌ Invalid max_messages: " + parts[1] + "\n");
                                continue;
                            }
                        }
                        if (parts.length >= 3) {
                            try {
                                timeoutMs = Long.parseLong(parts[2]);
                            } catch (NumberFormatException e) {
                                System.out.println("❌ Invalid timeout_ms: " + parts[2] + "\n");
                                continue;
                            }
                        }

                        try {
                            if (timeoutMs > 0) {
                                System.out.printf("Long-polling (waiting up to %dms for messages)...%n", timeoutMs);
                            } else {
                                System.out.println("Polling for messages...");
                            }
                            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(maxMessages, timeoutMs);

                            if (messages.isEmpty()) {
                                System.out.println("  No new messages\n");
                            } else {
                                System.out.printf("  Received %d message(s):\n\n", messages.size());
                                for (DRMQConsumer.ConsumedMessage msg : messages) {
                                    System.out.printf("  ┌─ [%s] Offset: %d\n", msg.topic(), msg.offset());
                                    System.out.printf("  │  Message: %s\n", msg.payloadAsString());
                                    if (msg.key() != null) {
                                        System.out.printf("  │  Key: %s\n", msg.key());
                                    }
                                    System.out.printf("  │  Timestamp: %d\n", msg.timestamp());
                                    System.out.println("  └─");
                                }
                                System.out.println();
                                if (consumer.isAutoCommit()) {
                                    System.out.println("  ✓ Offsets committed to broker automatically\n");
                                } else {
                                    System.out.println("  ⚠ Offsets NOT committed (auto-commit is off)\n");
                                }
                            }
                        } catch (IOException e) {
                            System.out.printf("❌ Error polling: %s\n\n", e.getMessage());
                        }
                    }

                    case "stream" -> {
                        // Continuous mode: keep long-polling until Ctrl+C
                        long streamTimeout = 2000; // 2s long-poll per iteration
                        if (parts.length >= 2) {
                            try {
                                streamTimeout = Long.parseLong(parts[1]);
                            } catch (NumberFormatException e) {
                                System.out.println("❌ Invalid timeout_ms: " + parts[1] + "\n");
                                continue;
                            }
                        }
                        System.out.printf("📡 Streaming... (press Ctrl+C to stop) [auto-commit=%s]\n\n",
                                consumer.isAutoCommit() ? "on" : "off");
                        try {
                            while (true) {
                                List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(100, streamTimeout);
                                for (DRMQConsumer.ConsumedMessage msg : messages) {
                                    System.out.printf("  ┌─ [%s] Offset: %d\n", msg.topic(), msg.offset());
                                    System.out.printf("  │  Message: %s\n", msg.payloadAsString());
                                    if (msg.key() != null) {
                                        System.out.printf("  │  Key: %s\n", msg.key());
                                    }
                                    System.out.printf("  │  Timestamp: %d\n", msg.timestamp());
                                    System.out.println("  └─");
                                }
                            }
                        } catch (IOException e) {
                            System.out.printf("❌ Stream error: %s\n\n", e.getMessage());
                        }
                    }

                    case "status" -> {
                        System.out.printf("\nGroup: %s\n", consumerGroup);
                        System.out.printf("Consumer ID: %s\n", consumer.getConsumerId());
                        System.out.println("─────────────────────────────");
                        System.out.printf("Group mode:  %s\n", consumer.isGroupMode() ? "on (broker coordinates)" : "off (single mode)");
                        System.out.printf("Auto-commit: %s\n", consumer.isAutoCommit() ? "on" : "off");
                        System.out.println("  (Use 'subscribe <topic>' to add subscriptions)");
                        System.out.println("  Tip: Multiple consumers with the same group share the workload!\n");
                    }

                    case "commit" -> {
                        if (parts.length < 2) {
                            System.out.println("❌ Usage: commit <topic> [offset]\n");
                            continue;
                        }

                        String topic = parts[1];
                        long offset;
                        if (parts.length >= 3) {
                            try {
                                offset = Long.parseLong(parts[2]);
                            } catch (NumberFormatException e) {
                                System.out.println("❌ Invalid offset: " + parts[2] + "\n");
                                continue;
                            }
                        } else {
                            offset = consumer.getCurrentOffset(topic);
                        }

                        try {
                            consumer.commit(topic, offset);
                            System.out.printf("✓ Committed offset %d for topic '%s'\n\n", offset, topic);
                        } catch (IOException e) {
                            System.out.printf("❌ Commit failed: %s\n\n", e.getMessage());
                        }
                    }

                    case "autocommit" -> {
                        if (parts.length < 2) {
                            System.out.println("❌ Usage: autocommit on|off\n");
                            continue;
                        }
                        String value = parts[1].toLowerCase();
                        if ("on".equals(value) || "true".equals(value)) {
                            consumer.setAutoCommit(true);
                            System.out.println("✓ Auto-commit enabled\n");
                        } else if ("off".equals(value) || "false".equals(value)) {
                            consumer.setAutoCommit(false);
                            System.out.println("✓ Auto-commit disabled\n");
                        } else {
                            System.out.println("❌ Usage: autocommit on|off\n");
                        }
                    }

                    case "mode" -> {
                        if (parts.length < 2) {
                            System.out.printf("Current mode: %s\n",
                                    consumer.isGroupMode() ? "group (broker coordinates)" : "single mode (client offsets)");
                            System.out.println("Usage: mode group|single\n");
                            continue;
                        }
                        String value = parts[1].toLowerCase();
                        if ("group".equals(value)) {
                            consumer.setGroupMode(true);
                            System.out.println("✓ Switched to group mode (broker coordinates offsets)\n");
                        } else if ("single".equals(value)) {
                            consumer.setGroupMode(false);
                            System.out.println("✓ Switched to single mode (client manages offsets)\n");
                        } else {
                            System.out.println("❌ Usage: mode group|single\n");
                        }
                    }

                    default -> {
                        System.out.printf("❌ Unknown command: %s\n", command);
                        System.out.println("   Type 'help' for available commands\n");
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("\n❌ Failed to connect to broker: " + e.getMessage());
            System.err.println("\nMake sure the broker is running:");
            System.err.println("  cd drmq-broker && mvn exec:java\n");
        }
    }

    private static void printHelp() {
        System.out.println("\nCommands:");
        System.out.println("  subscribe <topic>                    - Resume from broker-committed offset");
        System.out.println("  subscribe <topic> <offset>           - Override to a specific offset");
        System.out.println("  poll [max] [timeout_ms]              - One-shot fetch (long-poll if timeout_ms>0)");
        System.out.println("    timeout_ms=0 → short poll (return immediately if empty)");
        System.out.println("    timeout_ms>0 → long poll (broker waits up to N ms for messages)");
        System.out.println("  commit <topic> [offset]              - Commit current or explicit offset");
        System.out.println("  autocommit on|off                    - Enable/disable auto-commit");
        System.out.println("  mode group|single                    - Switch consumption mode");
        System.out.println("  stream [timeout_ms]                  - Continuous mode: auto-polls forever");
        System.out.println("    (press Ctrl+C to stop streaming)");
        System.out.println("  status                               - Show group info");
        System.out.println("  exit                                 - Quit");
        System.out.println("  help                                 - Show this help");
        System.out.println("\nExamples:");
        System.out.println("  subscribe orders       (resume from last offset)");
        System.out.println("  subscribe payments 5   (seek to offset 5)");
        System.out.println("  poll                   (long-poll, 1s timeout)");
        System.out.println("  poll 50 2000           (up to 50 msgs, wait 2s)");
        System.out.println("  poll 100 0             (short poll)");
        System.out.println();
        System.out.println("─────────────────────────────────────────\n");
    }
}

