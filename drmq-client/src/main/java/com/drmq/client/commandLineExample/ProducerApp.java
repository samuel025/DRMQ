package com.drmq.client.commandLineExample;

import java.io.IOException;
import java.util.Scanner;

import com.drmq.client.DRMQProducer;

/**
 * Interactive Producer CLI - allows sending messages via command line.
 * Type topic and message interactively.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp"
 *   mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp" -Dexec.args="localhost:9092,localhost:9093,localhost:9094"
 */
public class ProducerApp {
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║     DRMQ Interactive Producer CLI     ║");
        System.out.println("╚════════════════════════════════════════╝\n");

        // Accept bootstrap servers as first argument (e.g. "localhost:9092,localhost:9093,localhost:9094")
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        try (DRMQProducer producer = new DRMQProducer(bootstrapServers);
             Scanner scanner = new Scanner(System.in)) {
            
            producer.connect();
            System.out.println("✓ Connected to broker (servers: " + bootstrapServers + ")\n");
            System.out.println("Commands:");
            System.out.println("  send <topic> <message>  - Send a message to a topic");
            System.out.println("  exit                    - Quit the application");
            System.out.println("  help                    - Show this help\n");
            System.out.println("Examples:");
            System.out.println("  send orders Book Order #101");
            System.out.println("  send payments Payment of ₦25.50");
            System.out.println("  send alerts System is ONLINE\n");
            System.out.println("─────────────────────────────────────────\n");

            // Interactive loop
            while (true) {
                System.out.print("producer> ");
                String input = scanner.nextLine().trim();
                
                if (input.isEmpty()) {
                    continue;
                }
                
                String[] parts = input.split("\\s+", 3);
                String command = parts[0].toLowerCase();
                
                switch (command) {
                    case "exit", "quit" -> {
                        System.out.println("\n✓ Goodbye!");
                        return;
                    }
                    
                    case "help" -> {
                        System.out.println("\nCommands:");
                        System.out.println("  send <topic> <message>  - Send a message");
                        System.out.println("  exit                    - Quit");
                        System.out.println("  help                    - Show help\n");
                    }
                    
                    case "send" -> {
                        if (parts.length < 3) {
                            System.out.println("❌ Usage: send <topic> <message>");
                            System.out.println("   Example: send orders Book Order #101\n");
                            continue;
                        }
                        
                        String topic = parts[1];
                        String message = parts[2];
                        
                        try {
                            DRMQProducer.SendResult result = producer.send(topic, message);
                            
                            if (result.isSuccess()) {
                                System.out.printf("✓ Sent to [%s] at offset %d\n\n", topic, result.getOffset());
                            } else {
                                System.out.printf("❌ Failed: %s\n\n", result.getErrorMessage());
                            }
                        } catch (IOException e) {
                            System.out.printf("❌ Error: %s\n\n", e.getMessage());
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
}

