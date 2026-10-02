//go:build ignore

package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	drmq "github.com/samuel025/DRMQ/drmq-go-client"
)

func main() {
	// Configure logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Broker addresses you'll be spinning up
	bootstrapServers := "localhost:9092,localhost:9093,localhost:9094"
	topic := "e2e-test-topic"

	fmt.Println("==================================================")
	fmt.Println("Starting DRMQ Go Client E2E Integration Test")
	fmt.Printf("Bootstrap Servers: %s\n", bootstrapServers)
	fmt.Printf("Target Topic:      %s\n", topic)
	fmt.Println("==================================================")

	// 1. Create and Connect Producer
	fmt.Println("\n[1] Starting Producer...")
	producer, err := drmq.NewProducer(drmq.ProducerConfig{
		BootstrapServers: bootstrapServers,
		Logger:           logger,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	if err := producer.Connect(); err != nil {
		log.Fatalf("Producer failed to connect: %v", err)
	}
	fmt.Println(" -> Producer connected successfully.")

	// 2. Send messages
	fmt.Println("\n[2] Sending messages...")
	messagesSent := 0
	for i := 1; i <= 5; i++ {
		payload := fmt.Sprintf("Hello from Go Client - Message %d", i)
		key := fmt.Sprintf("key-%d", i)
		
		future := producer.SendWithKey(topic, []byte(payload), key)
		result, err := future.GetWithTimeout(5 * time.Second)
		if err != nil {
			log.Printf(" -> Failed to send message %d: %v", i, err)
		} else if !result.Success {
			log.Printf(" -> Broker rejected message %d: %s", i, result.ErrorMessage)
		} else {
			fmt.Printf(" -> Sent message %d (offset: %d)\n", i, result.Offset)
			messagesSent++
		}
	}

	// Wait briefly to ensure replication/flushing if needed by the broker
	time.Sleep(1 * time.Second)

	// 3. Create and Connect Consumer
	fmt.Println("\n[3] Starting Consumer (Single Mode)...")
	consumer, err := drmq.NewConsumer(drmq.ConsumerConfig{
		BootstrapServers: bootstrapServers,
		Logger:           logger,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Connect(); err != nil {
		log.Fatalf("Consumer failed to connect: %v", err)
	}
	fmt.Println(" -> Consumer connected successfully.")

	// Subscribe from the beginning
	if err := consumer.SubscribeFrom(topic, 0); err != nil {
		log.Fatalf("Consumer failed to subscribe: %v", err)
	}

	// 4. Poll for messages
	fmt.Println("\n[4] Polling for messages...")
	messagesReceived := 0
	
	// Poll loop to give it a few chances to retrieve messages
	for attempts := 0; attempts < 3 && messagesReceived < messagesSent; attempts++ {
		messages, err := consumer.PollWithOptions(100, 2000)
		if err != nil {
			log.Printf(" -> Poll error: %v", err)
			continue
		}

		for _, msg := range messages {
			fmt.Printf(" -> Received: offset=%d key=%s payload='%s'\n", 
				msg.Offset, msg.Key, msg.PayloadAsString())
			messagesReceived++
		}
	}

	// 5. Final Report
	fmt.Println("\n==================================================")
	if messagesReceived >= messagesSent && messagesSent > 0 {
		fmt.Printf("✅ E2E TEST PASSED! (Sent: %d, Received: %d)\n", messagesSent, messagesReceived)
	} else {
		fmt.Printf("❌ E2E TEST FAILED! (Sent: %d, Received: %d)\n", messagesSent, messagesReceived)
	}
	fmt.Println("==================================================")
}
