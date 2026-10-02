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
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	producerExample(logger)

	consumerExample(logger)
}

func producerExample(logger *slog.Logger) {
	fmt.Println("=== Producer Example ===")

	producer, err := drmq.NewProducer(drmq.ProducerConfig{
		BootstrapServers: "localhost:9092",
		BatchSizeBytes:   16384,
		LingerMs:         5,
		MaxInflight:      5,
		Logger:           logger,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	if err := producer.Connect(); err != nil {
		log.Fatalf("Failed to connect producer: %v", err)
	}

	future := producer.SendString("my-topic", "Hello from Go!")
	result, err := future.GetWithTimeout(5 * time.Second)
	if err != nil {
		log.Printf("Send failed: %v", err)
	} else {
		fmt.Printf("Message sent: %s\n", result)
	}

	future = producer.SendWithKey("my-topic", []byte("keyed message"), "user-123")
	result, err = future.GetWithTimeout(5 * time.Second)
	if err != nil {
		log.Printf("Keyed send failed: %v", err)
	} else {
		fmt.Printf("Keyed message sent: %s\n", result)
	}

	// Atomic multi-topic send
	atomicFuture := producer.SendAtomic(map[string][]byte{
		"orders":   []byte(`{"orderId": "123"}`),
		"payments": []byte(`{"paymentId": "456"}`),
	})
	offsets, err := atomicFuture.GetWithTimeout(5 * time.Second)
	if err != nil {
		log.Printf("Atomic send failed: %v", err)
	} else {
		fmt.Printf("Atomic send offsets: %v\n", offsets)
	}
}

func consumerExample(logger *slog.Logger) {
	fmt.Println("\n=== Consumer Example ===")

	// Single mode consumer
	consumer, err := drmq.NewConsumer(drmq.ConsumerConfig{
		BootstrapServers: "localhost:9092",
		Logger:           logger,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Connect(); err != nil {
		log.Fatalf("Failed to connect consumer: %v", err)
	}

	if err := consumer.Subscribe("my-topic"); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Poll loop
	for i := 0; i < 5; i++ {
		messages, err := consumer.Poll()
		if err != nil {
			log.Printf("Poll error: %v", err)
			continue
		}
		for _, msg := range messages {
			fmt.Printf("Received: topic=%s offset=%d payload=%s\n",
				msg.Topic, msg.Offset, msg.PayloadAsString())
		}
	}

	// --- Group mode consumer ---
	fmt.Println("\n=== Group Consumer Example ===")

	groupConsumer, err := drmq.NewConsumer(drmq.ConsumerConfig{
		BootstrapServers: "localhost:9092,localhost:9093,localhost:9094",
		ConsumerGroup:    "my-group",
		AutoCommit:       true,
		Logger:           logger,
	})
	if err != nil {
		log.Fatalf("Failed to create group consumer: %v", err)
	}
	defer groupConsumer.Close()

	if err := groupConsumer.Connect(); err != nil {
		log.Fatalf("Failed to connect group consumer: %v", err)
	}

	if err := groupConsumer.Subscribe("my-topic"); err != nil {
		log.Fatalf("Failed to subscribe group consumer: %v", err)
	}

	// Poll with custom settings
	messages, err := groupConsumer.PollWithOptions(50, 2000)
	if err != nil {
		log.Printf("Group poll error: %v", err)
	}
	for _, msg := range messages {
		fmt.Printf("Group received: topic=%s offset=%d payload=%s\n",
			msg.Topic, msg.Offset, msg.PayloadAsString())
	}
}
