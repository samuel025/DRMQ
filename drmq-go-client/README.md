# DRMQ Go Client

Go client library for the DRMQ (Distributed Reliable Message Queue) broker.

## Installation

```bash
go get github.com/samuel025/DRMQ/drmq-go-client
```

## Quick Start

### Producer

```go
package main

import (
    "fmt"
    "log"
    "time"

    drmq "github.com/samuel025/DRMQ/drmq-go-client"
)

func main() {
    producer, err := drmq.NewProducer(drmq.ProducerConfig{
        BootstrapServers: "localhost:9092",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    if err := producer.Connect(); err != nil {
        log.Fatal(err)
    }

    // Send a message (async)
    future := producer.SendString("my-topic", "Hello from Go!")
    result, err := future.GetWithTimeout(5 * time.Second)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Sent at offset: %d\n", result.Offset)

    // Send with key
    future = producer.SendWithKey("my-topic", []byte("data"), "user-123")
    result, _ = future.Get()

    // Atomic multi-topic send
    atomicFuture := producer.SendAtomic(map[string][]byte{
        "orders":   []byte(`{"orderId": "1"}`),
        "payments": []byte(`{"paymentId": "2"}`),
    })
    offsets, _ := atomicFuture.Get()
    fmt.Printf("Atomic offsets: %v\n", offsets)
}
```

### Consumer (Single Mode)

```go
consumer, err := drmq.NewConsumer(drmq.ConsumerConfig{
    BootstrapServers: "localhost:9092",
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

consumer.Connect()
consumer.Subscribe("my-topic")

for {
    messages, err := consumer.Poll()
    if err != nil {
        log.Printf("Poll error: %v", err)
        continue
    }
    for _, msg := range messages {
        fmt.Printf("offset=%d payload=%s\n", msg.Offset, msg.PayloadAsString())
    }
}
```

### Consumer (Group Mode)

```go
consumer, err := drmq.NewConsumer(drmq.ConsumerConfig{
    BootstrapServers: "localhost:9092,localhost:9093,localhost:9094",
    ConsumerGroup:    "my-group",
    AutoCommit:       true,
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

consumer.Connect()
consumer.Subscribe("my-topic")

messages, err := consumer.PollWithOptions(50, 2000) // max 50 msgs, 2s timeout
for _, msg := range messages {
    fmt.Printf("offset=%d payload=%s\n", msg.Offset, msg.PayloadAsString())
}
```

## Features

| Feature | Supported |
|---------|-----------|
| Bootstrap server failover | ✅ |
| Leader redirection (NOT_LEADER) | ✅ |
| Automatic reconnection | ✅ |
| Single consumption mode | ✅ |
| Consumer group mode | ✅ |
| Auto-commit offsets | ✅ |
| Manual commit | ✅ |
| NACK / Dead-Letter Queue | ✅ |
| Seek by timestamp | ✅ |
| Async batched producer | ✅ |
| Keyed messages | ✅ |
| Atomic multi-topic produce | ✅ |
| Configurable batch size & linger | ✅ |
| Inflight backpressure | ✅ |

## Wire Protocol

The Go client uses the same length-prefixed protobuf wire protocol as the Java, Python, and TypeScript clients:

```
[4-byte big-endian length][protobuf MessageEnvelope]
```

## Building from Source

```bash
# Regenerate protobuf (requires protoc + protoc-gen-go)
make proto

# Build
make build

# Vet
make vet
```
