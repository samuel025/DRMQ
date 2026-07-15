# DRMQ - Distributed Reliable Message Queue

**Official Documentation:** [https://drmq.vercel.app](https://drmq.vercel.app)

DRMQ is a fault-tolerant, high-performance distributed message broker built from first principles. It provides guaranteed message delivery, strict ordering, and high availability through the implementation of the Raft consensus algorithm for log replication and leader election. DRMQ supports scalable consumption via multi-consumer groups — multiple consumers can share a group to load-balance message processing without partitions.

## Overview

DRMQ is designed to be a resilient, easily understandable, and scalable message queue. It operates via a custom TCP protocol and supports both a standalone single-node mode for development or simple deployments, and a robust cluster mode for production environments requiring fault tolerance.

The project is structured as a multi-module Maven build, separating the core broker logic, client libraries, protocol definitions, and integration tests to ensure maintainability and clear boundaries.

## Key Features

- **Scalable Consumer Groups:** Scale your consumers dynamically without the complexity of partitions. Simply start multiple consumers with the same group name, and the broker will automatically distribute messages among them. Messages are load-balanced across consumers in a group with at-least-once delivery; a lease-based protocol ensures that uncommitted messages are redelivered if a consumer fails. Need to replay or read specific messages? Switch to single mode for full manual offset control.
- **Dead-Letter Queues (DLQ):** Gracefully handle poison pill messages. Consumers can explicitly `nack()` unprocessable messages. After a configurable threshold of delivery failures, the broker automatically routes the message to an isolated DLQ topic and advances the consumer group, preventing blockages.
- **Raft Consensus Integration:** Full implementation of the Raft protocol for distributed state replication, leader election, and high availability. Features **Quorum-Loss Stepdown** to detect network partitions and demote isolated leaders, preventing split-brain/ghost leadership data loss.
- **Persistent Storage:** Custom Write-Ahead Log (WAL) and segment-based message storage ensure messages are durably persisted to disk. Features thread-safe, atomic consumer offset management with bounds locking designed to minimize data loss during concurrent background writes and handle shutdowns gracefully.
- **Tiered Storage (S3 Archiving):** Seamlessly archive old log segments to Amazon S3 (or MinIO) to decouple storage costs from compute. Consumers requesting historical offsets transparently trigger the broker to download and resolve missing segments from the cloud, with built-in pagination and atomic concurrent recovery.
- **Time-based Message Lookup:** Clients can precisely rewind consumers to the earliest message at or after a specific UNIX timestamp (`seekByTime`), enabling accurate historical replay without knowing exact offsets.
- **Graceful Teardown Coordination:** Orchestrated, safe termination of Netty EventLoops, RPC executors, and disk storage guaranteeing state integrity without resource leaks during node shutdowns.
- **High Performance:**
  - **Client-Side Batching:** Producers feature high-throughput, latency-optimized message batching via a configurable `linger.ms` window. This groups thousands of messages into a single network round-trip and Raft log flush, massively increasing throughput.
  - **Configurable Disk Durability:** By default, DRMQ guarantees strict flush-before-ack durability (`fsync`). However, administrators can explicitly disable this (`--log-segment-fsync false`) for extreme throughput scenarios where hardware page-cache flushing is acceptable.
  - **Follower-based Reads:** Scalable read operations allowing consumers to fetch messages from follower nodes, distributing the load across the cluster.
  - **Dedicated Thread Pools:** Separated executor services for Raft tasks and client handling prevent thread starvation and ensure consistent performance. Independent scheduler threads prevent I/O blocking from stunting cluster heartbeats.
- **Robust Client Ecosystem:** Includes Java, Python, and TypeScript SDKs featuring automatic reconnects, randomized bootstrap load balancing, typed Error Code handling, and seamless leader failovers.
- **Metrics & Observability:** Integrated with Micrometer and Prometheus to provide deep visibility into broker health, log replication lag, and throughput metrics.

## Architecture & Modules

The repository is divided into several Maven modules:

- `drmq-protocol`: Defines the Protocol Buffers (protobuf) messages used for client-broker and inter-broker communication.
- `drmq-broker`: The core server implementation containing the Raft node logic, TCP server, message storage engine, and offset management.
- `drmq-client`: Java client library providing high-level `Producer` and `Consumer` APIs.
- `drmq-integration-tests`

## Prerequisites

- Java 17 or higher
- Maven 3.8.x or higher

## Building the Project

To compile the project, generate the protobuf classes, and build the artifacts, run the following command from the root directory:

```bash
mvn clean install
```

## Running the Broker

### Single-Node Mode

To run a standalone broker (useful for testing and development):

```bash
./mvnw -pl drmq-broker exec:java -Dexec.args="--port 9092 --data-dir ./data-1"
```

### Cluster Mode

To run a fault-tolerant cluster, you must start multiple broker instances and provide them with the addresses of their peers.

**Node 1:**

```bash
./mvnw -pl drmq-broker exec:java -Dexec.args="--node-id 1 --port 9092 --data-dir ./data-1 --peers 2:localhost:9093,3:localhost:9094"
```

**Node 2:**

```bash
./mvnw -pl drmq-broker exec:java -Dexec.args="--node-id 2 --port 9093 --data-dir ./data-2 --peers 1:localhost:9092,3:localhost:9094"
```

**Node 3:**

```bash
./mvnw -pl drmq-broker exec:java -Dexec.args="--node-id 3 --port 9094 --data-dir ./data-3 --peers 1:localhost:9092,2:localhost:9093"
```

### Configuration File

Instead of passing dozens of command-line arguments, DRMQ supports loading properties from a `.properties` file using the `--config` flag. This is highly recommended for production, especially when configuring Tiered Storage.

Create a `server.properties` file:
```properties
node.id=1
port=9092
data.dir=./data
peers=2:localhost:9093,3:localhost:9094

# S3 Tiered Storage Configuration
s3.archive.bucket=drmq-archive
s3.archive.region=us-east-1
#s3.archive.endpoint=http://localhost:9000 # Uncomment for MinIO

# Advanced Tuning
log.segment.bytes=10485760
log.retention.ms=86400000
log.segment.fsync=true
```

Then run the broker:
```bash
./mvnw -pl drmq-broker exec:java -Dexec.args="--config server.properties"
```

## Usage Example

### Producer

```java
try (DRMQProducer producer = new DRMQProducer("localhost:9092,localhost:9093")) {
    producer.connect();
    DRMQProducer.SendResult result = producer.send("my-topic", "Hello, DRMQ!");

    if (result.isSuccess()) {
        System.out.println("Message sent at offset " + result.getOffset());
    } else {
        System.err.println("Send failed: " + result.getErrorMessage());
    }
} catch (IOException e) {
    e.printStackTrace();
}
```

### Consumer

DRMQ supports two modes of consumption: **Group Mode** (for scalable, load-balanced processing) and **Single Consumer Mode** (for precise manual control and replay).

#### 1. Group Mode (Default - Auto Load Balancing)

By default, the broker coordinates message delivery. If you start multiple consumers with the same group name, the broker will automatically split the workload among them. Messages are load-balanced across consumers in a group with at-least-once delivery semantics — consumers should be idempotent to handle potential redelivery of uncommitted messages. No partitions required!

```java
// Consumer 1
DRMQConsumer c1 = new DRMQConsumer("localhost:9092,localhost:9093", "order-processors");
c1.setAutoCommit(true);
c1.connect();
c1.subscribe("orders"); // Broker assigns offsets automatically

// Consumer 2 (running on another machine/thread)
DRMQConsumer c2 = new DRMQConsumer("localhost:9092,localhost:9093", "order-processors");
c2.setAutoCommit(true);
c2.connect();
c2.subscribe("orders");

// The broker ensures c1 and c2 receive different messages.
// A different group (e.g., "analytics") would receive its own full copy of all messages.

while (true) {
    List<DRMQConsumer.ConsumedMessage> messages = c1.poll(100, 1000);
    for (DRMQConsumer.ConsumedMessage msg : messages) {
        System.out.printf("Received: %s\n", msg.payloadAsString());
    }
}
```

#### 2. Single Consumer Mode (Manual Offset Control & Replay)

If you need strict control over what messages you read—for example, if you want to replay messages from the beginning or start from a specific offset—you can disable group mode. In this mode, the client tells the broker exactly which offset to fetch.

```java
try (DRMQConsumer consumer = new DRMQConsumer("localhost:9092", "my-group")) {
    consumer.setGroupMode(false); // Disable broker coordination
    consumer.connect();
    
    // Subscribe and explicitly tell the broker to start from offset 0 (replay from the start)
    consumer.subscribe("my-topic", 0);

    // Alternatively, seek by time (timestamp in milliseconds)
    // consumer.seekByTime("my-topic", System.currentTimeMillis() - 3600000); // Replay last hour

    while (true) {
        List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(100, 1000);
        for (DRMQConsumer.ConsumedMessage msg : messages) {
            System.out.printf("Replaying (offset %d): %s\n", msg.offset(), msg.payloadAsString());
        }
        
        // In Single consumer mode, you must manually commit the offset if you want the broker to remember where you stopped
        if (!messages.isEmpty()) {
            long lastOffset = messages.get(messages.size() - 1).offset();
            consumer.commit("my-topic", lastOffset + 1);
        }
    }
} catch (IOException e) {
    e.printStackTrace();
}
```

#### 3. Dead-Letter Queues (DLQ) & Explicit NACK

If a consumer encounters a "poison pill" (a message that always causes a crash or fails validation), it can explicitly reject it using `nack()`. If a message fails too many times (default 5), the broker will automatically route it to a DLQ topic (e.g., `dlq.my-group.my-topic`) so it doesn't block the rest of the queue.

```java
try (DRMQConsumer consumer = new DRMQConsumer("localhost:9092", "order-processors")) {
    consumer.connect();
    consumer.subscribe("orders");

    while (true) {
        List<DRMQConsumer.ConsumedMessage> messages = consumer.poll();
        for (DRMQConsumer.ConsumedMessage msg : messages) {
            try {
                processOrder(msg); // Your business logic
                consumer.commit("orders", msg.offset() + 1); 
            } catch (Exception e) {
                // Explicitly reject the message on failure
                boolean routedToDlq = consumer.nack("orders", msg.offset());
                if (routedToDlq) {
                    System.err.println("Poison pill routed to DLQ: " + msg.offset());
                }
            }
        }
    }
} catch (IOException e) {
    e.printStackTrace();
}
```

### Python Client (SDK)

DRMQ supports cross-language communication via raw TCP and Protocol Buffers. A Python client implementation is provided in `drmq-python-client`. 
The SDK features automatic leader failover, transparent retries, and offset auto-commit functionality identical to the Java client.

**Producer Example:**
```python
from drmq_client import DRMQProducer

producer = DRMQProducer("localhost:9092,localhost:9093")
producer.connect()
res = producer.send("python-topic", b"Hello from Python!")
print(f"Sent at offset {res.offset}")
```

**Consumer Example:**
```python
from drmq_client import DRMQConsumer

consumer = DRMQConsumer("localhost:9092,localhost:9093", group_id="python-workers")
consumer.auto_commit = True
consumer.connect()
consumer.subscribe("python-topic")

messages = consumer.poll(max_messages=10, timeout_ms=5000)
for msg in messages:
    print(f"Received: {msg.payload.decode('utf-8')}")
```

### TypeScript Client (SDK)

A native Node.js/TypeScript client is provided in `drmq-ts-client`. Like the Python SDK, it natively supports cluster failovers and leader redirects.

**Producer Example:**
```typescript
import { DRMQProducer } from './client';

const producer = new DRMQProducer("localhost:9092,localhost:9093");
await producer.connect();

const payload = Buffer.from("Hello from TypeScript!");
const res = await producer.send("ts-topic", payload);
console.log(`Sent at offset ${res.offset}`);
```

**Consumer Example:**
```typescript
import { DRMQConsumer } from './client';

const consumer = new DRMQConsumer("localhost:9092,localhost:9093", "ts-workers");
consumer.autoCommit = true;
await consumer.connect();
await consumer.subscribe("ts-topic");

const messages = await consumer.poll(10, 5000);
for (const msg of messages) {
  console.log(`Received: ${Buffer.from(msg.payload).toString('utf-8')}`);
}
```

## Interactive CLI

DRMQ provides an interactive command-line interface for both the producer and consumer. This is great for testing and debugging.

**Run the Producer CLI:**

```bash
cd drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp" -Dexec.args="localhost:9092,localhost:9093"
```

_Commands:_ `send <topic> <message>`

**Run the Consumer CLI:**

```bash
cd drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp" -Dexec.args="localhost:9092,localhost:9093 my-consumer-group"
```

Run multiple instances with the same group name in separate terminals to see messages load-balanced across consumers.

_Commands:_ `subscribe <topic> [offset]`, `seek <topic> <timestamp>`, `poll`, `stream`, `commit`, `mode group|single`, `status`

## Monitoring

The broker exposes Prometheus metrics. When integrated with a Prometheus server, you can monitor key metrics such as:

- `drmq_messages_produced_total`
- `drmq_messages_consumed_total`
- `drmq_raft_state` (Leader/Follower/Candidate)
- `drmq_log_size_bytes`
