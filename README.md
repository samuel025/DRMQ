# DRMQ - Distributed Reliable Message Queue

**Official Documentation:** [https://drmq.vercel.app](https://drmq.vercel.app)

DRMQ is a fault-tolerant, consensus-backed distributed message queue built from first principles. Unlike high-throughput, partition-centric message systems (such as Apache Kafka) designed primarily for massive raw streaming ingest, **DRMQ prioritizes strict cross-topic atomicity, linear consensus consistency, and zero-coordinator transactional safety**. By unifying message logs, consumer offset state, and multi-topic writes under a single Raft consensus engine, DRMQ guarantees that cross-topic operations commit or fail as a single atomic unit without the latency and failure modes of external two-phase commit (2PC) coordinators.

## Core Design Philosophy & Architectural Positioning

Modern distributed message queues usually trade atomic guarantees for extreme write throughput:

* **High-Throughput Systems (e.g., Apache Kafka)**: Scale throughput horizontally by distributing independent topic partitions across distinct broker nodes. However, cross-topic atomic writes require complex, two-phase commit (2PC) transaction coordinators, transaction markers, and background state topics (`__transaction_state`). This introduces coordinator failure modes, partial commit vulnerability windows, and significant operational complexity.
* **DRMQ (Atomicity & Reliability First)**: Designed specifically for mission-critical transactional workloads (such as financial payment flows, order processing pipelines, and audit logs) where **a partial commit across topics is catastrophic**. In DRMQ, cross-topic atomic batches are proposed and committed directly into the core Raft log as a single, indivisible entry. Either **all** messages across all requested topics are durably replicated and committed, or **none** are.

### Key Architectural Trade-Offs

| Architectural Dimension | **Apache Kafka** | **DRMQ** |
| :--- | :--- | :--- |
| **Primary Design Goal** | Multi-million msg/sec horizontal scale | **Strict Cross-Topic Atomicity & Zero-2PC Reliability** |
| **Cross-Topic Transactions** | Heavy 2PC Coordinator + Transaction Markers | **Native Single-Raft Atomic Log Commit** |
| **Offset & Group State** | Separate internal `__consumer_offsets` log | **Unified Raft Consensus State Machine** |
| **Durability Discipline** | Deferred OS page-cache flushes (by default) | **Synchronous `fsync` by default (Configurable to OS Page Cache)** |
| **Target Use Cases** | Event streaming, metrics, log aggregation | Financial transactions, order pipelines, audit ledgers |

---

## Overview

DRMQ operates via a custom high-performance TCP protocol and supports both a standalone single-node mode for development and a robust cluster mode for production environments requiring fault tolerance.

The project is structured as a multi-module Maven build, separating the core broker logic, client libraries, protocol definitions, and integration tests to ensure maintainability and clear boundaries.

## Key Features

- **Scalable Consumer Groups:** Scale your consumers dynamically without the complexity of partitions. Simply start multiple consumers with the same group name, and the broker will automatically distribute messages among them. Messages are load-balanced across consumers in a group with at-least-once delivery; a lease-based protocol ensures that uncommitted messages are redelivered if a consumer fails. Need to replay or read specific messages? Switch to single mode for full manual offset control.
- **Cross-Topic Atomic Transactions:** Produce messages to multiple distinct topics in a single, atomic operation. Guaranteed to commit or fail as a single unit at the Raft consensus level, avoiding the overhead of external two-phase commit coordinators (like Kafka's transaction API). Supported natively in Java, Python, and TypeScript clients.
- **Advanced Raft Consensus:** Full implementation of the Raft protocol with robust stability extensions:
  - **Pre-Vote:** Prevents returning partitioned followers with artificially inflated terms from disrupting a healthy leader.
  - **Quorum-Loss Stepdown:** Detects network partitions and immediately demotes isolated leaders, preventing split-brain scenarios and ensuring clients aren't writing to dead-end nodes.
- **Incremental State Reconstruction:** Instead of relying on monolithic snapshots that pause the cluster, DRMQ seamlessly catches up lagging followers using bounded, per-topic segment transfers. This ensures rapid recovery without OOM errors.
- **Dead-Letter Queues (DLQ):** Gracefully handle poison pill messages. Consumers can explicitly `nack()` unprocessable messages. After a configurable threshold of delivery failures, the broker automatically routes the message to an isolated DLQ topic and advances the consumer group, preventing blockages.
- **Persistent Storage:** Custom Write-Ahead Log (WAL) and segment-based message storage ensure messages are durably persisted to disk. Features thread-safe, atomic consumer offset management with `.atomic-intent` crash safety designed to minimize data loss during concurrent background writes.
- **InfinityLog (Tiered Storage):** Seamlessly archive old log segments to Amazon S3 (or MinIO) to decouple storage costs from compute. Consumers requesting historical offsets transparently trigger the broker to download and resolve missing segments from the cloud, with built-in pagination and atomic concurrent recovery.
- **Time-based Message Lookup:** Clients can precisely rewind consumers to the earliest message at or after a specific UNIX timestamp (`seekByTime`), enabling accurate historical replay without knowing exact offsets.
- **High Performance:**
  - **Client-Side Batching:** Producers feature high-throughput, latency-optimized message batching via a configurable `linger.ms` window. This groups thousands of messages into a single network round-trip and Raft log flush.
  - **Configurable Disk Durability:** By default, DRMQ guarantees strict flush-before-ack durability (`fsync`). However, administrators can explicitly disable this for extreme throughput scenarios where hardware page-cache flushing is acceptable.
  - **Follower-based Reads:** Scalable read operations allowing consumers to fetch messages from follower nodes, distributing the load across the cluster.
- **Robust Client Ecosystem:** Includes Java, Python, and TypeScript SDKs featuring automatic reconnects, randomized bootstrap load balancing, typed Error Code handling, and seamless leader failovers.
- **Real-Time Telemetry Dashboard:** Integrated React/Vite dashboard connecting to the broker via WebSockets, providing real-time metrics on Raft status, throughput, offset lag, and system health.

## Architecture & Modules

The repository is divided into several Maven modules:

- `drmq-protocol`: Defines the Protocol Buffers (protobuf) messages used for client-broker and inter-broker communication.
- `drmq-broker`: The core server implementation containing the Raft node logic, TCP server, message storage engine, and offset management.
- `drmq-client`: Java client library providing high-level `Producer` and `Consumer` APIs.
- `drmq-integration-tests`: Rigorous end-to-end benchmark and latency testing suites.
- `drmq-dashboard`: React/Vite web application for real-time cluster telemetry visualization.

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

# InfinityLog (Tiered Storage) Configuration
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

### Java Producer (Standard & Atomic Batch)

```java
try (DRMQProducer producer = new DRMQProducer("localhost:9092,localhost:9093")) {
    producer.connect();
    
    // 1. Standard Produce (Buffered automatically by linger.ms)
    CompletableFuture<DRMQProducer.SendResult> future = producer.send("my-topic", "Hello, DRMQ!");
    future.thenAccept(res -> System.out.println("Sent at offset: " + res.getOffset()));
    
    // 2. Cross-Topic Atomic Produce
    Map<String, byte[]> atomicBatch = new HashMap<>();
    atomicBatch.put("orders", "order-123".getBytes());
    atomicBatch.put("inventory", "reserve-sku-456".getBytes());
    
    CompletableFuture<Map<String, Long>> atomicFuture = producer.sendAtomic(atomicBatch);
    atomicFuture.thenAccept(offsets -> System.out.println("Atomic commit successful! Offsets: " + offsets));

} catch (IOException e) {
    e.printStackTrace();
}
```

### Java Consumer

DRMQ supports **Group Mode** (load-balanced) and **Single Consumer Mode** (manual offset control).

```java
// Group Mode Example (Automatic Load Balancing)
DRMQConsumer c1 = new DRMQConsumer("localhost:9092,localhost:9093", "order-processors");
c1.setAutoCommit(true);
c1.connect();
c1.subscribe("orders"); // Broker assigns offsets automatically

// Consumer 2 (running on another machine/thread)
DRMQConsumer c2 = new DRMQConsumer("localhost:9092,localhost:9093", "order-processors");
c2.setAutoCommit(true);
c2.connect();
c2.subscribe("orders");

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

The Python client features automatic leader failover, pipelined batching, cross-topic atomicity, and offset auto-commit functionality.

**Producer & Atomic Example:**
```python
from drmq_client import DRMQProducer

producer = DRMQProducer("localhost:9092,localhost:9093")
producer.connect()

# Standard
res = producer.send("python-topic", b"Hello from Python!").result()

# Cross-Topic Atomic
batch = {
    "topic-A": b"Event A",
    "topic-B": b"Event B"
}
offsets = producer.send_atomic(batch).result()
print(f"Atomic commit successful: {offsets}")
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

A native Node.js/TypeScript client natively supporting cluster failovers and leader redirects.

**Producer & Atomic Example:**
```typescript
import { DRMQProducer } from './client';

const producer = new DRMQProducer("localhost:9092,localhost:9093");
await producer.connect();

// Standard
await producer.send("ts-topic", Buffer.from("Hello from TypeScript!"));

// Cross-Topic Atomic
const offsets = await producer.sendAtomic({
  "topic-A": Buffer.from("Event A"),
  "topic-B": Buffer.from("Event B")
});
console.log("Atomic success:", offsets);
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
_Commands:_ `subscribe <topic> [offset]`, `seek <topic> <timestamp>`, `poll`, `stream`, `commit`, `mode group|single`, `status`

## Monitoring

The broker exposes Prometheus metrics and real-time WebSocket telemetry. When integrated with a Prometheus server, you can monitor key metrics such as:

- `drmq_messages_produced_total`
- `drmq_messages_consumed_total`
- `drmq_raft_state` (Leader/Follower/Candidate)
- `drmq_log_size_bytes`

## Benchmarks Reproducibility

DRMQ was designed to aggressively optimize atomic multi-topic transactions by bypassing the traditional Two-Phase Commit (2PC) coordinator. 

To view the raw performance data, load-testing methodology, and instructions on how to perfectly replicate the comparative experiments (DRMQ vs. Apache Kafka), please see the exact [Figures Reproducibility Guide](benchmarks/THESIS_REPRODUCIBILITY.md).
