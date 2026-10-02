# DRMQ — Distributed Reliable Message Queue

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
- **Cross-Topic Atomic Transactions:** Produce messages to multiple distinct topics in a single, atomic operation. Guaranteed to commit or fail as a single unit at the Raft consensus level, avoiding the overhead of external two-phase commit coordinators (like Kafka's transaction API). Supported natively in Java, Go, Python, and TypeScript clients.
- **Advanced Raft Consensus:** Full implementation of the Raft protocol with robust stability extensions:
  - **Pre-Vote:** Prevents returning partitioned followers with artificially inflated terms from disrupting a healthy leader.
  - **Quorum-Loss Stepdown:** Detects network partitions and immediately demotes isolated leaders, preventing split-brain scenarios and ensuring clients aren't writing to dead-end nodes.
- **Incremental State Reconstruction:** Instead of relying on monolithic snapshots that pause the cluster, DRMQ seamlessly catches up lagging followers using bounded, per-topic segment transfers. This ensures rapid recovery without OOM errors.
- **Dead-Letter Queues (DLQ):** Gracefully handle poison pill messages. Consumers can explicitly `nack()` unprocessable messages. After a configurable threshold of delivery failures, the broker automatically routes the message to an isolated DLQ topic and advances the consumer group, preventing blockages.
- **Persistent Storage:** Custom Write-Ahead Log (WAL) and segment-based message storage ensure messages are durably persisted to disk. Features thread-safe, atomic consumer offset management and strictly isolated cross-topic batching with `.atomic-done` post-write completion markers to prevent data loss or partial visibility during crashes.
- **InfinityLog (Tiered Storage):** Seamlessly archive old log segments to Amazon S3 (or MinIO) to decouple storage costs from compute. Consumers requesting historical offsets transparently trigger the broker to download and resolve missing segments from the cloud, with built-in pagination and atomic concurrent recovery.
- **Time-based Message Lookup:** Clients can precisely rewind consumers to the earliest message at or after a specific UNIX timestamp (`seekByTime`), enabling accurate historical replay without knowing exact offsets.
- **High Performance:**
  - **Client-Side Batching:** Producers feature high-throughput, latency-optimized message batching via a configurable `linger.ms` window. This groups thousands of messages into a single network round-trip and Raft log flush.
  - **Configurable Disk Durability:** By default, DRMQ guarantees strict flush-before-ack durability (`fsync`). However, administrators can explicitly disable this for extreme throughput scenarios where hardware page-cache flushing is acceptable.
  - **Follower-based Reads:** Scalable read operations allowing consumers to fetch messages from follower nodes, distributing the load across the cluster.
- **Robust Client Ecosystem:** Includes Java, Go, Python, and TypeScript SDKs featuring automatic reconnects, randomized bootstrap load balancing, typed Error Code handling, and seamless leader failovers.
- **Real-Time Telemetry Dashboard:** Integrated React/Vite dashboard connecting to the broker via WebSockets, providing real-time metrics on Raft status, throughput, offset lag, and system health.

## Architecture & Modules

The repository is divided into several Maven modules:

- `drmq-protocol`: Defines the Protocol Buffers (protobuf) messages used for client-broker and inter-broker communication.
- `drmq-broker`: The core server implementation containing the Raft node logic, TCP server, message storage engine, and offset management.
- `drmq-client`: Java client library providing high-level `Producer` and `Consumer` APIs.
- `drmq-integration-tests`: Rigorous end-to-end benchmark and latency testing suites.
- `drmq-dashboard`: React/Vite web application for real-time cluster telemetry visualization.

---

## Docker (Recommended)

The easiest way to run DRMQ is via Docker. Official images are published to Docker Hub:

| Image | Description |
| :--- | :--- |
| `0xuell/drmq:latest` | The broker (JRE 21 Alpine, ~120 MB) |
| `0xuell/drmq-dashboard:latest` | The telemetry dashboard (Nginx Alpine, ~30 MB) |

### Port Reference

Each broker exposes four ports derived from a single base `PORT`:

| Port | Formula | Default | Purpose |
| :--- | :--- | :--- | :--- |
| `PORT` | base | `9092` | Client TCP connections & inter-broker Raft RPC |
| `PORT + 200` | `wsPort` | `9292` | WebSocket telemetry (dashboard) |
| `PORT + 300` | `adminPort` | `9392` | Admin HTTP API |
| `METRICS_PORT` | independent | `9096` | Prometheus `/metrics` endpoint |

> In cluster mode each node uses the **same internal ports** (`9092`, `9292`, etc.) and Docker maps them to different host ports (`9092`, `9093`, `9094` …). Inter-broker Raft traffic always uses the **container-internal hostname** (e.g. `drmq-1`, `drmq-2`) so peer addresses never depend on host port offsets.

### Quick Start — Standalone Broker

```bash
docker run -d \
  --name drmq \
  -p 9092:9092 \
  -p 9096:9096 \
  -p 9292:9292 \
  -p 9392:9392 \
  -v drmq-data:/data \
  -e NODE_ID=standalone \
  -e PORT=9092 \
  0xuell/drmq:latest
```

### Quick Start — Docker Compose (Standalone + Dashboard)

```bash
docker compose up -d
```

The included [`docker-compose.yml`](docker-compose.yml) starts one broker and the telemetry dashboard. Dashboard opens at **http://localhost:8088** and auto-connects to the broker's WebSocket telemetry port.

### Quick Start — 3-Node Raft Cluster

```bash
docker compose -f docker-compose.cluster.yml up -d
```

The included [`docker-compose.cluster.yml`](docker-compose.cluster.yml) starts a fault-tolerant 3-node Raft cluster. Clients connect to any of the exposed broker ports and are automatically redirected to the current leader:

| Node | Client Port | Metrics Port | Dashboard WS Port |
| :--- | :--- | :--- | :--- |
| `drmq-1` | `9092` | `9096` | `9292` |
| `drmq-2` | `9093` | `9097` | `9293` |
| `drmq-3` | `9094` | `9098` | `9294` |

Dashboard opens at **http://localhost:8088** and fans out to all three broker WebSocket endpoints simultaneously, merging their telemetry into a single cluster view.

### Broker Environment Variables

All broker settings are configured via environment variables, which the `docker-entrypoint.sh` translates to CLI flags:

| Variable | CLI Flag | Default | Description |
| :--- | :--- | :--- | :--- |
| `NODE_ID` | `--node-id` | `standalone` | Unique node identifier |
| `PORT` | `--port` | `9092` | Base TCP port for client connections & Raft |
| `ADVERTISED_HOST` | `--host` | container hostname | Hostname advertised to peers (set for multi-host deployments) |
| `WS_PORT` | `--ws-port` | `PORT + 200` | Override WebSocket telemetry port |
| `DATA_DIR` | `--data-dir` | `/data` | Persistent data directory |
| `PEERS` | `--peers` | _(none)_ | Peer list: `id:host:port,...` — e.g. `2:drmq-2:9092,3:drmq-3:9092` |
| `METRICS_ENABLED` | `--metrics-enabled` | `true` | Enable Prometheus metrics endpoint |
| `METRICS_PORT` | `--metrics-port` | `9096` | Prometheus metrics port |
| `METRICS_PATH` | `--metrics-path` | `/metrics` | Prometheus metrics path |
| `RAFT_FSYNC_ENABLED` | `--raft-fsync-enabled` | `true` | Fsync Raft WAL before acknowledging |
| `LOG_SEGMENT_FSYNC` | `--log-segment-fsync` | `false` | Fsync message log segments |
| `RAFT_COMPACT_THRESHOLD` | `--raft-compact-threshold` | `1000` | Log compaction threshold (entries) |
| `MAX_DELIVERIES` | `--max-deliveries` | `5` | Max delivery attempts before DLQ routing |
| `DLQ_TOPIC_PREFIX` | `--dlq-topic-prefix` | `dlq.` | Dead-letter queue topic prefix |
| `S3_ARCHIVE_BUCKET` | `--s3-archive-bucket` | _(none)_ | S3/MinIO bucket for tiered storage |
| `S3_ARCHIVE_REGION` | `--s3-archive-region` | `us-east-1` | AWS region for tiered storage |
| `S3_ARCHIVE_ENDPOINT` | `--s3-archive-endpoint` | _(none)_ | Custom endpoint URL (for MinIO) |
| `JAVA_OPTS` | _(JVM flags)_ | `-Xms256m -Xmx2g -XX:+UseG1GC` | JVM tuning flags |

### Dashboard — Broker URL Configuration

The dashboard resolves broker WebSocket URLs in this priority order (highest first):

1. **`?ws=` query parameter** — `http://localhost:8088?ws=ws://broker1:9292,ws://broker2:9293`  
   Persisted to `localStorage` for subsequent page loads.
2. **`localStorage`** — Automatically populated from the last `?ws=` value.
3. **`VITE_WEBSOCKET_URLS` build env var** — Baked in at image build time.
4. **Auto-detect** — Falls back to `ws://{page hostname}:9292,ws://{page hostname}:9293,ws://{page hostname}:9294`.

To bake in a broker URL at build time:

```bash
docker build \
  --build-arg VITE_WEBSOCKET_URLS=ws://my-broker:9292 \
  -t my-org/drmq-dashboard:latest \
  ./drmq-dashboard
```

Or point the running dashboard at a custom broker at runtime:

```
http://localhost:8088?ws=ws://my-broker-host:9292
```

### Multi-Host Deployment

For brokers running on different physical servers, set `ADVERTISED_HOST` to each node's externally reachable hostname or IP, and list the external addresses in `PEERS`:

```yaml
services:
  drmq-1:
    image: 0xuell/drmq:latest
    ports:
      - "9092:9092"
      - "9096:9096"
      - "9292:9292"
    environment:
      NODE_ID: "1"
      PORT: "9092"
      ADVERTISED_HOST: "10.0.1.10"          # this machine's IP
      PEERS: "2:10.0.1.11:9092,3:10.0.1.12:9092"
```

Then point the dashboard at all three hosts:

```
http://dashboard-host:8088?ws=ws://10.0.1.10:9292,ws://10.0.1.11:9292,ws://10.0.1.12:9292
```

### Building & Publishing Images

```bash
# Broker — multi-arch (amd64 + arm64)
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t youruser/drmq:latest \
  --push .

# Dashboard
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t youruser/drmq-dashboard:latest \
  --push ./drmq-dashboard
```

Set `DRMQ_IMAGE` and `DASHBOARD_IMAGE` env vars to point the compose files at your registry:

```bash
DRMQ_IMAGE=youruser/drmq:latest \
DASHBOARD_IMAGE=youruser/drmq-dashboard:latest \
docker compose up -d
```

---

## Building From Source

### Prerequisites

- Java 17 or higher
- Maven 3.8.x or higher

To compile the project, generate the protobuf classes, and build the artifacts:

```bash
mvn clean install
```

## Running the Broker (Without Docker)

### Single-Node Mode

```bash
./mvnw -pl drmq-broker exec:java -Dexec.args="--port 9092 --data-dir ./data-1"
```

### Cluster Mode

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

---

## Usage Examples

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

#### Single Consumer Mode (Manual Offset Control & Replay)

If you need strict control over what messages you read—for example, if you want to replay messages from the beginning or start from a specific offset—you can disable group mode.

```java
try (DRMQConsumer consumer = new DRMQConsumer("localhost:9092", "my-group")) {
    consumer.setGroupMode(false); // Disable broker coordination
    consumer.connect();
    
    consumer.subscribe("my-topic", 0); // Start from offset 0

    // Alternatively, seek by time (timestamp in milliseconds)
    // consumer.seekByTime("my-topic", System.currentTimeMillis() - 3600000); // Replay last hour

    while (true) {
        List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(100, 1000);
        for (DRMQConsumer.ConsumedMessage msg : messages) {
            System.out.printf("Replaying (offset %d): %s\n", msg.offset(), msg.payloadAsString());
        }
        if (!messages.isEmpty()) {
            long lastOffset = messages.get(messages.size() - 1).offset();
            consumer.commit("my-topic", lastOffset + 1);
        }
    }
} catch (IOException e) {
    e.printStackTrace();
}
```

#### Dead-Letter Queues (DLQ) & Explicit NACK

If a consumer encounters a "poison pill", it can explicitly reject it using `nack()`. After a configurable threshold of delivery failures (default 5), the broker automatically routes the message to a DLQ topic (e.g., `dlq.my-group.my-topic`).

```java
try (DRMQConsumer consumer = new DRMQConsumer("localhost:9092", "order-processors")) {
    consumer.connect();
    consumer.subscribe("orders");

    while (true) {
        List<DRMQConsumer.ConsumedMessage> messages = consumer.poll();
        for (DRMQConsumer.ConsumedMessage msg : messages) {
            try {
                processOrder(msg);
                consumer.commit("orders", msg.offset() + 1); 
            } catch (Exception e) {
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

```python
from drmq_client import DRMQProducer, DRMQConsumer

# Producer
producer = DRMQProducer("localhost:9092,localhost:9093")
producer.connect()

res = producer.send("python-topic", b"Hello from Python!").result()

# Cross-Topic Atomic
offsets = producer.send_atomic({"topic-A": b"Event A", "topic-B": b"Event B"}).result()
print(f"Atomic commit successful: {offsets}")

# Consumer
consumer = DRMQConsumer("localhost:9092,localhost:9093", group_id="python-workers")
consumer.auto_commit = True
consumer.connect()
consumer.subscribe("python-topic")

messages = consumer.poll(max_messages=10, timeout_ms=5000)
for msg in messages:
    print(f"Received: {msg.payload.decode('utf-8')}")
```

### TypeScript Client (SDK)

```typescript
import { DRMQProducer, DRMQConsumer } from './client';

// Producer
const producer = new DRMQProducer("localhost:9092,localhost:9093");
await producer.connect();

await producer.send("ts-topic", Buffer.from("Hello from TypeScript!"));

const offsets = await producer.sendAtomic({
  "topic-A": Buffer.from("Event A"),
  "topic-B": Buffer.from("Event B")
});
console.log("Atomic success:", offsets);

// Consumer
const consumer = new DRMQConsumer("localhost:9092,localhost:9093", "ts-workers");
consumer.autoCommit = true;
await consumer.connect();
await consumer.subscribe("ts-topic");

const messages = await consumer.poll(10, 5000);
for (const msg of messages) {
  console.log(`Received: ${Buffer.from(msg.payload).toString('utf-8')}`);
}
```

### Go Client (SDK)

```go
package main

import (
    "fmt"
    "log"
    "time"
    drmq "github.com/samuel025/DRMQ/drmq-go-client"
)

// Producer
producer, err := drmq.NewProducer(drmq.ProducerConfig{
    BootstrapServers: "localhost:9092,localhost:9093",
})
if err != nil { log.Fatal(err) }
defer producer.Close()
producer.Connect()

future := producer.SendString("go-topic", "Hello from Go!")
res, _ := future.GetWithTimeout(5 * time.Second)
fmt.Printf("Sent at offset: %d\n", res.Offset)

atomicFuture := producer.SendAtomic(map[string][]byte{
    "topic-A": []byte("Event A"),
    "topic-B": []byte("Event B"),
})
offsets, _ := atomicFuture.GetWithTimeout(5 * time.Second)
fmt.Println("Atomic commit offsets:", offsets)

// Consumer
consumer, err := drmq.NewConsumer(drmq.ConsumerConfig{
    BootstrapServers: "localhost:9092,localhost:9093",
    ConsumerGroup:    "go-workers",
    AutoCommit:       true,
})
if err != nil { log.Fatal(err) }
defer consumer.Close()
consumer.Connect()
consumer.Subscribe("go-topic")

for {
    messages, err := consumer.PollWithOptions(10, 5000)
    if err != nil { continue }
    for _, msg := range messages {
        fmt.Printf("Received: %s\n", msg.PayloadAsString())
    }
}
```

---

## Interactive CLI

DRMQ provides an interactive command-line interface for both the producer and consumer.

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

---

## Monitoring

The broker exposes Prometheus metrics and real-time WebSocket telemetry at:

- **Metrics HTTP**: `http://<host>:<METRICS_PORT><METRICS_PATH>` (default: `http://localhost:9096/metrics`)
- **WebSocket Telemetry**: `ws://<host>:<PORT+200>` (default: `ws://localhost:9292`)

Key Prometheus metrics:

- `drmq_messages_produced_total`
- `drmq_messages_consumed_total`
- `drmq_raft_state` (Leader/Follower/Candidate)
- `drmq_log_size_bytes`

---

## Benchmarks Reproducibility

DRMQ was designed to aggressively optimize atomic multi-topic transactions by bypassing the traditional Two-Phase Commit (2PC) coordinator.

To view the raw performance data, load-testing methodology, and instructions on how to perfectly replicate the comparative experiments (DRMQ vs. Apache Kafka), please see the exact [Figures Reproducibility Guide](benchmarks/THESIS_REPRODUCIBILITY.md).
