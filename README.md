# DRMQ - Distributed Reliable Message Queue

DRMQ is a fault-tolerant, high-performance distributed message broker built from first principles. It provides guaranteed message delivery, strict ordering, and high availability through the implementation of the Raft consensus algorithm for log replication and leader election. DRMQ supports scalable consumption via multi-consumer groups — multiple consumers can share a group to load-balance message processing without partitions.

## Overview

DRMQ is designed to be a resilient, easily understandable, and scalable message queue. It operates via a custom TCP protocol and supports both a standalone single-node mode for development or simple deployments, and a robust cluster mode for production environments requiring fault tolerance.

The project is structured as a multi-module Maven build, separating the core broker logic, client libraries, protocol definitions, and integration tests to ensure maintainability and clear boundaries.

## Key Features

- **Scalable Consumer Groups:** Scale your consumers dynamically without the complexity of partitions. Simply start multiple consumers with the same group name, and the broker will automatically distribute messages among them. Each message goes to exactly one consumer in the group. Need to replay or read specific messages? Switch to single mode for full manual offset control.
- **Raft Consensus Integration:** Full implementation of the Raft protocol for distributed state replication, leader election, and high availability. Features **Quorum-Loss Stepdown** to detect network partitions and demote isolated leaders, preventing split-brain/ghost leadership data loss.
- **Persistent Storage:** Custom Write-Ahead Log (WAL) and segment-based message storage ensure messages are durably persisted to disk. Features thread-safe, atomic consumer offset management with bounds locking designed to minimize data loss during concurrent background writes and handle shutdowns gracefully.
- **Graceful Teardown Coordination:** Orchestrated, safe termination of Netty EventLoops, RPC executors, and disk storage guaranteeing state integrity without resource leaks during node shutdowns.
- **High Performance:**
  - **Follower-based Reads:** Scalable read operations allowing consumers to fetch messages from follower nodes, distributing the load across the cluster.
  - **Dedicated Thread Pools:** Separated executor services for Raft tasks and client handling prevent thread starvation and ensure consistent performance. Independent scheduler threads prevent I/O blocking from stunting cluster heartbeats.
- **Robust Client Ecosystem:** Includes a producer and consumer client with automatic reconnects and randomized bootstrap load balancing for seamless failover operations.
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
java -jar drmq-broker/target/drmq-broker-1.0.0-SNAPSHOT.jar --port 9092 --data-dir ./data-1
```

### Cluster Mode

To run a fault-tolerant cluster, you must start multiple broker instances and provide them with the addresses of their peers.

**Node 1:**

```bash
java -jar drmq-broker/target/drmq-broker-1.0.0-SNAPSHOT.jar --node-id 1 --port 9092 --data-dir ./data-1 --peers 2:localhost:9093,3:localhost:9094
```

**Node 2:**

```bash
java -jar drmq-broker/target/drmq-broker-1.0.0-SNAPSHOT.jar --node-id 2 --port 9093 --data-dir ./data-2 --peers 1:localhost:9092,3:localhost:9094
```

**Node 3:**

```bash
java -jar drmq-broker/target/drmq-broker-1.0.0-SNAPSHOT.jar --node-id 3 --port 9094 --data-dir ./data-3 --peers 1:localhost:9092,2:localhost:9093
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

By default, the broker coordinates message delivery. If you start multiple consumers with the same group name, the broker will automatically split the workload among them. Each message is delivered to exactly one consumer in the group. No partitions required!

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

_Commands:_ `subscribe <topic>`, `poll`, `stream`, `commit`, `mode group|single`, `status`

## Monitoring

The broker exposes Prometheus metrics. When integrated with a Prometheus server, you can monitor key metrics such as:

- `drmq_messages_produced_total`
- `drmq_messages_consumed_total`
- `drmq_raft_state` (Leader/Follower/Candidate)
- `drmq_log_size_bytes`
