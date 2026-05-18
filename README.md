# DRMQ - Distributed Reliable Message Queue

DRMQ is a fault-tolerant, high-performance distributed message broker built from first principles. It provides guaranteed message delivery, strict ordering, and high availability through the implementation of the Raft consensus algorithm for log replication and leader election.

## Overview

DRMQ is designed to be a resilient, easily understandable, and scalable message queue. It operates via a custom TCP protocol and supports both a standalone single-node mode for development or simple deployments, and a robust cluster mode for production environments requiring fault tolerance. 

The project is structured as a multi-module Maven build, separating the core broker logic, client libraries, protocol definitions, and integration tests to ensure maintainability and clear boundaries.

## Key Features

*   **Raft Consensus Integration:** Full implementation of the Raft protocol for distributed state replication, leader election, and high availability. Features **Quorum-Loss Stepdown** to detect network partitions and demote isolated leaders, preventing split-brain/ghost leadership data loss.
*   **Persistent Storage:** Custom Write-Ahead Log (WAL) and segment-based message storage ensure messages are durably persisted to disk. Features thread-safe, atomic consumer offset management with bounds locking to ensure zero data loss during concurrent background writes and shutdown gracefully.
*   **Graceful Teardown Coordination:** Orchestrated, safe termination of Netty EventLoops, RPC executors, and disk storage guaranteeing state integrity without resource leaks during node shutdowns.
*   **High Performance:** 
    *   **Follower-based Reads:** Scalable read operations allowing consumers to fetch messages from follower nodes, distributing the load across the cluster.
    *   **Dedicated Thread Pools:** Separated executor services for Raft tasks and client handling prevent thread starvation and ensure consistent performance. Independent scheduler threads prevent I/O blocking from stunting cluster heartbeats.
*   **Robust Client Ecosystem:** Includes a producer and consumer client with automatic reconnects and randomized bootstrap load balancing for seamless failover operations.
*   **Metrics & Observability:** Integrated with Micrometer and Prometheus to provide deep visibility into broker health, log replication lag, and throughput metrics.

## Architecture & Modules

The repository is divided into several Maven modules:

*   `drmq-protocol`: Defines the Protocol Buffers (protobuf) messages used for client-broker and inter-broker communication.
*   `drmq-broker`: The core server implementation containing the Raft node logic, TCP server, message storage engine, and offset management.
*   `drmq-client`: Java client library providing high-level `Producer` and `Consumer` APIs.
*   `drmq-integration-tests`
## Prerequisites

*   Java 17 or higher
*   Maven 3.8.x or higher

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

```java
try (DRMQConsumer consumer = new DRMQConsumer("localhost:9092,localhost:9093", "my-group")) {
    consumer.connect();
    consumer.setAutoCommit(true);
    consumer.subscribe("my-topic");

    while (true) {
        // Poll up to 100 messages, waiting up to 1000ms
        List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(100, 1000);
        
        for (DRMQConsumer.ConsumedMessage msg : messages) {
            System.out.printf("Received (offset %d): %s\n", msg.offset(), msg.payloadAsString());
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
*Commands:* `send <topic> <message>`

**Run the Consumer CLI:**
```bash
cd drmq-client
mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ConsumerApp" -Dexec.args="localhost:9092,localhost:9093 my-consumer-group"
```
*Commands:* `subscribe <topic>`, `poll`, `stream`, `commit`

## Monitoring

The broker exposes Prometheus metrics. When integrated with a Prometheus server, you can monitor key metrics such as:
*   `drmq_messages_produced_total`
*   `drmq_messages_consumed_total`
*   `drmq_raft_state` (Leader/Follower/Candidate)
*   `drmq_log_size_bytes`
