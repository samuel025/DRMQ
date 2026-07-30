# DRMQ Benchmark Suite Guide

This directory contains the benchmarking scripts used to evaluate DRMQ's performance against industry standards like Kafka and RabbitMQ. 

All scripts are designed to provide **apples-to-apples** comparisons by standardising the following parameters wherever possible:
- 3-node clusters (Raft/KRaft/Quorum Queues)
- Replication Factor: 3
- ACKs: `all` (majority quorum)
- Payload Size: 1 KB per record
- Batch Size: 1 MiB
- Linger: 10 ms

---

## 1. DRMQ Native Benchmarks

These scripts compile the DRMQ broker and client locally (using Maven) and run the cluster as background Java processes.

### `stress_test.sh`
Evaluates standard, single-topic throughput for DRMQ.

**Usage:** `./stress_test.sh [options]`

**Parameters:**
- `-c <concurrency>`: Number of threads feeding the producer (default: `1`).
- `-n <numRecords>`: Total number of records to send (default: `200000`).
- `-s <recordSize>`: Size of the payload in bytes (default: `1024`).
- `-m <mode>`: Producer mode.
  - `shared` (default): A single producer instance shared among threads.
  - `separate`: One independent producer instance per thread.
- `-b <brokers>`: Bootstrap server addresses (default: `localhost:9092,localhost:9093,localhost:9094`).

**Example:**
```bash
./stress_test.sh -c 4 -n 500000 -m separate
```

### `atomic_stress_test.sh`
Evaluates DRMQ's unique multi-topic atomic transaction performance.

**Usage:** `./atomic_stress_test.sh [options]`

**Parameters:**
- `-c <concurrency>`: Number of threads issuing transactions (default: `10`).
- `-n <numTransactions>`: Total atomic transactions to execute (default: `200000`).
- `-t <topics>`: Number of topics to write to atomically per transaction (default: `2`).
- `-i <inFlight>`: Maximum in-flight transactions in the accumulator (default: `5000` to maximize batching).
- `-m <mode>`: Producer mode (`shared` or `separate`).
- `-b <brokers>`: Bootstrap server addresses.

**Example:**
```bash
# Test 5 topics per transaction, batched
./atomic_stress_test.sh -t 5 -c 10 -i 5000

# Test serial execution (matches Kafka 2PC behavior)
./atomic_stress_test.sh -c 1 -i 1
```

---

## 2. Kafka Benchmarks

These scripts spin up a 3-node KRaft Kafka cluster via Docker (`docker-compose.yml`), extract the Kafka client tools, and run the equivalent benchmarks.

### `kafka_benchmark.sh`
Evaluates Kafka's standard single-topic throughput.

**Usage:** `./kafka_benchmark.sh [options]`

**Parameters:**
- `-c <concurrency>`: Number of parallel `kafka-producer-perf-test` containers (default: `1`).
- `-n <numRecords>`: Total records to send (default: `200000`).
- `-m <mode>`: Container mode.
  - `shared` (default): Single producer container.
  - `separate`: One container per concurrency count.
- `-b <bootstrap>`: Bootstrap server (default: `localhost:9092`).

**Example:**
```bash
./kafka_benchmark.sh -c 4 -m separate -n 500000
```

### `kafka_txn_benchmark.sh`
Evaluates Kafka's Transaction API (Two-Phase Commit) for multi-topic atomic writes.

**Usage:** `./kafka_txn_benchmark.sh [options]`

**Parameters:**
- `-c <concurrency>`: Number of transactional producers (default: `1`, which is required for a fair comparison against DRMQ's serial single-leader model).
- `-n <numTransactions>`: Total transactions to execute (default: `200000`).
- `-t <topics>`: Number of topics written per transaction (default: `2`).
- `-m <mode>`: Mode (must be `separate` for Kafka txns).

**Example:**
```bash
./kafka_txn_benchmark.sh -c 1 -t 2 -n 1000
```

---

## 3. Containerised Benchmarks

### `drmq_docker_benchmark.sh`
Runs DRMQ as a 3-node cluster inside Docker (`docker-compose-drmq.yml`) to provide an exact environment match against the Kafka/RabbitMQ docker benchmarks.

**Usage:** `./drmq_docker_benchmark.sh [options]`

**Parameters:**
- `-c <concurrency>`: Number of parallel JVM producers (default: `4`).
- `-n <numRecords>`: Total number of records (default: `500000`).
- `-b <bootstrap>`: Bootstrap addresses.

**Example:**
```bash
./drmq_docker_benchmark.sh -c 8 -n 1000000
```

### `rabbitmq_benchmark.sh`
Spins up a 3-node RabbitMQ cluster (`.docker-compose.rabbitmq.yml`) and evaluates throughput using Quorum Queues and Batch Confirms.

**Usage:** Just run the script. It automatically compiles the Java benchmark and runs it for 200,000 messages.

**Example:**
```bash
./rabbitmq_benchmark.sh
```
