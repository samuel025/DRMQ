#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Kafka Transactions benchmark – mirrors DRMQ AtomicStressTestApp
#
# Mirrors DRMQ atomic_stress_test.sh configuration exactly:
#   * 3-node KRaft cluster (replication factor 3, majority ACK)
#   * Each transaction writes atomically to 2 topics (txn-topic-a + txn-topic-b)
#   * 1 KB payload per topic per transaction  →  2 KB total per transaction
#   * Kafka Transactions API: beginTransaction → send×2 → commitTransaction
#   * 200,000 transactions
#   * batch.size = 1 MiB, linger.ms = 10, acks = all, max.in.flight = 1
#   * Concurrency = 1 transactional producer (serial, mirrors DRMQ's single Raft leader)
#
# Output:
#   N transactions committed, X.X TPS (Y.Y MB/sec)
#   avg / max / p50 / p95 / p99 / p999 latency
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

KAFKA_IMAGE="apache/kafka:3.7.0"
KAFKA_BIN="/opt/kafka/bin"
KAFKA_LIBS="/opt/kafka/libs"

# ─────────────────── Configuration ───────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
TOPIC_A="txn-topic-a"
TOPIC_B="txn-topic-b"
REPLICATION_FACTOR=3
PARTITIONS=1
NUM_TRANSACTIONS=200000     # total atomic transactions
CONCURRENCY=1               # transactional producers (1 = serial, matches DRMQ single leader)
MODE="separate"              # always "separate" for Kafka txns (one producer per thread)
BOOTSTRAP="localhost:9092"
TOPICS=2

# ─────────────────────────────────────────────────────────────────────────────

while getopts "b:c:n:m:t:h" opt; do
  case $opt in
    b) BOOTSTRAP="$OPTARG" ;;
    c) CONCURRENCY="$OPTARG" ;;
    n) NUM_TRANSACTIONS="$OPTARG" ;;
    m) MODE="$OPTARG" ;;
    t) TOPICS="$OPTARG" ;;
    h) echo "Usage: ./kafka_txn_benchmark.sh [-b bootstrap] [-c concurrency] [-n numTransactions] [-m mode] [-t topics]"
       echo ""
       echo "  Modes:"
       echo "    separate  (default) — one producer per thread (Kafka txns require it)"
       echo ""
       echo "  Examples:"
       echo "    ./kafka_txn_benchmark.sh                     → CONCURRENCY=1, 200K txns (serial)"
       echo "    ./kafka_txn_benchmark.sh -c 4                → 4 parallel producers"
       echo "    ./kafka_txn_benchmark.sh -c 4 -n 50000       → 4 producers, 50K txns"
       exit 0 ;;
    *) echo "Usage: ./kafka_txn_benchmark.sh [-b bootstrap] [-c concurrency] [-n numTransactions] [-m mode] [-t topics]" >&2
       exit 1 ;;
  esac
done

# ─────────────────── Start the KRaft cluster ─────────────────────────────────
start_cluster() {
  echo "Launching 3-node KRaft cluster via docker-compose..."
  docker compose -f "${COMPOSE_FILE}" up -d

  echo "Waiting for the KRaft controller to become ready (up to 2 min)..."
  for i in {1..120}; do
    if docker exec kafka1 ${KAFKA_BIN}/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "Cluster ready after ${i}s"
      echo ""
      return
    fi
    sleep 1
  done
  echo "ERROR: Cluster did not become ready in time" >&2
  exit 1
}

# ─────────────────── Create both topics ──────────────────────────────────────
create_topics() {
  for t in {0..10}; do
    TOPIC="txn-topic-$t"
    echo "Creating topic ${TOPIC}..."
    for attempt in {1..10}; do
      if docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh \
          --bootstrap-server localhost:9092 \
          --create \
          --if-not-exists \
          --replication-factor ${REPLICATION_FACTOR} \
          --partitions ${PARTITIONS} \
          --topic "${TOPIC}"; then
        break
      fi
      echo "  Attempt ${attempt} failed, retrying in 3s..."
      sleep 3
    done
  done
  echo ""
}

# ─────────────────── Extract Kafka libs & compile on host ────────────────────
# apache/kafka:3.7.0 ships a JRE only — no javac.
# Solution: copy all Kafka libs out of the image, compile with the host JDK,
# and run on the host directly (ports are mapped, so localhost:9092 works).
BENCH_DIR="${SCRIPT_DIR}/.kafka-bench"

prepare_benchmark() {
  echo "Extracting Kafka client libs from image to ${BENCH_DIR}..."
  rm -rf "${BENCH_DIR}"
  mkdir -p "${BENCH_DIR}/classes"

  # Spin up a throwaway container and docker cp the entire libs dir out
  local CID
  CID=$(docker create "${KAFKA_IMAGE}")
  docker cp "${CID}:/opt/kafka/libs/." "${BENCH_DIR}/libs/"
  docker rm "${CID}" >/dev/null

  local CP
  CP=$(find "${BENCH_DIR}/libs" -name "*.jar" | tr '\n' ':')

  echo "Compiling KafkaTransactionBenchmark.java on host (javac)..."
  javac -cp "${CP}" \
        "${SCRIPT_DIR}/KafkaTransactionBenchmark.java" \
        -d "${BENCH_DIR}/classes"

  echo "✓  Compilation complete"
  echo ""
}

# ─────────────────── Run the transaction benchmark ───────────────────────────
run_txn_benchmark() {
  echo "────────────────────────────────────────────────────────────"
  echo " Kafka Transactions Performance Test"
  echo "   Transactions : ${NUM_TRANSACTIONS}"
  echo "   Topics       : ${TOPIC_A}  +  ${TOPIC_B}"
  echo "   Payload/topic: 1 KB  (2 KB total per transaction)"
  echo "   Concurrency  : ${CONCURRENCY} transactional producer(s)"
  echo "   Producer mode: separate (one per thread — Kafka txns require it)"
  echo "   ACKs         : all (ISR quorum)"
  echo "   Bootstrap    : ${BOOTSTRAP}"
  echo "────────────────────────────────────────────────────────────"
  echo ""

  local CP
  CP="${BENCH_DIR}/classes:$(find "${BENCH_DIR}/libs" -name "*.jar" | tr '\n' ':')"

  # Run directly on the host — Kafka ports are mapped to localhost
  java -cp "${CP}" KafkaTransactionBenchmark \
    "${BOOTSTRAP}" "${NUM_TRANSACTIONS}" "${CONCURRENCY}" "${MODE}" "${TOPICS}"
}

# ─────────────────── Cleanup ──────────────────────────────────────────────────
cleanup() {
  echo ""
  echo "Shutting down KRaft cluster..."
  docker compose -f "${COMPOSE_FILE}" down -v
  rm -rf "${BENCH_DIR}"
}
trap cleanup EXIT

# ─────────────────── Main flow ────────────────────────────────────────────────
start_cluster
create_topics
prepare_benchmark
run_txn_benchmark
