#!/usr/bin/env bash
# ------------------------------------------------------------
# Kafka benchmark script – Docker KRaft cluster (disk‑friendly)
# Mirrors DRMQ StressTestApp / AtomicStressTestApp configuration:
#   * 3‑node KRaft cluster via apache/kafka:3.7.0 (replication factor 3, majority ACK)
#   * Single producer to a single topic
#   * batch.size = 64KB (65536 bytes)
#   * linger.ms = 1 ms (flush quickly)
#   * max.in.flight.requests.per.connection = 1 (no inflight)
#   * 1KB payload per record (matches AtomicStressTestApp)
#
# IMPORTANT: the producer test is run from a throwaway container on
# --network host, NOT via `docker exec kafka1 ...`. Each broker
# advertises itself as localhost:9092/9094/9096 (correct for host
# clients using the mapped ports). If the producer ran inside
# kafka1's own network namespace instead, metadata pointing it at
# kafka2/kafka3 (e.g. localhost:9094) would resolve to kafka1's own
# loopback, where nothing is listening on those ports — connections
# to any broker other than kafka1 would fail. Running the perf-test
# container on the host network avoids this entirely.
# ------------------------------------------------------------
set -euo pipefail

# apache/kafka:3.7.0 puts scripts in /opt/kafka/bin
KAFKA_IMAGE="apache/kafka:3.7.0"
KAFKA_BIN="/opt/kafka/bin"

# ------------------- Configuration -------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
TOPIC_NAME="benchmark-topic"
REPLICATION_FACTOR=3
PARTITIONS=1
BATCH_SIZE=1048576   
RECORD_SIZE=1024   # 1 KB payload per record
NUM_RECORDS=200000 # ~200k records = 200 MiB raw data
LINGER_MS=10
ACKS="all"
IN_FLIGHT=1
CONCURRENCY=1
MODE="shared"
BOOTSTRAP="localhost:9092"
# ----------------------------------------------------

# ----------- Start the Docker‑KRaft cluster ----------
start_cluster() {
  echo "Launching 3-node KRaft cluster via docker-compose..."
  docker compose -f "${COMPOSE_FILE}" up -d

  echo "Waiting for the KRaft controller to become ready (up to 2 min)..."
  for i in {1..120}; do
    if docker exec kafka1 ${KAFKA_BIN}/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "Cluster ready after ${i}s"
      return
    fi
    sleep 1
  done
  echo "ERROR: Cluster did not become ready in time" >&2
  exit 1
}

# ------------------- Create the topic -------------------
create_topic() {
  echo "Creating topic ${TOPIC_NAME} ..."
  # Retry a few times in case kafka2/kafka3 haven't fully joined the
  # quorum yet even though kafka1 is answering API-versions requests.
  local attempt
  for attempt in {1..10}; do
    if docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create \
        --if-not-exists \
        --replication-factor ${REPLICATION_FACTOR} \
        --partitions ${PARTITIONS} \
        --topic ${TOPIC_NAME}; then
      return
    fi
    echo "  topic creation attempt ${attempt} failed, retrying in 3s..."
    sleep 3
  done
  echo "ERROR: could not create topic after multiple attempts" >&2
  exit 1
}

# ------------------- Run the producer test -------------------
run_single_producer() {
  local label="$1" records="$2"
  docker run --rm --network host "${KAFKA_IMAGE}" \
    ${KAFKA_BIN}/kafka-producer-perf-test.sh \
    --topic ${TOPIC_NAME} \
    --num-records ${records} \
    --record-size ${RECORD_SIZE} \
    --throughput -1 \
    --producer-props \
      bootstrap.servers=${BOOTSTRAP} \
      acks=${ACKS} \
      batch.size=${BATCH_SIZE} \
      linger.ms=${LINGER_MS} \
      max.in.flight.requests.per.connection=${IN_FLIGHT}
}

run_producer_perf_test() {
  echo "Running producer performance test..."
  echo "  Mode: ${MODE} ($([ "${MODE}" = "separate" ] && echo "${CONCURRENCY} parallel containers" || echo "single container"))"

  if [ "${MODE}" = "separate" ] && [ "${CONCURRENCY}" -gt 1 ]; then
    local per_instance=$(( NUM_RECORDS / CONCURRENCY ))
    local remainder=$(( NUM_RECORDS % CONCURRENCY ))
    local pids=()

    echo "  Launching ${CONCURRENCY} parallel kafka-producer-perf-test instances"
    echo "  (${per_instance} records each, last gets ${remainder} extra)"
    echo ""
    for i in $(seq 1 "${CONCURRENCY}"); do
      local r=${per_instance}
      [ "${i}" -eq "${CONCURRENCY}" ] && r=$(( r + remainder ))
      echo "  [Producer ${i}] sending ${r} records..."
      run_single_producer "producer-${i}" "${r}" &
      pids+=($!)
    done

    # Wait for all and exit with first failure
    local failed=0
    for pid in "${pids[@]}"; do
      wait "${pid}" || failed=1
    done
    [ "${failed}" -eq 1 ] && exit 1
  else
    run_single_producer "main" "${NUM_RECORDS}"
  fi
}

# ------------ Clean-up (stop containers on exit) ------------
cleanup() {
  echo "Shutting down Docker KRaft cluster..."
  docker compose -f "${COMPOSE_FILE}" down -v
}
trap cleanup EXIT

while getopts "b:c:n:m:h" opt; do
  case $opt in
    b) BOOTSTRAP="$OPTARG" ;;
    c) CONCURRENCY="$OPTARG" ;;
    n) NUM_RECORDS="$OPTARG" ;;
    m) MODE="$OPTARG" ;;
    h) echo "Usage: ./kafka_benchmark.sh [-b bootstrap] [-c concurrency] [-n numRecords] [-m mode]"
       echo ""
       echo "  Modes:"
       echo "    shared    (default) — single kafka-producer-perf-test container"
       echo "    separate           — one container per concurrency thread"
       echo ""
       echo "  Examples:"
       echo "    ./kafka_benchmark.sh                       → shared mode, 200K records"
       echo "    ./kafka_benchmark.sh -c 4 -m separate       → 4 parallel containers"
       exit 0 ;;
    *) echo "Usage: ./kafka_benchmark.sh [-b bootstrap] [-c concurrency] [-n numRecords] [-m mode]" >&2
       exit 1 ;;
  esac
done

# ------------------- Main flow -------------------
start_cluster
create_topic
run_producer_perf_test
