#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# DRMQ Docker benchmark – 3-node Raft cluster in Docker
# Mirrors kafka_benchmark.sh exactly:
#   * 3-node DRMQ cluster in Docker Compose
#   * acks=all (Raft quorum), RF=3
#   * 1 KiB payload, 1 MiB batch, 10 ms linger
#   * N parallel client JVMs (one per producer, matching Kafka's
#     per-container kafka-producer-perf-test.sh)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ──────────────────── Configuration ──────────────────────────
TOPIC_NAME="benchmark-topic"
NUM_RECORDS=500000          # total records across all producers
RECORD_SIZE=1024
BATCH_SIZE=1048576          # 1 MiB
LINGER_MS=10
CONCURRENCY=4               # number of parallel producer JVMs
BOOTSTRAP="localhost:9092,localhost:9094,localhost:9096"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose-drmq.yml"
CLIENT_DIR="${SCRIPT_DIR}/../drmq-client"
# ──────────────────────────────────────────────────────────────

# ──────────────────── Start the Docker cluster ────────────────
start_cluster() {
  echo "Launching 3-node DRMQ Raft cluster via docker-compose..."
  docker compose -f "${COMPOSE_FILE}" up -d

  echo "Waiting for a Raft leader (up to 90 s)..."
  local deadline=$(( $(date +%s) + 90 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    for node in drmq1 drmq2 drmq3; do
      if docker logs "${node}" 2>&1 | grep -q "Became LEADER"; then
        echo "✓  ${node} became leader"
        sleep 3  # let cluster stabilise
        return 0
      fi
    done
    sleep 2
  done
  echo "ERROR: no leader elected within 90 s" >&2
  exit 1
}

# ──────────────────── Wait for ports ──────────────────────────
wait_for_ports() {
  echo "Waiting for broker ports..."
  local deadline=$(( $(date +%s) + 30 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local all_up=true
    for port in 9092 9094 9096; do
      if ! nc -z -w1 localhost "${port}" 2>/dev/null; then
        all_up=false; break
      fi
    done
    if $all_up; then
      echo "✓  All brokers reachable on localhost:9092,9094,9096"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: broker ports not reachable" >&2
  exit 1
}

# ──────────────────── Run the benchmark ───────────────────────
run_benchmark() {
  local per_instance=$(( NUM_RECORDS / CONCURRENCY ))
  local remainder=$(( NUM_RECORDS % CONCURRENCY ))
  local pids=()
  local outdir
  outdir=$(mktemp -d)

  echo "────────────────────────────────────────────────────────────"
  echo " DRMQ Docker Producer Performance Test"
  echo "   Records      : ${NUM_RECORDS}  (${per_instance} per producer, last +${remainder})"
  echo "   Record size  : ${RECORD_SIZE} bytes"
  echo "   Batch size   : ${BATCH_SIZE} bytes  (1 MiB)"
  echo "   Linger       : ${LINGER_MS} ms"
  echo "   Concurrency  : ${CONCURRENCY} parallel JVMs"
  echo "   ACKs         : all (Raft quorum)"
  echo "   Bootstrap    : ${BOOTSTRAP}"
  echo "────────────────────────────────────────────────────────────"
  echo ""

  for i in $(seq 1 "${CONCURRENCY}"); do
    local r=${per_instance}
    [[ ${i} -eq ${CONCURRENCY} ]] && r=$(( r + remainder ))
    # Run producer in background, stdout to temp file
    (cd "${CLIENT_DIR}" && \
      mvn exec:java \
        -Dexec.mainClass="com.drmq.client.commandLineExample.StressTestApp" \
        -Dexec.args="${BOOTSTRAP} 1 ${TOPIC_NAME} ${RECORD_SIZE} ${r} separate" \
        -q 2>/dev/null) > "${outdir}/p${i}.txt" 2>&1 &
    pids+=($!)
    echo "  [producer-${i}] PID $! — ${r} records"
  done

  # Wait for all
  local failed=0
  for pid in "${pids[@]}"; do
    wait "${pid}" 2>/dev/null || failed=1
  done

  echo ""
  echo "────────────────────────────────────────────────────────────"
  echo "📊  Results"
  echo "────────────────────────────────────────────────────────────"
  local total_rate=0
  for i in $(seq 1 "${CONCURRENCY}"); do
    local line
    line=$(grep "records sent" "${outdir}/p${i}.txt" 2>/dev/null | tail -1)
    if [[ -n "${line}" ]]; then
      echo "  ${line}"
      # Extract numeric throughput
      local rate
      rate=$(echo "${line}" | grep -oP '[\d,.]+(?= records/sec)' | tr -d ',')
      total_rate=$(echo "${total_rate} + ${rate}" | bc 2>/dev/null || echo "${total_rate}")
    fi
  done
  echo ""
  echo "  TOTAL throughput : ${total_rate} records/sec"
  echo "────────────────────────────────────────────────────────────"

  rm -rf "${outdir}"
  [[ ${failed} -eq 1 ]] && exit 1
}

# ──────────────────── Cleanup ─────────────────────────────────
cleanup() {
  echo ""
  echo "Shutting down DRMQ Docker cluster..."
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null
  echo "Done."
}
trap cleanup EXIT

# ──────────────────── Argument parsing ────────────────────────
while getopts "b:c:n:h" opt; do
  case $opt in
    b) BOOTSTRAP="$OPTARG" ;;
    c) CONCURRENCY="$OPTARG" ;;
    n) NUM_RECORDS="$OPTARG" ;;
    h) echo "Usage: $0 [-b bootstrap] [-c concurrency] [-n numRecords]"
       echo ""
       echo "  Examples:"
       echo "    $0                        → 4 parallel JVMs, 500K total"
       echo "    $0 -c 8 -n 1000000        → 8 parallel JVMs, 1M total"
       exit 0 ;;
    *) echo "Usage: $0 [-b bootstrap] [-c concurrency] [-n numRecords]" >&2
       exit 1 ;;
  esac
done

# ──────────────────── Main ────────────────────────────────────
start_cluster
wait_for_ports
run_benchmark
