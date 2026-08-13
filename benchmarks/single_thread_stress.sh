#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# DRMQ Lightweight Single-Threaded Sequential Stress Test Script
#
# Runs 1 thread, 1 message per round-trip (sync ACK), with batching disabled.
#
# Options / Usage:
#   ./single_thread_stress.sh [-n numRecords] [-s recordSize] [-b bootstrap] [-t topic]
#   OR pass number of records directly as positional argument:
#   ./single_thread_stress.sh 5000
#
# Examples:
#   ./single_thread_stress.sh 1000
#   ./single_thread_stress.sh -n 5000 -s 1024
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# Default Configuration
TOPIC_NAME="single-thread-topic"
NUM_RECORDS=1000            # Default 1,000 records
RECORD_SIZE=512             # 512 bytes payload per record
BOOTSTRAP="localhost:9092,localhost:9093,localhost:9094"
DATA_BASE_DIR="/tmp/drmq-single-thread-benchmark"

# Broker ports
BROKER_PORTS=(9092 9093 9094)
BROKER_IDS=(node1 node2 node3)
BROKER_PEERS=(
  "node2:localhost:9093,node3:localhost:9094"
  "node1:localhost:9092,node3:localhost:9094"
  "node1:localhost:9092,node2:localhost:9093"
)

BROKER_PIDS=()

# Locate directory paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_DIR="${SCRIPT_DIR}/../drmq-broker"
CLIENT_DIR="${SCRIPT_DIR}/../drmq-client"

# Print usage help
show_help() {
  echo "Usage: ./single_thread_stress.sh [numRecords] OR [-n numRecords] [-s recordSize] [-b brokers] [-t topic]"
  echo ""
  echo "Options:"
  echo "  -n numRecords   Number of records to send (default: 1000, 0 = unbounded)"
  echo "  -s recordSize   Payload size per message in bytes (default: 512)"
  echo "  -b brokers      Bootstrap servers (default: localhost:9092,localhost:9093,localhost:9094)"
  echo "  -t topic        Topic name (default: single-thread-topic)"
  echo "  -h              Show this help message"
  echo ""
  echo "Examples:"
  echo "  ./single_thread_stress.sh 5000"
  echo "  ./single_thread_stress.sh -n 2000 -s 1024"
  exit 0
}

# Allow simple positional argument for numRecords e.g. ./single_thread_stress.sh 5000
if [[ $# -gt 0 ]] && [[ "$1" =~ ^[0-9]+$ ]]; then
  NUM_RECORDS="$1"
  shift
fi

while getopts "n:s:b:t:h" opt; do
  case $opt in
    n) NUM_RECORDS="$OPTARG" ;;
    s) RECORD_SIZE="$OPTARG" ;;
    b) BOOTSTRAP="$OPTARG" ;;
    t) TOPIC_NAME="$OPTARG" ;;
    h) show_help ;;
    *) echo "Invalid option. Use -h for help." >&2; exit 1 ;;
  esac
done

build_modules() {
  echo "⏳ Building drmq-broker and drmq-client..."
  (cd "${SCRIPT_DIR}/.." && mvn compile -pl drmq-broker,drmq-client -am -q 2>/dev/null)
  echo "✓  Build complete"
  echo ""
}

start_broker() {
  local node_id="$1"
  local port="$2"
  local peers="$3"
  local data_dir="${DATA_BASE_DIR}/${node_id}"

  mkdir -p "${data_dir}"

  local cfg="${data_dir}/broker.properties"
  cat > "${cfg}" <<EOF
node.id=${node_id}
port=${port}
data.dir=${data_dir}/data
peers=${peers}
metrics.enabled=false
log.segment.bytes=67108864
log.retention.ms=3600000
raft.compact.threshold=500
raft.fsync.enabled=false
EOF

  echo "  Starting broker ${node_id} on port ${port}..."
  (cd "${BROKER_DIR}" && \
    mvn exec:java \
      -Dexec.mainClass="com.drmq.broker.BrokerServer" \
      -Dexec.args="--config ${cfg}" \
      -q 2>"${data_dir}/broker.log") &
  BROKER_PIDS+=($!)
}

start_cluster() {
  echo "Launching 3-node DRMQ Raft cluster..."
  rm -rf "${DATA_BASE_DIR}"
  mkdir -p "${DATA_BASE_DIR}"

  for i in 0 1 2; do
    start_broker "${BROKER_IDS[$i]}" "${BROKER_PORTS[$i]}" "${BROKER_PEERS[$i]}"
  done

  echo "Waiting for Raft cluster leader election (up to 60 s)..."
  local deadline=$(( $(date +%s) + 60 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if nc -z -w1 localhost "${BROKER_PORTS[0]}" 2>/dev/null; then
      echo "✓  Cluster ready (leader elected)"
      echo ""
      return 0
    fi
    sleep 1
  done

  echo "ERROR: DRMQ cluster failed to start" >&2
  exit 1
}

wait_for_brokers() {
  echo "Checking reachable brokers on ${BOOTSTRAP}..."
  local deadline=$(( $(date +%s) + 60 ))
  local all_up=false
  while [[ $(date +%s) -lt $deadline ]]; do
    all_up=true
    for port in "${BROKER_PORTS[@]}"; do
      if ! nc -z -w1 localhost "${port}" 2>/dev/null; then
        all_up=false
        break
      fi
    done
    if $all_up; then
      echo "✓  All brokers reachable"
      echo ""
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Brokers not reachable" >&2
  exit 1
}

run_test() {
  echo "────────────────────────────────────────────────────────────"
  echo " DRMQ Single-Thread Sequential Stress Test"
  echo "   Records      : ${NUM_RECORDS}"
  echo "   Record size  : ${RECORD_SIZE} bytes"
  echo "   Pattern      : 1 Thread • 1 Msg / Round Trip (Sync ACK)"
  echo "   Batching     : Disabled (batchSize=1 byte, lingerMs=0)"
  echo "   Bootstrap    : ${BOOTSTRAP}"
  echo "────────────────────────────────────────────────────────────"
  echo ""

  (cd "${CLIENT_DIR}" && \
    mvn exec:java \
      -Dexec.mainClass="com.drmq.client.commandLineExample.SingleThreadStressTestApp" \
      -Dexec.args="${BOOTSTRAP} ${TOPIC_NAME} ${RECORD_SIZE} ${NUM_RECORDS}")
}

cleanup() {
  if [[ ${#BROKER_PIDS[@]} -gt 0 ]]; then
    echo ""
    echo "Shutting down DRMQ test cluster..."
    for pid in "${BROKER_PIDS[@]}"; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    done
    echo "Done."
  fi
}
trap cleanup EXIT

# Detect if cluster is already running
all_ports_open=true
for port in "${BROKER_PORTS[@]}"; do
  if ! nc -z -w1 localhost "${port}" 2>/dev/null; then
    all_ports_open=false
    break
  fi
done

if $all_ports_open; then
  echo "ℹ️  Detected existing DRMQ cluster — using existing brokers."
  wait_for_brokers
else
  build_modules
  start_cluster
fi

run_test
