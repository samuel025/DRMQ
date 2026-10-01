#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
DATA_BASE_DIR="/tmp/drmq-serial-benchmark"
BROKER_DIR="${ROOT_DIR}/drmq-broker"
CLIENT_DIR="${ROOT_DIR}/drmq-client"

BROKER_PORTS=(9092 9093 9094)
BROKER_IDS=(node1 node2 node3)
BROKER_PEERS=(
  "node2:localhost:9093,node3:localhost:9094"
  "node1:localhost:9092,node3:localhost:9094"
  "node1:localhost:9092,node2:localhost:9093"
)
BROKER_PIDS=()

cleanup() {
  echo "Stopping DRMQ brokers..."
  for pid in "${BROKER_PIDS[@]}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "${DATA_BASE_DIR}"
  echo "✓ Cleanup done."
}
trap cleanup EXIT

echo "Starting 3-node DRMQ Raft cluster..."
rm -rf "${DATA_BASE_DIR}"
mkdir -p "${DATA_BASE_DIR}"

for i in 0 1 2; do
  node_id="${BROKER_IDS[$i]}"
  port="${BROKER_PORTS[$i]}"
  peers="${BROKER_PEERS[$i]}"
  data_dir="${DATA_BASE_DIR}/${node_id}"
  mkdir -p "${data_dir}"

  cfg="${data_dir}/broker.properties"
  cat > "${cfg}" <<EOF
node.id=${node_id}
port=${port}
data.dir=${data_dir}/data
peers=${peers}
metrics.enabled=false
log.segment.bytes=67108864
log.retention.ms=3600000
raft.compact.threshold=50000
raft.fsync.enabled=false
log.segment.fsync=false
EOF

  (cd "${BROKER_DIR}" && \
    mvn exec:java \
      -Dexec.mainClass="com.drmq.broker.BrokerServer" \
      -Dexec.args="--config ${cfg}" \
      -q 2>"${data_dir}/broker.log") &
  BROKER_PIDS+=($!)
done

echo "Waiting for DRMQ cluster to elect a leader..."
for i in {1..30}; do
  if nc -z -w1 localhost 9092 2>/dev/null && nc -z -w1 localhost 9093 2>/dev/null && nc -z -w1 localhost 9094 2>/dev/null; then
    echo "✓ All 3 brokers listening on ports 9092, 9093, 9094"
    sleep 3 # allow leader election to stabilize
    break
  fi
  sleep 1
done

echo "Running DRMQ Serial Atomic Transactions Benchmark (2,000 txns, Concurrency=1, InFlight=1, 2 topics, RF=3)..."
(cd "${CLIENT_DIR}" && \
  mvn exec:java \
    -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" \
    -Dexec.args="localhost:9092,localhost:9093,localhost:9094 1 2000 1 separate 2")
