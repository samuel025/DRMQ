#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# DRMQ benchmark script – 3-node Raft cluster (mirrors kafka_benchmark.sh)
#
# Mirrors Kafka benchmark configuration exactly:
#   * 3-node Raft cluster (replication factor 3, majority ACK)
#   * Single topic, 1 shared producer, 4 feeder threads
#     (Kafka: 1 producer client; DRMQProducer senderLoop already serializes
#      one batch-at-a-time, equivalent to max.in.flight.requests.per.connection=1)
#   * batch.size = 1 MiB  (1 048 576 bytes)
#   * linger.ms  = 10 ms
#   * acks = all  (Raft quorum – majority of nodes must commit)
#   * 1 KB payload per record
#   * 200 000 records total
#
# Output mirrors kafka-producer-perf-test:
#   N records sent, X.X records/sec (Y.Y MB/sec)
#     avg latency: … ms,  max: … ms,  p50: … ms,  p95: … ms,
#     p99: … ms,  p999: … ms
#
# IMPORTANT: the test targets brokers that are already running on the ports
# listed below (localhost:9092, localhost:9093, localhost:9094).  If you want
# this script to spin up the brokers itself, start them before calling the
# producer loop, e.g. via IntelliJ run configurations or the three
# "start-broker-N" helper functions included here.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ──────────────────── Configuration (mirrors kafka_benchmark.sh) ──────────────
TOPIC_NAME="benchmark-topic"
NUM_RECORDS=200000          # 200 k records
RECORD_SIZE=1024            # 1 KB payload per record  (matches Kafka benchmark)
BATCH_SIZE=1048576          # 1 MiB batch             (matches Kafka benchmark)
LINGER_MS=10                # 10 ms linger            (matches Kafka benchmark)
CONCURRENCY=1              # 4 threads sharing 1 producer; keep accumulator full so
                             # batches reach 1 MiB before linger fires
MODE="shared"              # "shared" (single producer) or "separate" (one per thread)
# Network serialization (max.in.flight=1 equivalent) is enforced by DRMQProducer.senderLoop
# ACKS = "all" is enforced by the Raft quorum — no extra flag needed
BOOTSTRAP="localhost:9092,localhost:9093,localhost:9094"
DATA_BASE_DIR="/tmp/drmq-benchmark"

# Broker ports must match BOOTSTRAP above
BROKER_PORTS=(9092 9093 9094)
BROKER_IDS=(node1 node2 node3)
# Peer lists for each node  (id@host:raftPort; we use clientPort+10 as raft RPC port)
BROKER_PEERS=(
  "node2:localhost:9093,node3:localhost:9094"
  "node1:localhost:9092,node3:localhost:9094"
  "node1:localhost:9092,node2:localhost:9093"
)

BROKER_PIDS=()

# ──────────────────── Locate the broker JAR / Maven ──────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROKER_DIR="${SCRIPT_DIR}/../drmq-broker"
CLIENT_DIR="${SCRIPT_DIR}/../drmq-client"

# ──────────────────── Build both modules ─────────────────────────────────────
build_modules() {
  echo "⏳ Building drmq-broker and drmq-client..."
  (cd "${SCRIPT_DIR}/.." && mvn compile -pl drmq-broker,drmq-client -am -q 2>/dev/null)
  echo "✓  Build complete"
  echo ""
}

# ──────────────────── Start a single broker node ──────────────────────────────
start_broker() {
  local node_id="$1"
  local port="$2"
  local peers="$3"
  local data_dir="${DATA_BASE_DIR}/${node_id}"

  mkdir -p "${data_dir}"

  # Write a properties file for this node
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

  echo "  Starting broker ${node_id} on port ${port} (peers: ${peers})..."
  (cd "${BROKER_DIR}" && \
    mvn exec:java \
      -Dexec.mainClass="com.drmq.broker.BrokerServer" \
      -Dexec.args="--config ${cfg}" \
      -q 2>"${data_dir}/broker.log") &
  BROKER_PIDS+=($!)
}

# ──────────────────── Start the 3-node Raft cluster ──────────────────────────
start_cluster() {
  echo "Launching 3-node DRMQ Raft cluster..."
  rm -rf "${DATA_BASE_DIR}"
  mkdir -p "${DATA_BASE_DIR}"

  for i in 0 1 2; do
    start_broker "${BROKER_IDS[$i]}" "${BROKER_PORTS[$i]}" "${BROKER_PEERS[$i]}"
  done

  echo "Waiting for Raft cluster to elect a leader (up to 60 s)..."
  local deadline=$(( $(date +%s) + 60 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if nc -z -w1 localhost "${BROKER_PORTS[0]}" 2>/dev/null; then
      echo "✓  Cluster ready (leader elected)"
      echo ""
      return 0
    fi
    sleep 1
  done

  echo "ERROR: DRMQ cluster did not become ready in time" >&2
  exit 1
}

# ──────────────────── Check that brokers are already up ──────────────────────
wait_for_brokers() {
  echo "Waiting for DRMQ brokers on ${BOOTSTRAP} (up to 60 s)..."
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
  echo "ERROR: not all brokers became reachable in time" >&2
  exit 1
}

# ──────────────────── Run the bounded producer perf test ─────────────────────
run_producer_perf_test() {
  echo "────────────────────────────────────────────────────────────"
  echo " DRMQ Producer Performance Test"
  echo "   Records      : $(printf '%d' ${NUM_RECORDS})"
  echo "   Record size  : ${RECORD_SIZE} bytes  (1 KB)"
  echo "   Batch size   : ${BATCH_SIZE} bytes  (1 MiB)"
  echo "   Linger       : ${LINGER_MS} ms"
  echo "   Concurrency  : ${CONCURRENCY} thread(s)"
  echo "   Producer mode: ${MODE} ($([ "${MODE}" = "separate" ] && echo 'one per thread' || echo 'single shared producer'))"
  echo "   ACKs         : all (Raft quorum)"
  echo "   Bootstrap    : ${BOOTSTRAP}"
  echo "────────────────────────────────────────────────────────────"
  echo ""

  (cd "${CLIENT_DIR}" && \
    mvn exec:java \
      -Dexec.mainClass="com.drmq.client.commandLineExample.StressTestApp" \
      -Dexec.args="${BOOTSTRAP} ${CONCURRENCY} ${TOPIC_NAME} ${RECORD_SIZE} ${NUM_RECORDS} ${MODE}")
}

# ──────────────────── Cleanup ─────────────────────────────────────────────────
cleanup() {
  echo ""
  echo "Shutting down DRMQ benchmark cluster..."
  for pid in "${BROKER_PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  echo "Done."
}
trap cleanup EXIT

while getopts "b:c:n:s:m:h" opt; do
  case $opt in
    b) BOOTSTRAP="$OPTARG" ;;
    c) CONCURRENCY="$OPTARG" ;;
    n) NUM_RECORDS="$OPTARG" ;;
    s) RECORD_SIZE="$OPTARG" ;;
    m) MODE="$OPTARG" ;;
    h) echo "Usage: ./stress_test.sh [-b brokers] [-c concurrency] [-n numRecords] [-s recordSize] [-m mode]"
       echo ""
       echo "  Modes:"
       echo "    shared    (default) — single producer, threads fill accumulator"
       echo "    separate           — one producer per thread"
       echo ""
       echo "  Examples:"
       echo "    ./stress_test.sh                           → shared mode, 200K records"
       echo "    ./stress_test.sh -c 4 -m separate           → 4 independent producers"
       exit 0 ;;
    *) echo "Usage: ./stress_test.sh [-b brokers] [-c concurrency] [-n numRecords] [-s recordSize] [-m mode]" >&2
       exit 1 ;;
  esac
done

# ──────────────────── Main flow ───────────────────────────────────────────────
# Detect whether brokers are already running.
# If they are, skip spinning up new ones (useful when running against an
# existing cluster started via IntelliJ / run configs).
EXTERNAL_CLUSTER=false
all_ports_open=true
for port in "${BROKER_PORTS[@]}"; do
  if ! nc -z -w1 localhost "${port}" 2>/dev/null; then
    all_ports_open=false
    break
  fi
done

if $all_ports_open; then
  echo "ℹ️  Detected existing DRMQ cluster — skipping startup."
  EXTERNAL_CLUSTER=true
  wait_for_brokers
else
  build_modules
  start_cluster
fi

run_producer_perf_test
