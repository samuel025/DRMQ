#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# DRMQ Atomic Transactions benchmark – mirrors kafka_txn_benchmark.sh
#
# Mirrors DRMQ's natural architectural advantage:
#   * 3-node Raft cluster (replication factor 3, majority ACK)
#   * Each transaction writes atomically to 2 topics (Topic-A + Topic-B)
#   * 1 KB payload per topic per transaction  →  2 KB total per transaction
#   * CONCURRENCY = 10  (enables atomic batching: the atomicSenderLoop collapses
#     multiple sendAtomic() calls into ONE Raft proposal — this is DRMQ's core
#     architectural advantage, analogous to Kafka's partitions in throughput tests)
#   * Semaphore(5000): keeps atomicAccumulator full so batching is maximised
#   * 200,000 transactions (bounded)
#
# Two modes for honest comparison:
#   ./atomic_stress_test.sh            → DRMQ natural mode (batching ON, CONCURRENCY=10)
#   ./atomic_stress_test.sh -c 1 -i 1 → Serial mode (1 txn at a time, matches Kafka 2PC)
#
# Output:
#   N transactions committed, X.X TPS (Y.Y MB/sec)
#   avg / max / p50 / p95 / p99 / p999 latency
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ─────────────────── Configuration (mirrors kafka_txn_benchmark.sh) ───────────
TOPIC_A="Topic-A"
TOPIC_B="Topic-B"
NUM_TRANSACTIONS=200000     # total atomic transactions
CONCURRENCY=10             # threads feeding shared producer (enables atomic batching)
IN_FLIGHT=5000             # Semaphore size — keeps accumulator full for max batching
MODE="shared"              # "shared" (one producer) or "separate" (one per thread, mirrors Kafka)
BROKERS="localhost:9092,localhost:9093,localhost:9094"
TOPICS=2                   # number of topics per transaction
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_DIR="${SCRIPT_DIR}/../drmq-client"

while getopts "b:c:n:i:m:t:h" opt; do
  case $opt in
    b) BROKERS="$OPTARG" ;;
    c) CONCURRENCY="$OPTARG" ;;
    n) NUM_TRANSACTIONS="$OPTARG" ;;
    i) IN_FLIGHT="$OPTARG" ;;
    m) MODE="$OPTARG" ;;
    t) TOPICS="$OPTARG" ;;
    h) echo "Usage: ./atomic_stress_test.sh [-b brokers] [-c concurrency] [-n numTransactions] [-i inFlight] [-m mode] [-t topics]"
       echo ""
       echo "  Modes:"
       echo "    shared    (default) — single producer, threads fill accumulator (DRMQ's advantage)"
       echo "    separate           — one producer per thread (mirrors Kafka txn model)"
       echo ""
       echo "  Examples:"
       echo "    ./atomic_stress_test.sh                     → batched mode (CONCURRENCY=10, IN_FLIGHT=5000)"
       echo "    ./atomic_stress_test.sh -c 1 -i 1           → serial, shared producer"
       echo "    ./atomic_stress_test.sh -c 4 -i 1 -m separate → 4 independent producers (Kafka comparable)"
       exit 0 ;;
    *) echo "Usage: ./atomic_stress_test.sh [-b brokers] [-c concurrency] [-n numTransactions] [-i inFlight] [-m mode] [-t topics]" >&2
       exit 1 ;;
  esac
done

# ─────────────────── Build ────────────────────────────────────────────────────
echo "⏳ Building drmq-client..."
(cd "${CLIENT_DIR}" && mvn compile -q 2>/dev/null)
echo "✓  Build complete"
echo ""

# ─────────────────── Run ─────────────────────────────────────────────────────
echo "────────────────────────────────────────────────────────────"
echo " DRMQ Atomic Transactions Performance Test"
echo "   Transactions : ${NUM_TRANSACTIONS}"
echo "   Topics       : ${TOPIC_A}  +  ${TOPIC_B}"
echo "   Payload/topic: 1 KB  (2 KB total per transaction)"
echo "   Concurrency  : ${CONCURRENCY} thread(s)"
echo "   Producer mode: ${MODE} ($([ "${MODE}" = "separate" ] && echo 'one producer per thread, Kafka comparable' || echo 'single shared producer'))"
echo "   In-flight    : ${IN_FLIGHT} (batching $([ "${IN_FLIGHT}" -gt 1 ] && echo 'ON' || echo 'OFF — serial mode'))"
echo "   ACKs         : all (Raft quorum)"
echo "   Bootstrap    : ${BROKERS}"
echo "────────────────────────────────────────────────────────────"
echo ""

(cd "${CLIENT_DIR}" && \
  mvn exec:java \
    -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" \
    -Dexec.args="${BROKERS} ${CONCURRENCY} ${NUM_TRANSACTIONS} ${IN_FLIGHT} ${MODE} ${TOPICS}")
