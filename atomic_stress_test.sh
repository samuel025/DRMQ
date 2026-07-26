#!/bin/bash
# ─────────────────────────────────────────────────────────────
# DRMQ Atomic Cross-Topic Stress Test
# ─────────────────────────────────────────────────────────────

cd drmq-client || { echo "❌ Cannot find drmq-client directory"; exit 1; }

echo "⏳ Building client..."
mvn compile -q 2>/dev/null
echo "✓ Build complete"
echo ""

BROKERS="localhost:9092,localhost:9093,localhost:9094"
CONCURRENCY=10

while getopts "b:c:h" opt; do
  case $opt in
    b) BROKERS="$OPTARG" ;;
    c) CONCURRENCY="$OPTARG" ;;
    h) echo "Usage: ./atomic_stress_test.sh [-c concurrency] [-b brokers]"
       echo "Example: ./atomic_stress_test.sh -c 50"
       exit 0 ;;
    *) echo "Usage: ./atomic_stress_test.sh [-c concurrency] [-b brokers]" >&2
       exit 1 ;;
  esac
done

mvn exec:java \
  -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" \
  -Dexec.args="$BROKERS $CONCURRENCY"
