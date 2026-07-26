#!/bin/bash
# ─────────────────────────────────────────────────────────────
# DRMQ Multi-Threaded Java Stress Test
# Runs a single JVM with multiple internal threads, sharing
# a single connection pool. Very memory efficient.
# ─────────────────────────────────────────────────────────────

cd drmq-client || { echo "❌ Cannot find drmq-client directory"; exit 1; }

# Build the client quietly before running
echo "⏳ Building client..."
mvn compile -q 2>/dev/null
echo "✓ Build complete"
echo ""

BROKERS="localhost:9092,localhost:9093,localhost:9094"
CONCURRENCY=4
TOPIC="load-test-topic"
MSG_SIZE=1024

while getopts "b:c:t:s:h" opt; do
  case $opt in
    b) BROKERS="$OPTARG" ;;
    c) CONCURRENCY="$OPTARG" ;;
    t) TOPIC="$OPTARG" ;;
    s) MSG_SIZE="$OPTARG" ;;
    h) echo "Usage: ./stress_test.sh [-c concurrency] [-b brokers] [-t topic] [-s payload_size]"
       echo "Example: ./stress_test.sh -c 10"
       exit 0 ;;
    *) echo "Usage: ./stress_test.sh [-c concurrency] [-b brokers] [-t topic] [-s payload_size]" >&2
       exit 1 ;;
  esac
done

mvn exec:java \
  -Dexec.mainClass="com.drmq.client.commandLineExample.StressTestApp" \
  -Dexec.args="$BROKERS $CONCURRENCY $TOPIC $MSG_SIZE"
