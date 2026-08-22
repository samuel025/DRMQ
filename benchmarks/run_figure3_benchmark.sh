#!/bin/bash
set -e

TOPIC_COUNTS=(2 3 5 7 10)
CONCURRENCY=20
RESULTS_CSV="figure3_fanout_results.csv"

echo "fanout,tps" > $RESULTS_CSV

echo "=========================================="
echo " Starting DRMQ Fan-out Benchmark          "
echo "=========================================="

echo "Ensuring clean docker environment..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Starting DRMQ cluster via Docker..."
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"

echo "Running warm-up (T=2, C=5)..."
(cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP 5 1000 5 separate 2" -q) >/dev/null || true

for T in "${TOPIC_COUNTS[@]}"; do
  echo "Running DRMQ with Fan-out T=$T (Concurrency=$CONCURRENCY)..."
  
  OUTPUT_FILE="drmq_out_${T}.log"
  > $OUTPUT_FILE
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $CONCURRENCY 2000 $CONCURRENCY separate $T" -q > ../benchmarks/$OUTPUT_FILE 2>&1) &
  DRMQ_PID=$!
  
  while ! grep -q "transactions committed" $OUTPUT_FILE 2>/dev/null; do
    sleep 2
  done
  
  TPS=$(grep "transactions committed" $OUTPUT_FILE | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "$T,$TPS" >> $RESULTS_CSV
  echo "  -> $TPS TPS"
  
  kill -9 $DRMQ_PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Done! Results saved to $RESULTS_CSV"
cat $RESULTS_CSV
