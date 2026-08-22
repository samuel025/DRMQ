#!/usr/bin/env bash
set -e

# Maximum Power Asynchronous Benchmark for DRMQ (Figure 6)
# Concurrencies: 1, 10, 20, 50

echo "Compiling all modules first..."
(cd .. && mvn compile -q)

CONCURRENCIES=(1 10 20 50)
TRANSACTIONS=20000
RESULTS_CSV="figure6_maxpower_results.csv"

echo "concurrency,tps" > $RESULTS_CSV

echo "=========================================="
echo " Starting DRMQ Maximum Power Benchmark    "
echo "=========================================="

echo "Ensuring clean docker environment..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Starting DRMQ cluster via Docker..."
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"

for C in "${CONCURRENCIES[@]}"; do
  echo "Running DRMQ [Max Power] with Concurrency=$C ..."
  
  # Run test in background
  > drmq_out_maxpower_${C}.log
  
  # Arguments: bootstrap C numTransactions pendingTxnLimit mode numTopics batchSize linger
  # We use pendingTxnLimit=5000 (massive pipelining), mode=shared, batch=1MB, linger=5ms
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C $TRANSACTIONS 5000 shared 2 1048576 5" -q > ../benchmarks/drmq_out_maxpower_${C}.log 2>&1 ) &
  
  DRMQ_PID=$!
  
  # Timeout mechanism just in case
  TIMEOUT=240
  ELAPSED=0
  while ! grep -q "transactions committed" drmq_out_maxpower_${C}.log 2>/dev/null; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "Timeout reached for C=$C!"
        kill -9 $DRMQ_PID 2>/dev/null || true
        break
    fi
  done
  
  DRMQ_TPS=$(grep "transactions committed" drmq_out_maxpower_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "$C,$DRMQ_TPS" >> $RESULTS_CSV
  
  kill -9 $DRMQ_PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Done! Results saved to $RESULTS_CSV"
cat $RESULTS_CSV
