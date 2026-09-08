#!/usr/bin/env bash
set -e

# Benchmark for Figure 5: Transaction Batching (Shared vs Separate Producers)
# Concurrencies: 1, 5, 10, 20, 50

echo "Compiling all modules first..."
(cd .. && mvn compile -q)

CONCURRENCIES=(1 5 10 20 50)
RESULTS_CSV="figure5_batching_results.csv"

echo "concurrency,mode,tps" > $RESULTS_CSV

echo "=========================================="
echo " Starting DRMQ Batching Benchmark         "
echo "=========================================="

echo "Ensuring clean docker environment..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Starting DRMQ cluster via Docker..."
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"

# Separate Mode
for C in "${CONCURRENCIES[@]}"; do
  echo "Running DRMQ [Separate Mode] with Concurrency=$C ..."
  > drmq_out_sep_${C}.log
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C 2000 $C separate 2" -q > ../benchmarks/drmq_out_sep_${C}.log 2>&1) &
  PID=$!
  
  while ! grep -q "transactions committed" drmq_out_sep_${C}.log 2>/dev/null; do
    sleep 2
  done
  
  TPS=$(grep "transactions committed" drmq_out_sep_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "$C,Separate,$TPS" >> $RESULTS_CSV
  
  kill -9 $PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

# Shared Mode (Batched)
for C in "${CONCURRENCIES[@]}"; do
  echo "Running DRMQ [Shared Mode] with Concurrency=$C ..."
  > drmq_out_shared_${C}.log
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C 2000 5000 shared 2" -q > ../benchmarks/drmq_out_shared_${C}.log 2>&1) &
  PID=$!
  
  while ! grep -q "transactions committed" drmq_out_shared_${C}.log 2>/dev/null; do
    sleep 2
  done
  
  TPS=$(grep "transactions committed" drmq_out_shared_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "$C,Shared,$TPS" >> $RESULTS_CSV
  
  kill -9 $PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Done! Results saved to $RESULTS_CSV"
cat $RESULTS_CSV
