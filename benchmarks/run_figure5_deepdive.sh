#!/usr/bin/env bash
set -e

# Benchmark for Figure 5 Deep Dive: Batching Impact
# Concurrencies: 1, 4, 8, 16, 32, 50

echo "Compiling all modules first..."
(cd .. && mvn compile -q)

CONCURRENCIES=(1 4 8 16 32 50)
RESULTS_CSV="figure5_deepdive_results.csv"

echo "concurrency,mode,tps" > $RESULTS_CSV

echo "=========================================="
echo " Starting DRMQ Batching Deep Dive         "
echo "=========================================="

echo "Ensuring clean docker environment..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Starting DRMQ cluster via Docker..."
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"
TRANSACTIONS=2000

# 2. Shared Mode (16KB Batch) - Size-based batching
for C in "${CONCURRENCIES[@]}"; do
  echo "Running DRMQ [Shared Mode - 16KB] with Concurrency=$C ..."
  > drmq_out_shared16_${C}.log
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C $TRANSACTIONS $C shared 2 16384 5" -q > ../benchmarks/drmq_out_shared16_${C}.log 2>&1) &
  PID=$!
  
  TIMEOUT=240
  START=$SECONDS
  while ! grep -q "transactions committed" drmq_out_shared16_${C}.log 2>/dev/null; do
    sleep 2
    if (( SECONDS - START > TIMEOUT )); then
      echo "Timed out waiting for completion"
      break
    fi
  done
  
  TPS=$(grep "transactions committed" drmq_out_shared16_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  TPS=${TPS:-0}
  echo "$C,Shared_16KB,$TPS" >> $RESULTS_CSV
  
  kill -9 $PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

# 3. Shared Mode (1MB Batch) - Time-based batching
for C in "${CONCURRENCIES[@]}"; do
  echo "Running DRMQ [Shared Mode - 1MB] with Concurrency=$C ..."
  > drmq_out_shared1m_${C}.log
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C $TRANSACTIONS $C shared 2 1048576 5" -q > ../benchmarks/drmq_out_shared1m_${C}.log 2>&1) &
  PID=$!
  
  TIMEOUT=240
  START=$SECONDS
  while ! grep -q "transactions committed" drmq_out_shared1m_${C}.log 2>/dev/null; do
    sleep 2
    if (( SECONDS - START > TIMEOUT )); then
      echo "Timed out waiting for completion"
      break
    fi
  done
  
  TPS=$(grep "transactions committed" drmq_out_shared1m_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  TPS=${TPS:-0}
  echo "$C,Shared_1MB,$TPS" >> $RESULTS_CSV
  
  kill -9 $PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Done! Results saved to $RESULTS_CSV"
cat $RESULTS_CSV
