#!/usr/bin/env bash
set -e

echo "Ensuring clean docker environment..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Starting DRMQ cluster via Docker..."
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"

for C in 5 10 20 50; do
  echo "Running DRMQ-Batch with Concurrency=$C ..."
  > drmq_batch_out_${C}.log
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C 2000 $C separate 2" -q > ../benchmarks/drmq_batch_out_${C}.log 2>&1) &
  DRMQ_PID=$!
  
  while ! grep -q "transactions committed" drmq_batch_out_${C}.log 2>/dev/null; do
    sleep 2
  done
  
  DRMQ_BATCH_TPS=$(grep "transactions committed" drmq_batch_out_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "Result for C=$C: $DRMQ_BATCH_TPS TPS"
  
  kill -9 $DRMQ_PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true
