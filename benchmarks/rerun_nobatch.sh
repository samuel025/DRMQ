#!/usr/bin/env bash
set -e

echo "Ensuring clean docker environment..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Starting DRMQ cluster via Docker..."
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"

# Temporary file to store the new values
> nobatch_results.tmp

for C in 1 5 10 20; do
  echo "Running DRMQ-NoBatch with Concurrency=$C ..."
  > drmq_nobatch_out_${C}.log
  # For NoBatch, pendingTxnLimit is 1
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C 2000 1 separate 2" -q > ../benchmarks/drmq_nobatch_out_${C}.log 2>&1) &
  DRMQ_PID=$!
  
  while ! grep -q "transactions committed" drmq_nobatch_out_${C}.log 2>/dev/null; do
    sleep 2
  done
  
  DRMQ_NOBATCH_TPS=$(grep "transactions committed" drmq_nobatch_out_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "Result for C=$C: $DRMQ_NOBATCH_TPS TPS"
  echo "${C},DRMQ-NoBatch,${DRMQ_NOBATCH_TPS}" >> nobatch_results.tmp
  
  kill -9 $DRMQ_PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

# Update the CSV file inplace using python
python3 -c "
import csv
new_nobatch = {}
with open('nobatch_results.tmp', 'r') as f:
    for line in f:
        c, sys, tps = line.strip().split(',')
        new_nobatch[c] = tps

rows = []
with open('figure2_scaling_results.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    rows.append(header)
    for row in reader:
        if row[1] == 'DRMQ-NoBatch' and row[0] in new_nobatch:
            row[2] = new_nobatch[row[0]]
        rows.append(row)

with open('figure2_scaling_results.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(rows)
"
rm nobatch_results.tmp
echo "CSV updated."
