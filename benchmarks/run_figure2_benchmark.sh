#!/usr/bin/env bash
set -e

# Laptop-friendly scaling benchmark for DRMQ vs Kafka (Figure 2)
# Concurrencies: 1, 5, 10, 50, 100

echo "Compiling all modules first..."
(cd .. && mvn compile -q)

CONCURRENCIES=(1 5 10 20)
RESULTS_CSV="figure2_scaling_results.csv"

echo "concurrency,system,tps" > $RESULTS_CSV

# ==========================================
# 1. DRMQ BENCHMARK (MODE=separate)
# ==========================================
echo "=========================================="
echo " Starting DRMQ Benchmark (Separate Mode)  "
echo "=========================================="

echo "Ensuring clean docker environment..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Starting DRMQ cluster via Docker..."
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"

for C in "${CONCURRENCIES[@]}"; do
  # DRMQ No-Batch (pendingTxnLimit = 1)
  echo "Running DRMQ-NoBatch with Concurrency=$C ..."
  > drmq_nobatch_out_${C}.log
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C 2000 1 separate 2" -q > ../benchmarks/drmq_nobatch_out_${C}.log 2>&1) &
  DRMQ_PID=$!
  
  while ! grep -q "transactions committed" drmq_nobatch_out_${C}.log 2>/dev/null; do
    sleep 2
  done
  
  DRMQ_NOBATCH_TPS=$(grep "transactions committed" drmq_nobatch_out_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "$C,DRMQ-NoBatch,$DRMQ_NOBATCH_TPS" >> $RESULTS_CSV
  
  kill -9 $DRMQ_PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true

  # DRMQ Batch (pendingTxnLimit = C)
  echo "Running DRMQ-Batch with Concurrency=$C ..."
  > drmq_batch_out_${C}.log
  (cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP $C 2000 $C separate 2" -q > ../benchmarks/drmq_batch_out_${C}.log 2>&1) &
  DRMQ_PID=$!
  
  while ! grep -q "transactions committed" drmq_batch_out_${C}.log 2>/dev/null; do
    sleep 2
  done
  
  DRMQ_BATCH_TPS=$(grep "transactions committed" drmq_batch_out_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "$C,DRMQ-Batch,$DRMQ_BATCH_TPS" >> $RESULTS_CSV
  
  kill -9 $DRMQ_PID 2>/dev/null || true
  pkill -f "com.drmq.client.commandLineExample.AtomicStressTestApp" || true
done

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true


# ==========================================
# 2. KAFKA BENCHMARK
# ==========================================
echo "=========================================="
echo " Starting KAFKA Benchmark                 "
echo "=========================================="

echo "Ensuring clean docker environment..."
docker-compose down -v >/dev/null 2>&1 || true

echo "Starting Kafka cluster via Docker..."
docker-compose up -d kafka1 kafka2 kafka3
echo "Waiting for Kafka to be ready (15s)..."
sleep 15

# Create topics
docker-compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-0 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists
docker-compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-1 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists

echo "Extracting Kafka client libs from image..."
BENCH_DIR=".kafka-bench"
rm -rf "${BENCH_DIR}"
mkdir -p "${BENCH_DIR}/classes"
CID=$(docker create apache/kafka:3.7.0)
docker cp "${CID}:/opt/kafka/libs/." "${BENCH_DIR}/libs/"
docker rm "${CID}" >/dev/null

CP="$(find "${BENCH_DIR}/libs" -name "*.jar" | tr '\n' ':')"
echo "Compiling Kafka benchmark..."
javac -cp "${CP}" KafkaTransactionBenchmark.java -d "${BENCH_DIR}/classes"

CP="${BENCH_DIR}/classes:${CP}"

for C in "${CONCURRENCIES[@]}"; do
  echo "Running Kafka with Concurrency=$C ..."
  > kafka_out_${C}.log
  java -cp "${CP}" KafkaTransactionBenchmark "localhost:9092,localhost:9094,localhost:9096" 2000 $C separate 2 > kafka_out_${C}.log 2>&1 &
  KAFKA_PID=$!

  while ! grep -q "transactions committed" kafka_out_${C}.log 2>/dev/null; do
    sleep 2
  done
  
  TPS=$(grep "transactions committed" kafka_out_${C}.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
  echo "$C,Kafka,$TPS" >> $RESULTS_CSV
  
  kill -9 $KAFKA_PID 2>/dev/null || true
  pkill -f "KafkaTransactionBenchmark" || true
done

echo "Stopping Kafka..."
docker-compose down

echo "Done! Results saved to $RESULTS_CSV"
cat $RESULTS_CSV
