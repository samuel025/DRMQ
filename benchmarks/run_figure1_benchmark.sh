#!/usr/bin/env bash
set -e

# Benchmark for Figure 1: Serial Throughput

echo "Compiling all modules first..."
(cd .. && mvn compile -q)

RESULTS_CSV="figure1_serial_results.csv"
echo "system,metric,value" > $RESULTS_CSV

# ==========================================
# 1. KAFKA BENCHMARK
# ==========================================
echo "=========================================="
echo " Starting KAFKA Benchmark                 "
echo "=========================================="
docker-compose down -v >/dev/null 2>&1 || true
docker-compose up -d kafka1 kafka2 kafka3
echo "Waiting for Kafka to be ready (15s)..."
sleep 15

# Create topics
docker-compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-0 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists
docker-compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-1 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists

echo "Running Kafka with Concurrency=1 ..."

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
# Run for 5000 transactions to get stable throughput
java -cp "${CP}" KafkaTransactionBenchmark "localhost:9092,localhost:9094,localhost:9096" 5000 1 separate 2 > kafka_out.log 2>&1 || true
KAFKA_TPS=$(grep "transactions committed" kafka_out.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
KAFKA_AVG=$(grep "avg latency" kafka_out.log | awk '{print $4}')
KAFKA_P50=$(grep "p50 latency" kafka_out.log | awk '{print $4}')
KAFKA_P95=$(grep "p95 latency" kafka_out.log | awk '{print $4}')
KAFKA_P99=$(grep "p99 latency" kafka_out.log | awk '{print $4}')
echo "Kafka,tps,$KAFKA_TPS" >> $RESULTS_CSV
echo "Kafka,avg_latency_ms,$KAFKA_AVG" >> $RESULTS_CSV
echo "Kafka,p50_latency_ms,$KAFKA_P50" >> $RESULTS_CSV
echo "Kafka,p95_latency_ms,$KAFKA_P95" >> $RESULTS_CSV
echo "Kafka,p99_latency_ms,$KAFKA_P99" >> $RESULTS_CSV

docker-compose down -v >/dev/null 2>&1 || true

# ==========================================
# 2. DRMQ BENCHMARK
# ==========================================
echo "=========================================="
echo " Starting DRMQ Benchmark                  "
echo "=========================================="
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"
echo "Running DRMQ with Concurrency=1 ..."
(cd ../drmq-client && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP 1 5000 1 separate 2" -q > ../benchmarks/drmq_out.log 2>&1) || true
DRMQ_TPS=$(grep "transactions committed" drmq_out.log | sed -n 's/.*committed, \([0-9,.]*\) TPS.*/\1/p' | tr -d ',')
DRMQ_AVG=$(grep "avg latency" drmq_out.log | awk '{print $4}')
DRMQ_P50=$(grep "p50 latency" drmq_out.log | awk '{print $4}')
DRMQ_P95=$(grep "p95 latency" drmq_out.log | awk '{print $4}')
DRMQ_P99=$(grep "p99 latency" drmq_out.log | awk '{print $4}')
echo "DRMQ,tps,$DRMQ_TPS" >> $RESULTS_CSV
echo "DRMQ,avg_latency_ms,$DRMQ_AVG" >> $RESULTS_CSV
echo "DRMQ,p50_latency_ms,$DRMQ_P50" >> $RESULTS_CSV
echo "DRMQ,p95_latency_ms,$DRMQ_P95" >> $RESULTS_CSV
echo "DRMQ,p99_latency_ms,$DRMQ_P99" >> $RESULTS_CSV

docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true

echo "Done! Results saved to $RESULTS_CSV"
cat $RESULTS_CSV
