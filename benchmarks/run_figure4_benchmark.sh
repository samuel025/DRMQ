#!/usr/bin/env bash
set -e

RESULTS_CSV="figure4_latency_results.csv"
echo "system,metric,value" > $RESULTS_CSV

echo "=========================================="
echo " Starting DRMQ Latency Benchmark          "
echo "=========================================="

DATA_BASE_DIR="/tmp/drmq-fig4-benchmark"
BROKER_DIR="../drmq-broker"
CLIENT_DIR="../drmq-client"

echo "Starting 3-node DRMQ Raft cluster via Docker..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true
sleep 1
docker-compose -f docker-compose-drmq.yml up -d drmq1 drmq2 drmq3
echo "Waiting for DRMQ cluster to elect a leader (15s)..."
sleep 15

BOOTSTRAP="localhost:9093,localhost:9095,localhost:9097"

echo "Running DRMQ warm-up..."
(cd "${CLIENT_DIR}" && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP 1 500 1 separate 2" -q) >/dev/null || true

echo "Running DRMQ Latency Benchmark (5000 txns, C=1, serial)..."
OUTPUT_DRMQ="drmq_out.log"
(cd "${CLIENT_DIR}" && mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.AtomicStressTestApp" -Dexec.args="$BOOTSTRAP 1 5000 1 separate 2" -q) > $OUTPUT_DRMQ 2>&1 || true

cat $OUTPUT_DRMQ

echo "Stopping DRMQ brokers..."
docker-compose -f docker-compose-drmq.yml down -v >/dev/null 2>&1 || true
sleep 3

echo "=========================================="
echo " Starting KAFKA Latency Benchmark         "
echo "=========================================="
echo "Ensuring clean docker environment..."
docker-compose down -v >/dev/null 2>&1 || true
sleep 2

echo "Starting Kafka cluster via Docker..."
docker-compose up -d kafka1 kafka2 kafka3
echo "Waiting for Kafka to be ready (15s)..."
sleep 15

# Create topics
docker-compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-0 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists
docker-compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-1 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists

CP=".kafka-bench/classes:$(find ".kafka-bench/libs" -name "*.jar" | tr '\n' ':')"
echo "Running Kafka Latency Benchmark (5000 txns, C=1, serial)..."
OUTPUT_KAFKA="kafka_out.log"
java -cp "${CP}" KafkaTransactionBenchmark "localhost:9092,localhost:9094,localhost:9096" 5000 1 separate 2 > $OUTPUT_KAFKA 2>&1 || true

cat $OUTPUT_KAFKA

echo "Tearing down Kafka..."
docker-compose down -v >/dev/null 2>&1 || true

echo "Parsing results..."
# Parse DRMQ
p50=$(grep "p50 latency" $OUTPUT_DRMQ | awk -F':' '{print $2}' | awk '{print $1}')
p95=$(grep "p95 latency" $OUTPUT_DRMQ | awk -F':' '{print $2}' | awk '{print $1}')
p99=$(grep "p99 latency" $OUTPUT_DRMQ | awk -F':' '{print $2}' | awk '{print $1}')
p999=$(grep "p999 latency" $OUTPUT_DRMQ | awk -F':' '{print $2}' | awk '{print $1}')
echo "DRMQ,p50,$p50" >> $RESULTS_CSV
echo "DRMQ,p95,$p95" >> $RESULTS_CSV
echo "DRMQ,p99,$p99" >> $RESULTS_CSV
echo "DRMQ,p999,$p999" >> $RESULTS_CSV

# Parse Kafka
kp50=$(grep "p50 latency" $OUTPUT_KAFKA | awk -F':' '{print $2}' | awk '{print $1}')
kp95=$(grep "p95 latency" $OUTPUT_KAFKA | awk -F':' '{print $2}' | awk '{print $1}')
kp99=$(grep "p99 latency" $OUTPUT_KAFKA | awk -F':' '{print $2}' | awk '{print $1}')
kp999=$(grep "p999 latency" $OUTPUT_KAFKA | awk -F':' '{print $2}' | awk '{print $1}')
echo "Kafka,p50,$kp50" >> $RESULTS_CSV
echo "Kafka,p95,$kp95" >> $RESULTS_CSV
echo "Kafka,p99,$kp99" >> $RESULTS_CSV
echo "Kafka,p999,$kp999" >> $RESULTS_CSV

echo "Done! Results saved to $RESULTS_CSV"
cat $RESULTS_CSV
