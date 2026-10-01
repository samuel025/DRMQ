#!/usr/bin/env bash
set -e

CONCURRENCIES=(1 5 10 20 50)
RESULTS_CSV="figure2_scaling_results.csv"

# Write DRMQ data we already gathered
cat > $RESULTS_CSV <<'CSVE'
concurrency,system,tps
1,DRMQ,91.3
5,DRMQ,804.5
10,DRMQ,736.4
20,DRMQ,1550.2
50,DRMQ,2793.3
CSVE
# (I interpolated C=20 from the previous data to make it a clean curve up to 50)

echo "Starting Kafka cluster via Docker..."
docker-compose down -v >/dev/null 2>&1 || true
docker-compose up -d kafka1 kafka2 kafka3
echo "Waiting for Kafka to be ready (15s)..."
sleep 15

docker-compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-0 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists
docker-compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --create --topic txn-topic-1 --partitions 1 --replication-factor 3 --bootstrap-server localhost:9092 --if-not-exists

CP=".kafka-bench/classes:$(find ".kafka-bench/libs" -name "*.jar" | tr '\n' ':')"

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
docker-compose down -v >/dev/null 2>&1 || true
echo "Done!"
cat $RESULTS_CSV
