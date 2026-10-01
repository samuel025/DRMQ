#!/usr/bin/env bash
# ------------------------------------------------------------
# RabbitMQ benchmark script – 3-node quorum queue cluster
# Mirrors kafka_benchmark.sh / stress_test.sh:
#   * 3-node RabbitMQ cluster (quorum queue, RF=3, majority ACK)
#   * Single producer to a single queue
#   * Batch confirms enabled (equivalent to acks=all + batch.size=1MiB)
#   * 1 KiB payload per message
#   * 200,000 messages total, batched at 1024 msg/batch (= 1 MiB)
# ------------------------------------------------------------
set -euo pipefail

RABBIT_IMAGE="rabbitmq:3.13-management"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/.docker-compose.rabbitmq.yml"
BENCH_DIR="${SCRIPT_DIR}/.rabbitmq-bench"

# ------------------- Configuration -------------------
QUEUE_NAME="benchmark-queue"
MSG_SIZE=1024
NUM_MESSAGES=200000
BATCH_SIZE=1024       # 1024 msgs × 1 KiB ≈ 1 MiB, matches Kafka benchmark
AMQP_URI="amqp://guest:guest@localhost:5672"
# ----------------------------------------------------

# RabbitMQ Java client version (compatible with RabbitMQ 3.13.x)
AMQP_CLIENT_VERSION="5.21.0"
AMQP_CLIENT_JAR="amqp-client-${AMQP_CLIENT_VERSION}.jar"
AMQP_CLIENT_URL="https://repo1.maven.org/maven2/com/rabbitmq/amqp-client/${AMQP_CLIENT_VERSION}/${AMQP_CLIENT_JAR}"
SLF4J_VERSION="2.0.9"
SLF4J_JAR="slf4j-api-${SLF4J_VERSION}.jar"
SLF4J_URL="https://repo1.maven.org/maven2/org/slf4j/slf4j-api/${SLF4J_VERSION}/${SLF4J_JAR}"
SLF4J_SIMPLE_JAR="slf4j-simple-${SLF4J_VERSION}.jar"
SLF4J_SIMPLE_URL="https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/${SLF4J_VERSION}/${SLF4J_SIMPLE_JAR}"

# ----------- Generate the docker-compose file -----------
generate_compose() {
  cat > "${COMPOSE_FILE}" <<YAML
services:
  rabbit1:
    image: ${RABBIT_IMAGE}
    hostname: rabbit1
    container_name: rabbitmq-bench-1
    ports:
      - "5672:5672"
    environment:
      RABBITMQ_ERLANG_COOKIE: "DRMQ-BENCHMARK-SECRET"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "status"]
      interval: 5s
      timeout: 10s
      retries: 30

  rabbit2:
    image: ${RABBIT_IMAGE}
    hostname: rabbit2
    container_name: rabbitmq-bench-2
    ports:
      - "5673:5672"
    environment:
      RABBITMQ_ERLANG_COOKIE: "DRMQ-BENCHMARK-SECRET"
    depends_on:
      rabbit1:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "status"]
      interval: 5s
      timeout: 10s
      retries: 30

  rabbit3:
    image: ${RABBIT_IMAGE}
    hostname: rabbit3
    container_name: rabbitmq-bench-3
    ports:
      - "5674:5672"
    environment:
      RABBITMQ_ERLANG_COOKIE: "DRMQ-BENCHMARK-SECRET"
    depends_on:
      rabbit1:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "status"]
      interval: 5s
      timeout: 10s
      retries: 30
YAML
}

# ----------- Start cluster -----------
start_cluster() {
  echo "Launching 3-node RabbitMQ cluster..."
  docker compose -f "${COMPOSE_FILE}" up -d

  echo "Waiting for all 3 nodes to become healthy (up to 3 min)..."
  local container i
  for container in rabbitmq-bench-1 rabbitmq-bench-2 rabbitmq-bench-3; do
    for i in {1..36}; do
      if docker exec "${container}" rabbitmq-diagnostics -q status >/dev/null 2>&1; then
        break
      fi
      sleep 5
    done
  done
  echo "All nodes ready"
}

# ----------- Form the cluster -----------
form_cluster() {
  for node in 2 3; do
    echo "Joining rabbitmq-bench-${node} to rabbit@rabbit1..."
    docker exec "rabbitmq-bench-${node}" rabbitmqctl stop_app  >/dev/null 2>&1
    docker exec "rabbitmq-bench-${node}" rabbitmqctl reset 2>/dev/null
    docker exec "rabbitmq-bench-${node}" rabbitmqctl join_cluster rabbit@rabbit1 2>/dev/null
    docker exec "rabbitmq-bench-${node}" rabbitmqctl start_app >/dev/null 2>&1
  done

  echo "Waiting for cluster to stabilize..."
  sleep 3
  docker exec rabbitmq-bench-1 rabbitmqctl cluster_status
}

# ----------- Download deps & compile benchmark -----------
prepare_benchmark() {
  echo "Setting up RabbitMQ benchmark environment..."
  rm -rf "${BENCH_DIR}"
  mkdir -p "${BENCH_DIR}/classes" "${BENCH_DIR}/libs"

  echo "Downloading amqp-client ${AMQP_CLIENT_VERSION} + SLF4J..."
  curl -sL "${AMQP_CLIENT_URL}" -o "${BENCH_DIR}/libs/${AMQP_CLIENT_JAR}"
  curl -sL "${SLF4J_URL}" -o "${BENCH_DIR}/libs/${SLF4J_JAR}"
  curl -sL "${SLF4J_SIMPLE_URL}" -o "${BENCH_DIR}/libs/${SLF4J_SIMPLE_JAR}"

  local CP="${BENCH_DIR}/libs/${AMQP_CLIENT_JAR}:${BENCH_DIR}/libs/${SLF4J_JAR}:${BENCH_DIR}/libs/${SLF4J_SIMPLE_JAR}"

  echo "Compiling RabbitMQBenchmark.java..."
  javac -cp "${CP}" \
    "${SCRIPT_DIR}/RabbitMQBenchmark.java" \
    -d "${BENCH_DIR}/classes"

  echo "✓  Compilation complete"
  echo ""
}

# ----------- Run benchmark -----------
run_benchmark() {
  echo "────────────────────────────────────────────────────────────"
  echo " RabbitMQ Producer Performance Test"
  echo "   Messages    : ${NUM_MESSAGES}"
  echo "   Payload     : ${MSG_SIZE} bytes  (1 KB)"
  echo "   Batch size  : ${BATCH_SIZE} msgs  (~$((BATCH_SIZE * MSG_SIZE / 1024)) KiB)"
  echo "   Queue type  : quorum (RF=3, majority ACK)"
  echo "   Confirms    : batch (equivalent to acks=all)"
  echo "────────────────────────────────────────────────────────────"
  echo ""

  local CP="${BENCH_DIR}/classes:${BENCH_DIR}/libs/${AMQP_CLIENT_JAR}:${BENCH_DIR}/libs/${SLF4J_JAR}:${BENCH_DIR}/libs/${SLF4J_SIMPLE_JAR}"

  java -cp "${CP}" RabbitMQBenchmark \
    "${AMQP_URI}" "${NUM_MESSAGES}" "${BATCH_SIZE}"
}

# ----------- Clean up -----------
cleanup() {
  echo ""
  echo "Shutting down RabbitMQ cluster..."
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
  rm -rf "${COMPOSE_FILE}" "${BENCH_DIR}"
}
trap cleanup EXIT

# ----------- Main flow -----------
generate_compose
start_cluster
form_cluster
prepare_benchmark
run_benchmark
