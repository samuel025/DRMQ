# syntax=docker/dockerfile:1

# ========================================================
# Stage 1: Build shaded executable broker JAR with Maven
# ========================================================
FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /build

# Copy root pom and module poms
COPY pom.xml .
COPY drmq-protocol/pom.xml drmq-protocol/
COPY drmq-broker/pom.xml drmq-broker/
COPY drmq-client/pom.xml drmq-client/
COPY drmq-integration-tests/pom.xml drmq-integration-tests/

# Copy all source code
COPY drmq-protocol drmq-protocol
COPY drmq-broker drmq-broker
COPY drmq-client drmq-client
COPY drmq-integration-tests drmq-integration-tests

# Compile and package shaded executable uber-jar for drmq-broker using Docker BuildKit cache for ~/.m2
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -pl drmq-broker -am -DskipTests -B

# ========================================================
# Stage 2: Minimal Production Runtime
# ========================================================
FROM eclipse-temurin:21-jre-alpine

LABEL org.opencontainers.image.title="DRMQ - Distributed Reliable Message Queue" \
      org.opencontainers.image.description="Consensus-backed, fault-tolerant distributed message broker" \
      org.opencontainers.image.licenses="Apache-2.0"

# Install curl and bash for health checks and entrypoint scripts
RUN apk add --no-cache curl bash

# Create unprivileged user and directories
RUN addgroup -S drmq && adduser -S drmq -G drmq \
    && mkdir -p /app /data \
    && chown -R drmq:drmq /app /data

WORKDIR /app

# Copy the shaded jar and entrypoint
COPY --from=builder /build/drmq-broker/target/drmq-broker-1.0.0-SNAPSHOT.jar /app/drmq-broker.jar
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER drmq

# 9092: Broker TCP Client & Inter-Broker Raft Traffic
# 9096: Prometheus Metrics Endpoint (/metrics)
# 9292: Telemetry WebSocket Server (PORT + 200)
# 9392: Admin HTTP Server (PORT + 300)
EXPOSE 9092 9096 9292 9392

VOLUME ["/data"]

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["drmq"]
