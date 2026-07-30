#!/usr/bin/env bash
set -euo pipefail

# Build exec.args from environment variables
ARGS=""
[ -n "${NODE_ID:-}" ]          && ARGS="${ARGS} --node-id ${NODE_ID}"
[ -n "${PORT:-}" ]             && ARGS="${ARGS} --port ${PORT}"
[ -n "${DATA_DIR:-}" ]         && ARGS="${ARGS} --data-dir ${DATA_DIR}"
[ -n "${PEERS:-}" ]            && ARGS="${ARGS} --peers ${PEERS}"
ARGS="${ARGS} --metrics-enabled ${METRICS_ENABLED:-false}"
ARGS="${ARGS} --raft-fsync-enabled ${RAFT_FSYNC_ENABLED:-false}"
[ -n "${RAFT_COMPACT_THRESHOLD:-}" ] && ARGS="${ARGS} --raft-compact-threshold ${RAFT_COMPACT_THRESHOLD}"

exec mvn exec:java -pl drmq-broker -Dexec.args="${ARGS}"
