#!/bin/sh
set -e

# If first argument starts with '-' or is empty, assume running the broker
if [ "${1#-}" != "$1" ] || [ -z "$1" ]; then
    set -- drmq "$@"
fi

if [ "$1" = 'drmq' ]; then
    shift
    ARGS=""
    [ -n "${NODE_ID:-}" ]                && ARGS="${ARGS} --node-id ${NODE_ID}"
    [ -n "${ADVERTISED_HOST:-${HOST:-}}" ] && ARGS="${ARGS} --host ${ADVERTISED_HOST:-${HOST}}"
    [ -n "${PORT:-}" ]                   && ARGS="${ARGS} --port ${PORT}"
    [ -n "${WS_PORT:-}" ]                && ARGS="${ARGS} --ws-port ${WS_PORT}"
    [ -n "${DATA_DIR:-}" ]               && ARGS="${ARGS} --data-dir ${DATA_DIR}"
    [ -n "${PEERS:-}" ]                  && ARGS="${ARGS} --peers ${PEERS}"
    [ -n "${METRICS_ENABLED:-}" ]        && ARGS="${ARGS} --metrics-enabled ${METRICS_ENABLED}"
    [ -n "${METRICS_PORT:-}" ]           && ARGS="${ARGS} --metrics-port ${METRICS_PORT}"
    [ -n "${METRICS_PATH:-}" ]           && ARGS="${ARGS} --metrics-path ${METRICS_PATH}"
    [ -n "${RAFT_FSYNC_ENABLED:-}" ]     && ARGS="${ARGS} --raft-fsync-enabled ${RAFT_FSYNC_ENABLED}"
    [ -n "${LOG_SEGMENT_FSYNC:-}" ]      && ARGS="${ARGS} --log-segment-fsync ${LOG_SEGMENT_FSYNC}"
    [ -n "${RAFT_COMPACT_THRESHOLD:-}" ] && ARGS="${ARGS} --raft-compact-threshold ${RAFT_COMPACT_THRESHOLD}"
    [ -n "${MAX_DELIVERIES:-}" ]          && ARGS="${ARGS} --max-deliveries ${MAX_DELIVERIES}"
    [ -n "${DLQ_TOPIC_PREFIX:-}" ]        && ARGS="${ARGS} --dlq-topic-prefix ${DLQ_TOPIC_PREFIX}"
    [ -n "${S3_ARCHIVE_BUCKET:-}" ]      && ARGS="${ARGS} --s3-archive-bucket ${S3_ARCHIVE_BUCKET}"
    [ -n "${S3_ARCHIVE_REGION:-}" ]      && ARGS="${ARGS} --s3-archive-region ${S3_ARCHIVE_REGION}"
    [ -n "${S3_ARCHIVE_ENDPOINT:-}" ]    && ARGS="${ARGS} --s3-archive-endpoint ${S3_ARCHIVE_ENDPOINT}"

    # Default data dir inside container if none was provided
    if [ -z "${DATA_DIR:-}" ]; then
        ARGS="${ARGS} --data-dir /data"
    fi

    # Append any additional flags passed to container
    [ $# -gt 0 ] && ARGS="${ARGS} $*"

    JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx2g -XX:+UseG1GC}"

    echo "==> Starting DRMQ Broker..."
    echo "==> JVM Options: ${JAVA_OPTS}"
    echo "==> Arguments: ${ARGS}"
    exec java ${JAVA_OPTS} -jar /app/drmq-broker.jar ${ARGS}
fi

exec "$@"
