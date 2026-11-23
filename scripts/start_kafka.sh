#!/usr/bin/env bash
# Starts the local Kafka broker (KRaft) on localhost:9092 in the foreground.
# Keep this terminal open while the pipeline runs; stop it with Ctrl+C.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="$ROOT_DIR/.kafka"
if [ ! -f "$INSTALL_DIR/current" ]; then
  echo "Kafka is not installed yet. Run: bash scripts/setup_kafka.sh"; exit 1
fi
KAFKA_HOME="$INSTALL_DIR/$(cat "$INSTALL_DIR/current")"
echo "Starting Kafka broker from $KAFKA_HOME (localhost:9092) ..."
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$INSTALL_DIR/server.properties"
