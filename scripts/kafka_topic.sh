#!/usr/bin/env bash
# Topic helper for the oral defense.
#   bash scripts/kafka_topic.sh create     - create the topic (also done automatically by the producer)
#   bash scripts/kafka_topic.sh describe   - show partitions / leader
#   bash scripts/kafka_topic.sh list       - list all topics
#   bash scripts/kafka_topic.sh peek       - print the first 3 JSON messages from the topic
#   bash scripts/kafka_topic.sh count      - number of messages stored in the topic
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="$ROOT_DIR/.kafka"
if [ -f "$INSTALL_DIR/current" ]; then
  BIN="$INSTALL_DIR/$(cat "$INSTALL_DIR/current")/bin"
else
  BIN="$(dirname "$(command -v kafka-topics.sh || command -v kafka-topics)")"
fi
EXT=".sh"; [ -x "$BIN/kafka-topics.sh" ] || EXT=""
TOPIC="${KAFKA_TOPIC:-github_repositories}"
SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

case "${1:-describe}" in
  create)   "$BIN/kafka-topics$EXT" --bootstrap-server "$SERVER" --create --if-not-exists \
              --topic "$TOPIC" --partitions 1 --replication-factor 1 ;;
  describe) "$BIN/kafka-topics$EXT" --bootstrap-server "$SERVER" --describe --topic "$TOPIC" ;;
  list)     "$BIN/kafka-topics$EXT" --bootstrap-server "$SERVER" --list ;;
  peek)     "$BIN/kafka-console-consumer$EXT" --bootstrap-server "$SERVER" --topic "$TOPIC" \
              --from-beginning --max-messages 3 --property print.key=true ;;
  count)    "$BIN/kafka-get-offsets$EXT" --bootstrap-server "$SERVER" --topic "$TOPIC" ;;
  *) echo "Unknown command: $1"; exit 1 ;;
esac
