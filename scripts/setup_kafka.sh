#!/usr/bin/env bash
# Downloads Apache Kafka (KRaft mode, no ZooKeeper) into ./.kafka and formats
# the local storage. Needs Java 17+ and curl. Works on macOS and Linux.
# Usage: bash scripts/setup_kafka.sh
set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-4.1.0}"
FALLBACK_VERSION="4.0.0"
SCALA_VERSION="2.13"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="$ROOT_DIR/.kafka"
DATA_DIR="$INSTALL_DIR/data"
CONFIG_FILE="$INSTALL_DIR/server.properties"

# ---- 1. Java check ---------------------------------------------------------
if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: Java is not installed. Kafka 4 needs Java 17 or newer."
  echo "  macOS (Homebrew): brew install openjdk@21"
  echo "  Anaconda        : conda install -c conda-forge openjdk=21"
  exit 1
fi
JAVA_MAJOR="$(java -version 2>&1 | awk -F'"' '/version/ {print $2}' | cut -d. -f1)"
if [ "${JAVA_MAJOR:-0}" -lt 17 ]; then
  echo "ERROR: Java $JAVA_MAJOR found, but Kafka 4 needs Java 17+ (brew install openjdk@21)."
  exit 1
fi
echo "Java $JAVA_MAJOR found."

# ---- 2. Download Kafka -----------------------------------------------------
mkdir -p "$INSTALL_DIR"
download() {
  local version="$1"
  local name="kafka_${SCALA_VERSION}-${version}"
  if [ -d "$INSTALL_DIR/$name" ]; then echo "$name already downloaded."; echo "$name" > "$INSTALL_DIR/current"; return 0; fi
  echo "Downloading Kafka $version ..."
  curl -fL "https://archive.apache.org/dist/kafka/${version}/${name}.tgz" -o "$INSTALL_DIR/${name}.tgz" || return 1
  tar -xzf "$INSTALL_DIR/${name}.tgz" -C "$INSTALL_DIR"
  rm -f "$INSTALL_DIR/${name}.tgz"
  echo "$name" > "$INSTALL_DIR/current"
}
download "$KAFKA_VERSION" || download "$FALLBACK_VERSION"
KAFKA_HOME="$INSTALL_DIR/$(cat "$INSTALL_DIR/current")"

# ---- 3. Broker configuration (KRaft, single node, data inside the project) --
BASE_CONFIG="$KAFKA_HOME/config/server.properties"
if ! grep -q "process.roles" "$BASE_CONFIG"; then BASE_CONFIG="$KAFKA_HOME/config/kraft/server.properties"; fi
grep -v '^log.dirs=' "$BASE_CONFIG" > "$CONFIG_FILE"
echo "log.dirs=$DATA_DIR" >> "$CONFIG_FILE"

# ---- 4. Format storage once -------------------------------------------------
if [ ! -f "$DATA_DIR/meta.properties" ]; then
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$CLUSTER_ID" -c "$CONFIG_FILE"
  echo "Storage formatted (cluster id $CLUSTER_ID)."
else
  echo "Storage already formatted."
fi
echo
echo "Kafka is ready. Start the broker with:  bash scripts/start_kafka.sh"
