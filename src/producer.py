"""
Stage 2a - Kafka Producer.

Reads the collected API records (data/raw/raw_records.json) and publishes every
record as one JSON message to the Kafka topic. After all data messages an
END_OF_STREAM control message is sent so the consumer knows when to stop.

Message format (value, UTF-8 JSON):
    {"message_type": "RECORD", "run_id": "...", "sequence": 1,
     "produced_at": "...", "payload": {<repository record>}}
Message key: repository id (as string)

Run standalone:  python -m src.producer
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src import config
from src.utils import banner, read_json, to_json_safe, write_json

logger = config.setup_logging("producer")


class KafkaConnectionError(RuntimeError):
    """Raised when the Kafka broker is not reachable."""


def check_broker(bootstrap_servers: str, timeout: float = 10.0) -> AdminClient:
    """Connect to the broker and fail fast with a clear message if it is down."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        metadata = admin.list_topics(timeout=timeout)
    except KafkaException as exc:
        raise KafkaConnectionError(
            f"Cannot connect to Kafka broker at {bootstrap_servers}. "
            f"Start Kafka first (see README, section 'Kafka setup'). Details: {exc}") from exc
    logger.info("Connected to Kafka broker(s): %s",
                ", ".join(f"id={b.id} {b.host}:{b.port}" for b in metadata.brokers.values()))
    return admin


def ensure_topic(admin: AdminClient, topic: str, partitions: int, replication: int) -> bool:
    """Create the topic if it does not exist. Returns True if it was created."""
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        logger.info("Topic '%s' already exists (%d partition(s)).", topic, len(existing[topic].partitions))
        return False
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions,
                                            replication_factor=replication)])
    try:
        futures[topic].result(timeout=15)
    except KafkaException as exc:
        if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            return False
        raise
    logger.info("Created topic '%s' (partitions=%d, replication_factor=%d).", topic, partitions, replication)
    return True


def serialize(message: dict[str, Any]) -> bytes:
    """JSON serialisation: Python dict -> UTF-8 bytes."""
    return json.dumps(to_json_safe(message), ensure_ascii=False).encode("utf-8")


def run_producer(input_path: Path = config.RAW_JSON_PATH,
                 topic: str = config.KAFKA_TOPIC,
                 bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS) -> dict[str, Any]:
    records = read_json(input_path)
    if not isinstance(records, list) or not records:
        raise ValueError(f"No records found in {input_path}")

    admin = check_broker(bootstrap_servers)
    topic_created = ensure_topic(admin, topic, config.KAFKA_TOPIC_PARTITIONS, config.KAFKA_REPLICATION_FACTOR)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "sis2-github-producer",
        "acks": "all",                 # wait for the broker to fully commit each message
        "enable.idempotence": True,    # no duplicates caused by internal retries
        "linger.ms": 10,
        "message.timeout.ms": 30000,
    })

    stats = {"delivered": 0, "failed": 0, "errors": []}

    def on_delivery(err: KafkaError | None, msg) -> None:
        """Called once per message with the broker's acknowledgement."""
        if err is not None:
            stats["failed"] += 1
            stats["errors"].append(str(err))
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            stats["delivered"] += 1

    started = time.time()
    attempted = 0
    serialization_errors = 0
    for sequence, record in enumerate(records, start=1):
        message = {
            "message_type": config.MESSAGE_TYPE_RECORD,
            "run_id": run_id,
            "sequence": sequence,
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "payload": record,
        }
        try:
            value = serialize(message)
        except (TypeError, ValueError) as exc:
            serialization_errors += 1
            logger.error("Could not serialise record #%d: %s", sequence, exc)
            continue

        while True:
            try:
                producer.produce(topic, key=str(record.get("repo_id")), value=value, on_delivery=on_delivery)
                attempted += 1
                break
            except BufferError:
                producer.poll(1.0)     # local queue full -> let it drain, then retry
            except KafkaException as exc:
                stats["failed"] += 1
                logger.error("Produce error for record #%d: %s", sequence, exc)
                break
        producer.poll(0)               # serve delivery callbacks

    remaining = producer.flush(30)
    if remaining:
        logger.error("%d message(s) still undelivered after flush timeout.", remaining)

    # Control message that marks the end of this finite stream
    end_message = {
        "message_type": config.MESSAGE_TYPE_END,
        "run_id": run_id,
        "sequence": attempted + 1,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "total_records_published": stats["delivered"],
    }
    end_delivered = {"ok": False}

    def on_end_delivery(err, _msg):
        end_delivered["ok"] = err is None
        if err:
            logger.error("END_OF_STREAM delivery failed: %s", err)

    producer.produce(topic, key="__END__", value=serialize(end_message), on_delivery=on_end_delivery)
    producer.flush(30)

    report = {
        "run_id": run_id,
        "topic": topic,
        "topic_created_now": topic_created,
        "bootstrap_servers": bootstrap_servers,
        "input_records": len(records),
        "attempted_messages": attempted,
        "published_messages": stats["delivered"],
        "failed_messages": stats["failed"],
        "serialization_errors": serialization_errors,
        "end_of_stream_sent": end_delivered["ok"],
        "duration_seconds": round(time.time() - started, 2),
        "errors": stats["errors"][:20],
    }
    write_json(config.PRODUCER_REPORT_PATH, report)
    logger.info("Producer finished: %d/%d messages published to '%s' (failed=%d, run_id=%s)",
                stats["delivered"], len(records), topic, stats["failed"], run_id)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish collected GitHub records to Kafka")
    parser.add_argument("--input", type=Path, default=config.RAW_JSON_PATH)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    print(banner("STAGE 2a - Kafka Producer"))
    try:
        report = run_producer(args.input, args.topic, args.bootstrap_servers)
    except (KafkaConnectionError, FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        return 1
    print(f"\nPublished messages : {report['published_messages']}")
    print(f"Failed messages    : {report['failed_messages']}")
    print(f"Run id             : {report['run_id']}")
    return 0 if report["failed_messages"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
