"""
Stage 2b - Kafka Consumer.

Subscribes to the Kafka topic, deserialises every JSON message and keeps the
records that belong to the producer run (run_id) being processed.

Termination conditions (the stream is finite):
  1. the END_OF_STREAM control message of this run_id is received, or
  2. no new message arrives for KAFKA_CONSUMER_IDLE_TIMEOUT_SECONDS, or
  3. the hard limit KAFKA_CONSUMER_MAX_RUNTIME_SECONDS is reached.

A fresh consumer group id is used on every run with auto.offset.reset=earliest,
so the consumer always reads the topic from the beginning and the run_id filter
selects exactly the messages of the current producer run.

Run standalone:  python -m src.consumer            (run_id taken from producer_report.json)
                 python -m src.consumer --run-id <id>
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from typing import Any

import pandas as pd
from confluent_kafka import Consumer, KafkaError, KafkaException

from src import config
from src.utils import banner, read_json, write_json

logger = config.setup_logging("consumer")


def deserialize(raw_value: bytes | None) -> dict[str, Any]:
    """JSON deserialisation: UTF-8 bytes -> Python dict."""
    if raw_value is None:
        raise ValueError("empty message value")
    message = json.loads(raw_value.decode("utf-8"))
    if not isinstance(message, dict) or "message_type" not in message:
        raise ValueError("message is not a valid pipeline envelope")
    return message


def run_consumer(run_id: str | None = None,
                 topic: str = config.KAFKA_TOPIC,
                 bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
                 expected: int | None = None) -> dict[str, Any]:
    if run_id is None:
        producer_report = read_json(config.PRODUCER_REPORT_PATH)
        run_id = producer_report["run_id"]
        expected = producer_report.get("published_messages")
    logger.info("Consuming topic '%s' for run_id=%s (expected=%s)", topic, run_id, expected)

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"sis2-consumer-{run_id}-{uuid.uuid4().hex[:6]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 45000,
    })

    records: list[dict[str, Any]] = []
    seen_sequences: set[int] = set()
    counters = {"messages_read": 0, "records_consumed": 0, "skipped_other_runs": 0,
                "deserialization_errors": 0, "broker_errors": 0, "duplicate_deliveries": 0}
    end_received = False
    stop_reason = ""
    started = last_message_at = time.time()

    try:
        consumer.subscribe([topic])
        while True:
            now = time.time()
            if end_received:
                stop_reason = "END_OF_STREAM received"
                break
            if now - last_message_at > config.KAFKA_CONSUMER_IDLE_TIMEOUT_SECONDS:
                stop_reason = f"idle timeout ({config.KAFKA_CONSUMER_IDLE_TIMEOUT_SECONDS}s without messages)"
                break
            if now - started > config.KAFKA_CONSUMER_MAX_RUNTIME_SECONDS:
                stop_reason = f"max runtime ({config.KAFKA_CONSUMER_MAX_RUNTIME_SECONDS}s) reached"
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                counters["broker_errors"] += 1
                logger.error("Kafka error: %s", msg.error())
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                continue

            last_message_at = time.time()
            counters["messages_read"] += 1
            try:
                message = deserialize(msg.value())
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                counters["deserialization_errors"] += 1
                logger.warning("Skipping malformed message at offset %s: %s", msg.offset(), exc)
                continue

            if message.get("run_id") != run_id:
                counters["skipped_other_runs"] += 1
                continue
            if message["message_type"] == config.MESSAGE_TYPE_END:
                end_received = True
                logger.info("END_OF_STREAM received (producer published %s records).",
                            message.get("total_records_published"))
                continue

            sequence = message.get("sequence")
            if sequence in seen_sequences:
                counters["duplicate_deliveries"] += 1
                continue
            seen_sequences.add(sequence)
            payload = message.get("payload")
            if not isinstance(payload, dict):
                counters["deserialization_errors"] += 1
                continue
            payload["kafka_offset"] = msg.offset()
            payload["kafka_partition"] = msg.partition()
            records.append(payload)
            counters["records_consumed"] += 1
            if counters["records_consumed"] % 100 == 0:
                logger.info("... %d records consumed", counters["records_consumed"])
    except KafkaException as exc:
        logger.error("Consumer stopped because of a Kafka error: %s", exc)
        stop_reason = f"kafka error: {exc}"
    except KeyboardInterrupt:
        stop_reason = "interrupted by user"
    finally:
        consumer.close()

    write_json(config.CONSUMED_JSON_PATH, records)
    pd.DataFrame(records).to_csv(config.CONSUMED_CSV_PATH, index=False)

    report = {
        "run_id": run_id,
        "topic": topic,
        "expected_records": expected,
        **counters,
        "end_of_stream_received": end_received,
        "stop_reason": stop_reason,
        "all_expected_received": expected is not None and counters["records_consumed"] == expected,
        "duration_seconds": round(time.time() - started, 2),
    }
    write_json(config.CONSUMER_REPORT_PATH, report)
    logger.info("Consumer finished: %d records consumed (stop reason: %s)",
                counters["records_consumed"], stop_reason)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Consume GitHub records from Kafka")
    parser.add_argument("--run-id", default=None, help="producer run id (default: from producer_report.json)")
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    print(banner("STAGE 2b - Kafka Consumer"))
    try:
        report = run_consumer(args.run_id, args.topic, args.bootstrap_servers)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 1
    print(f"\nConsumed records : {report['records_consumed']} (expected {report['expected_records']})")
    print(f"Stop reason      : {report['stop_reason']}")
    return 0 if report["records_consumed"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
