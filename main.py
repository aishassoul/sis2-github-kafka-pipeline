"""
SIS 2 - Data Streaming and Validation Pipeline (GitHub REST API -> Kafka -> Pandera
-> pandas/NumPy -> SQLite3 -> EDA).

Runs every stage in order, passes the output of each stage to the next one and
prints/saves a final verification report with the record counts of all stages.

Usage:
    python main.py                  # full pipeline (Kafka broker must be running)
    python main.py --skip-collect   # reuse data/raw/raw_records.json (no API calls)
    python main.py --keep-db        # do not reset data/output.db before loading
"""

from __future__ import annotations

import argparse
import sys
import time

from src import config
from src.utils import banner, read_json, write_json

logger = config.setup_logging("main")


def build_summary(collection: dict, producer: dict, consumer: dict, validation: dict,
                  cleaning: dict, loading: dict) -> dict:
    counts = {
        "1_collected_api_records": collection["total_records"],
        "2_published_to_kafka": producer["published_messages"],
        "3_consumed_from_kafka": consumer["records_consumed"],
        "4_test_records_added": validation["test_records_added"],
        "5_validated_total": validation["total_validated"],
        "6_valid_records": validation["valid_records"],
        "7_invalid_records": validation["invalid_records"],
        "8_records_after_cleaning": cleaning["rows_output"],
        "9_records_stored_in_sqlite": loading["stored_records"],
    }
    explanations = []
    if counts["1_collected_api_records"] == counts["2_published_to_kafka"] == counts["3_consumed_from_kafka"]:
        explanations.append("Collected = published = consumed: no message was lost in Kafka.")
    else:
        explanations.append("Collected / published / consumed differ - see producer_report.json and "
                            "consumer_report.json for failed or skipped messages.")
    explanations.append(
        f"Validated = consumed ({counts['3_consumed_from_kafka']}) + {counts['4_test_records_added']} deliberately "
        f"invalid test records = {counts['5_validated_total']}.")
    explanations.append(
        f"Invalid = {counts['7_invalid_records']} ({validation['invalid_test_records']} test records + "
        f"{validation['invalid_real_records']} real records); they were quarantined in invalid_records.csv.")
    explanations.append(
        f"Valid ({counts['6_valid_records']}) -> after cleaning ({counts['8_records_after_cleaning']}): "
        f"{cleaning['duplicate_repo_id_rows_removed']} rows were the same repository returned by several "
        f"overlapping topic queries and were merged into one row (topics kept in search_topics / "
        f"repository_topics).")
    explanations.append(
        f"After cleaning = stored in SQLite: {loading['processed_equals_stored']} "
        f"({counts['8_records_after_cleaning']} vs {counts['9_records_stored_in_sqlite']}).")
    explanations.append(
        f"Idempotency: second load inserted {loading['load_2']['new_rows_inserted']} new rows, "
        f"table checksum unchanged = {loading['idempotent']}, duplicate ids in DB = "
        f"{loading['duplicate_repo_ids_in_db']}.")

    checks = {
        "at_least_200_collected": counts["1_collected_api_records"] >= 200,
        "at_least_200_published": counts["2_published_to_kafka"] >= 200,
        "at_least_200_consumed": counts["3_consumed_from_kafka"] >= 200,
        "published_equals_consumed": counts["2_published_to_kafka"] == counts["3_consumed_from_kafka"],
        "all_test_records_detected": validation["all_test_records_detected"],
        "valid_plus_invalid_equals_total": counts["6_valid_records"] + counts["7_invalid_records"]
                                           == counts["5_validated_total"],
        "cleaned_equals_stored": loading["processed_equals_stored"],
        "idempotent_reload": loading["idempotent"],
        "no_duplicates_in_db": loading["duplicate_repo_ids_in_db"] == 0,
    }
    return {"counts": counts, "explanations": explanations, "checks": checks,
            "all_checks_passed": all(checks.values()), "kafka_run_id": producer["run_id"]}


def write_summary_markdown(summary: dict) -> None:
    lines = ["# Pipeline verification summary", "", "| Stage | Records |", "|---|---:|"]
    lines += [f"| {k[2:].replace('_', ' ')} | {v} |" for k, v in summary["counts"].items()]
    lines += ["", "## Explanation of differences", *[f"* {e}" for e in summary["explanations"]],
              "", "## Automatic checks", "| Check | Passed |", "|---|---|"]
    lines += [f"| {k.replace('_', ' ')} | {'yes' if v else 'NO'} |" for k, v in summary["checks"].items()]
    lines += ["", f"**All checks passed: {summary['all_checks_passed']}**", ""]
    config.PIPELINE_SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the complete SIS 2 pipeline")
    parser.add_argument("--skip-collect", action="store_true", help="reuse data/raw/raw_records.json")
    parser.add_argument("--keep-db", action="store_true", help="do not reset the SQLite database")
    args = parser.parse_args()
    started = time.time()

    # Imports here so that a missing optional dependency gives a clear error per stage
    from src import analysis, api_client, cleaner, consumer, loader, producer, validator

    try:
        print(banner("STAGE 1 - GitHub REST API data collection"))
        if args.skip_collect:
            collection = read_json(config.COLLECTION_REPORT_PATH)
            logger.info("Skipping API collection, reusing %s (%d records)",
                        config.RAW_JSON_PATH.name, collection["total_records"])
        else:
            api_client.collect_repositories()
            collection = read_json(config.COLLECTION_REPORT_PATH)

        print(banner("STAGE 2a - Kafka Producer"))
        producer_report = producer.run_producer()

        print(banner("STAGE 2b - Kafka Consumer"))
        consumer_report = consumer.run_consumer(run_id=producer_report["run_id"],
                                                expected=producer_report["published_messages"])
        if consumer_report["records_consumed"] == 0:
            raise RuntimeError("Consumer received no records - pipeline stopped.")

        print(banner("STAGE 3 - Pandera validation"))
        validation = validator.run_validation()

        print(banner("STAGE 4 - Cleaning and transformation"))
        cleaning = cleaner.run_cleaning(validation["valid_df"])

        print(banner("STAGE 5 - SQLite3 storage (loaded twice for the idempotency test)"))
        loading = loader.run_loading(cleaning["cleaned_df"], reset=not args.keep_db)

        print(banner("STAGE 6 - Exploratory data analysis"))
        analysis.run_analysis()
    except (api_client.GitHubAPIError, producer.KafkaConnectionError) as exc:
        logger.error("Pipeline stopped: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - report any unexpected failure clearly
        logger.exception("Pipeline failed: %s", exc)
        return 1

    summary = build_summary(collection, producer_report, consumer_report, validation["report"],
                            cleaning["report"], loading["report"])
    summary["duration_seconds"] = round(time.time() - started, 1)
    write_json(config.PIPELINE_SUMMARY_JSON, summary)
    write_summary_markdown(summary)

    print(banner("PIPELINE VERIFICATION SUMMARY"))
    for name, value in summary["counts"].items():
        print(f"  {name[2:].replace('_', ' '):<30} {value:>6}")
    print()
    for line in summary["explanations"]:
        print(f"  - {line}")
    print()
    for name, passed in summary["checks"].items():
        print(f"  [{'OK' if passed else 'FAIL'}] {name}")
    print(f"\nAll checks passed: {summary['all_checks_passed']}  "
          f"(finished in {summary['duration_seconds']} s)")
    print(f"Summary saved to {config.PIPELINE_SUMMARY_MD.relative_to(config.PROJECT_ROOT)}")
    return 0 if summary["all_checks_passed"] else 2


if __name__ == "__main__":
    sys.exit(main())
