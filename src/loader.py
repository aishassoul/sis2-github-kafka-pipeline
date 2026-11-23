"""
Stage 5 - Storage in SQLite3 + idempotency test.

* Creates data/output.db with the schema from sql/schema.sql
  (PRIMARY KEY repo_id, UNIQUE full_name, CHECK constraints, link table with a
  composite PRIMARY KEY).
* Loads the cleaned records with an UPSERT:
      INSERT ... ON CONFLICT(repo_id) DO UPDATE SET ...
  A repository that already exists is updated in place instead of being
  inserted again, so repeated execution can never create duplicates.
  first_loaded_at is written only on the first insert.
* Idempotency test: the same dataset is loaded twice; row counts and a SHA-256
  checksum of the whole table are compared after each load.
* Runs the verification queries from sql/verification_queries.sql.

Run standalone:  python -m src.loader          (loads twice and verifies)
                 python -m src.loader --reset  (delete the database first)
"""

from __future__ import annotations

import argparse
import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src import config
from src.utils import banner, write_json

logger = config.setup_logging("loader")

REPOSITORY_COLUMNS = [
    "repo_id", "full_name", "repo_name", "owner_login", "owner_type", "description", "language",
    "license_key", "stargazers_count", "forks_count", "open_issues_count", "size_kb", "topics_count",
    "is_fork", "is_archived", "default_branch", "html_url", "created_at", "updated_at", "pushed_at",
    "primary_search_topic", "search_topics", "matched_topics_count", "stars_log", "forks_to_stars_ratio",
    "issues_per_1k_stars", "repo_age_days", "days_since_last_push", "stars_per_year", "popularity_tier",
    "is_star_outlier", "is_active_popular", "created_year", "collected_at",
]


def connect(db_path: Path = config.DB_PATH) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(config.SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
    connection.commit()


def _to_db_value(value: Any) -> Any:
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, (bool, np.bool_)):
        return int(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def prepare_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Make sure every value has a SQLite-compatible Python type."""
    frame = df.copy()
    for column in ["created_at", "updated_at", "pushed_at", "collected_at"]:
        frame[column] = pd.to_datetime(frame[column], errors="coerce", format="ISO8601")
    for column in ["is_fork", "is_archived", "is_star_outlier", "is_active_popular"]:
        frame[column] = frame[column].astype(str).str.lower().isin(["true", "1"])
    missing = [c for c in REPOSITORY_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"Cleaned data is missing columns: {missing}")
    frame = frame[REPOSITORY_COLUMNS].astype(object)
    return [{k: _to_db_value(v) for k, v in row.items()} for row in frame.to_dict("records")]


def build_upsert_sql() -> str:
    columns = REPOSITORY_COLUMNS + ["first_loaded_at"]
    placeholders = ", ".join(f":{c}" for c in columns)
    update_set = ",\n        ".join(f"{c} = excluded.{c}" for c in REPOSITORY_COLUMNS if c != "repo_id")
    return (f"INSERT INTO repositories ({', '.join(columns)})\nVALUES ({placeholders})\n"
            f"ON CONFLICT(repo_id) DO UPDATE SET\n        {update_set};")


def load_records(connection: sqlite3.Connection, df: pd.DataFrame) -> dict[str, int]:
    rows = prepare_rows(df)
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for row in rows:
        row["first_loaded_at"] = loaded_at

    topic_links = [(row["repo_id"], topic) for row in rows for topic in str(row["search_topics"]).split("|") if topic]
    before = connection.execute("SELECT COUNT(*) FROM repositories").fetchone()[0]
    try:
        with connection:                                  # one transaction: all or nothing
            connection.executemany(build_upsert_sql(), rows)
            connection.executemany(
                "INSERT OR IGNORE INTO repository_topics (repo_id, search_topic) VALUES (?, ?)", topic_links)
    except sqlite3.Error as exc:
        logger.error("Database load failed and was rolled back: %s", exc)
        raise
    after = connection.execute("SELECT COUNT(*) FROM repositories").fetchone()[0]
    return {"rows_processed": len(rows), "rows_before": before, "rows_after": after,
            "new_rows_inserted": after - before, "existing_rows_upserted": len(rows) - (after - before)}


def table_checksum(connection: sqlite3.Connection) -> str:
    digest = hashlib.sha256()
    for table, order in (("repositories", "repo_id"), ("repository_topics", "repo_id, search_topic")):
        for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}"):
            digest.update(repr(row).encode("utf-8"))
    return digest.hexdigest()


def parse_verification_queries(path: Path = config.VERIFICATION_SQL_PATH) -> list[tuple[str, str]]:
    queries, name, buffer = [], None, []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("-- name:"):
            if name and buffer:
                queries.append((name, "\n".join(buffer).strip()))
            name, buffer = line.replace("-- name:", "").strip(), []
        elif name and not line.startswith("--"):
            buffer.append(line)
    if name and buffer:
        queries.append((name, "\n".join(buffer).strip()))
    return queries


def run_verification_queries(connection: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    results = {}
    lines = []
    for name, sql in parse_verification_queries():
        frame = pd.read_sql_query(sql, connection)
        results[name] = frame
        lines += [f"-- {name}", sql, "", frame.to_string(index=False) if not frame.empty else "(no rows)", ""]
    config.SQL_RESULTS_PATH.write_text("\n".join(lines), encoding="utf-8")
    return results


def run_loading(cleaned_df: pd.DataFrame | None = None, reset: bool = False) -> dict[str, Any]:
    if cleaned_df is None:
        cleaned_df = pd.read_csv(config.CLEANED_CSV_PATH)
    if reset and config.DB_PATH.exists():
        config.DB_PATH.unlink()
        logger.info("Existing database removed (--reset).")

    connection = connect()
    try:
        create_schema(connection)
        first = load_records(connection, cleaned_df)
        checksum_1 = table_checksum(connection)
        topics_1 = connection.execute("SELECT COUNT(*) FROM repository_topics").fetchone()[0]
        logger.info("Load #1: %s", first)

        second = load_records(connection, cleaned_df)           # idempotency test: same data again
        checksum_2 = table_checksum(connection)
        topics_2 = connection.execute("SELECT COUNT(*) FROM repository_topics").fetchone()[0]
        logger.info("Load #2: %s", second)

        verification = run_verification_queries(connection)
        duplicate_rows = len(verification["2_duplicate_repo_ids (expected: no rows)"])
        stored = int(verification["1_row_counts"]["total_rows"].iloc[0])
    finally:
        connection.close()

    report = {
        "database": str(config.DB_PATH.relative_to(config.PROJECT_ROOT)),
        "processed_records": int(len(cleaned_df)),
        "stored_records": stored,
        "topic_links_stored": topics_2,
        "load_1": first,
        "load_2": second,
        "checksum_after_load_1": checksum_1,
        "checksum_after_load_2": checksum_2,
        "topic_links_after_load_1": topics_1,
        "topic_links_after_load_2": topics_2,
        "idempotent": checksum_1 == checksum_2 and first["rows_after"] == second["rows_after"]
                      and topics_1 == topics_2,
        "duplicate_repo_ids_in_db": duplicate_rows,
        "processed_equals_stored": int(len(cleaned_df)) == stored,
    }
    write_json(config.LOAD_REPORT_PATH, report)
    return {"report": report, "verification": verification}


def main() -> int:
    parser = argparse.ArgumentParser(description="Load cleaned data into SQLite (twice) and verify")
    parser.add_argument("--reset", action="store_true", help="delete the existing database first")
    args = parser.parse_args()

    print(banner("STAGE 5 - SQLite3 storage and idempotency test"))
    try:
        result = run_loading(reset=args.reset)
    except (FileNotFoundError, ValueError, sqlite3.Error) as exc:
        logger.error("%s", exc)
        return 1
    report = result["report"]
    print(f"\nProcessed records       : {report['processed_records']}")
    print(f"Load #1                 : {report['load_1']}")
    print(f"Load #2 (same data)     : {report['load_2']}")
    print(f"Checksum #1             : {report['checksum_after_load_1'][:16]}...")
    print(f"Checksum #2             : {report['checksum_after_load_2'][:16]}...")
    print(f"Idempotent              : {report['idempotent']}")
    print(f"Stored records          : {report['stored_records']}")
    print(f"Duplicate ids in DB     : {report['duplicate_repo_ids_in_db']}")
    print(f"\nVerification query results saved to {config.SQL_RESULTS_PATH.relative_to(config.PROJECT_ROOT)}")
    for name, frame in result["verification"].items():
        if name.startswith(("1_", "2_", "3_", "4_", "8_")):
            print(f"\n-- {name}\n{frame.to_string(index=False) if not frame.empty else '(no rows)'}")
    return 0 if report["idempotent"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
