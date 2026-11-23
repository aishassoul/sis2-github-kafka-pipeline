"""
Stage 3 - Data validation with Pandera.

The records received from Kafka are validated BEFORE the main cleaning stage.

Validation rules (see build_schema):
  R1  repo_id              integer, required, > 0
  R2  (repo_id, search_topic) unique  - the same repository must not appear twice
                                        in the same search result
  R3  full_name            required, must match the "owner/repository" pattern
  R4  owner_type           allowed categorical values: User, Organization
  R5  search_topic         allowed categorical values: the configured topics
  R6  stargazers_count, forks_count, open_issues_count, size_kb
                           integers, required, >= 0
  R7  created_at           valid datetime, not before GitHub launch, not in the future
  R8  updated_at, pushed_at valid datetime (pushed_at may be missing)
  R9  custom: updated_at must not be earlier than created_at
  R10 custom: full_name must equal owner_login + "/" + repo_name

Handling strategy for invalid data: QUARANTINE. Invalid rows are removed from
the main flow and written, together with the exact failed rules, to
data/processed/invalid_records.csv and validation_errors.csv.

Validation testing: four deliberately invalid TEST records (is_test_record=True)
are added to the received data to prove that the schema detects errors.
They are always excluded from the final dataset.

Run standalone:  python -m src.validator
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

try:                                    # pandera >= 0.24 recommends this import
    import pandera.pandas as pa
except ImportError:                     # older pandera versions
    import pandera as pa
from pandera.errors import SchemaErrors

Check = pa.Check
Column = pa.Column

from src import config
from src.utils import banner, read_json, write_json

logger = config.setup_logging("validator")

INTEGER_COLUMNS = ["repo_id", "stargazers_count", "forks_count", "open_issues_count", "size_kb", "topics_count"]
DATETIME_COLUMNS = ["created_at", "updated_at", "pushed_at", "collected_at"]
UNIQUE_KEY = ["repo_id", "search_topic"]


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
def build_schema(reference_time: pd.Timestamp) -> pa.DataFrameSchema:
    github_launch = pd.Timestamp(config.GITHUB_LAUNCH_DATE)
    latest_allowed = reference_time + pd.Timedelta(days=1)
    non_negative_int = dict(dtype="Int64", nullable=False)

    return pa.DataFrameSchema(
        columns={
            # R1
            "repo_id": Column("Int64", Check.gt(0, error="R1: repo_id must be a positive integer"),
                              nullable=False),
            # R3
            "full_name": Column(str, Check.str_matches(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
                                                       error="R3: full_name must look like owner/repo"),
                                nullable=False),
            "repo_name": Column(str, nullable=False),
            "owner_login": Column(str, nullable=False),
            # R4
            "owner_type": Column(str, Check.isin(config.ALLOWED_OWNER_TYPES,
                                                 error="R4: owner_type must be User or Organization"),
                                 nullable=False),
            # R5
            "search_topic": Column(str, Check.isin(config.SEARCH_TOPICS,
                                                   error="R5: search_topic must be a configured topic"),
                                   nullable=False),
            "language": Column(str, nullable=True),         # missing values handled in cleaning
            "license_key": Column(str, nullable=True),
            # R6
            "stargazers_count": Column(checks=Check.ge(0, error="R6: stargazers_count must be >= 0"),
                                       **non_negative_int),
            "forks_count": Column(checks=Check.ge(0, error="R6: forks_count must be >= 0"), **non_negative_int),
            "open_issues_count": Column(checks=Check.ge(0, error="R6: open_issues_count must be >= 0"),
                                        **non_negative_int),
            "size_kb": Column(checks=Check.ge(0, error="R6: size_kb must be >= 0"), **non_negative_int),
            # R7
            "created_at": Column("datetime64[ns]", [
                Check.ge(github_launch, error="R7: created_at earlier than GitHub launch"),
                Check.le(latest_allowed, error="R7: created_at is in the future"),
            ], nullable=False),
            # R8
            "updated_at": Column("datetime64[ns]",
                                 Check.le(latest_allowed, error="R8: updated_at is in the future"),
                                 nullable=False),
            "pushed_at": Column("datetime64[ns]", nullable=True),
        },
        checks=[
            # R9
            Check(lambda df: df["updated_at"] >= df["created_at"],
                  error="R9: updated_at must not be earlier than created_at", ignore_na=False),
            # R10
            Check(lambda df: df["full_name"].fillna("") ==
                  df["owner_login"].fillna("") + "/" + df["repo_name"].fillna(""),
                  error="R10: full_name must equal owner_login/repo_name", ignore_na=False),
        ],
        # R2
        unique=UNIQUE_KEY,
        report_duplicates="exclude_first",
        strict=False,
        coerce=False,
        name="GitHubRepositorySchema",
    )


# --------------------------------------------------------------------------- #
# Validation testing - deliberately invalid records
# --------------------------------------------------------------------------- #
def build_test_records(received: pd.DataFrame) -> list[dict[str, Any]]:
    """Four clearly identified invalid records (is_test_record=True)."""
    now = datetime.now(timezone.utc).isoformat()
    base = received.iloc[0].to_dict()
    future = (datetime.now(timezone.utc) + timedelta(days=365)).isoformat()

    test_1 = {**base, "repo_id": -1, "full_name": "TEST/negative-values", "owner_login": "TEST",
              "repo_name": "negative-values", "stargazers_count": -50, "forks_count": -3,
              "owner_type": "Robot"}                                    # R1, R4, R6
    test_2 = {**base, "repo_id": 900000000002, "full_name": "TEST invalid name",
              "owner_login": "TEST", "repo_name": "invalid-name",
              "created_at": "not-a-date", "search_topic": "unknown-topic"}   # R3, R5, R7(null), R10
    test_3 = {**base, "created_at": "2005-05-05T00:00:00Z",
              "updated_at": "2004-01-01T00:00:00Z"}                      # R2 (duplicate key), R7, R9
    test_4 = {**base, "repo_id": None, "full_name": "TEST/missing-id", "owner_login": "TEST",
              "repo_name": "missing-id", "open_issues_count": "many", "updated_at": future}  # R1, R6, R8
    tests = [test_1, test_2, test_3, test_4]
    for number, record in enumerate(tests, start=1):
        record["is_test_record"] = True
        record["test_case"] = f"TEST_{number}"
        record["collected_at"] = now
        record["kafka_offset"] = None
        record["kafka_partition"] = None
    return tests


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def prepare_types(df: pd.DataFrame) -> pd.DataFrame:
    """Parse JSON values into typed columns. Unparseable values become missing
    (NaT / <NA>) so that Pandera reports them as rule violations per row."""
    df = df.copy()
    for column in INTEGER_COLUMNS:
        numeric = pd.to_numeric(df[column], errors="coerce")
        numeric = numeric.where(numeric.isna() | (numeric % 1 == 0))
        df[column] = numeric.astype("Int64")
    for column in DATETIME_COLUMNS:
        parsed = pd.to_datetime(df[column], errors="coerce", utc=True, format="ISO8601")
        df[column] = parsed.dt.tz_convert(None).astype("datetime64[ns]")
    for column in ["full_name", "repo_name", "owner_login", "owner_type", "search_topic",
                   "language", "license_key", "description"]:
        df[column] = df[column].astype(object).where(df[column].notna(), None)
    return df


def run_validation(received: pd.DataFrame | None = None) -> dict[str, Any]:
    if received is None:
        received = pd.DataFrame(read_json(config.CONSUMED_JSON_PATH))
    if received.empty:
        raise ValueError("No consumed records to validate.")

    received = received.copy()
    received["is_test_record"] = False
    received["test_case"] = None
    tests = pd.DataFrame(build_test_records(received))
    combined = pd.concat([received, tests], ignore_index=True)
    typed = prepare_types(combined)

    reference_time = pd.Timestamp(datetime.now(timezone.utc)).tz_convert(None)
    schema = build_schema(reference_time)

    try:
        schema.validate(typed, lazy=True)
        failure_cases = pd.DataFrame(columns=["schema_context", "column", "check", "check_number",
                                              "failure_case", "index"])
    except SchemaErrors as err:
        failure_cases = err.failure_cases.copy()

    # One line per (row, column, rule). DataFrame-level checks (R2, R9, R10) are
    # reported by Pandera once per column, so they are collapsed to column "<row>".
    raw_failure_count = len(failure_cases)
    if not failure_cases.empty:
        failure_cases = failure_cases.copy()
        dataframe_level = (failure_cases["schema_context"].astype(str) == "DataFrameSchema") & \
            ~failure_cases["check"].astype(str).str.contains("uniqueness")
        failure_cases.loc[dataframe_level, "column"] = "<row>"
        failure_cases.loc[dataframe_level, "failure_case"] = None
        uniqueness = failure_cases["check"].astype(str).str.contains("uniqueness")
        failure_cases.loc[uniqueness, "column"] = "repo_id, search_topic"
        failure_cases.loc[uniqueness, "failure_case"] = None
        failure_cases = failure_cases.drop_duplicates(subset=["column", "check", "index"]).reset_index(drop=True)

    # Rows that failed at least one rule
    row_failures = failure_cases[failure_cases["index"].notna()].copy()
    row_failures["index"] = row_failures["index"].astype(int)
    invalid_index = set(row_failures["index"].tolist())
    # Safety net: duplicated key rows and test records are never allowed through
    duplicated_mask = typed.duplicated(subset=UNIQUE_KEY, keep="first")
    invalid_index |= set(typed.index[duplicated_mask].tolist())
    invalid_index |= set(typed.index[typed["is_test_record"]].tolist())

    errors_by_row: dict[int, list[str]] = {}
    for _, failure in row_failures.iterrows():
        column = failure["column"] if pd.notna(failure["column"]) else "dataframe"
        message = f"[{column}] {failure['check']}"
        bucket = errors_by_row.setdefault(int(failure["index"]), [])
        if message not in bucket:
            bucket.append(message)

    invalid_mask = typed.index.isin(sorted(invalid_index))
    valid_df = typed.loc[~invalid_mask].drop(columns=["is_test_record", "test_case"])
    invalid_df = combined.loc[invalid_mask].copy()
    def _fallback(i: int) -> list[str]:
        if bool(duplicated_mask.loc[i]):
            return ["[repo_id, search_topic] duplicate key"]
        return ["deliberately invalid test record (always excluded)"]

    invalid_df["validation_errors"] = ["; ".join(errors_by_row.get(i) or _fallback(i)) for i in invalid_df.index]

    # Save outputs
    valid_df.to_csv(config.VALID_CSV_PATH, index=False)
    invalid_df.to_csv(config.INVALID_CSV_PATH, index=False)
    failure_cases.to_csv(config.VALIDATION_ERRORS_CSV_PATH, index=False)

    real_invalid = int((~invalid_df["is_test_record"].astype(bool)).sum())
    test_invalid = int(invalid_df["is_test_record"].astype(bool).sum())
    failures_per_rule = (failure_cases["check"].astype(str).value_counts().to_dict()
                         if not failure_cases.empty else {})
    test_detection = {
        row["test_case"]: row["validation_errors"]
        for _, row in invalid_df[invalid_df["is_test_record"].astype(bool)].iterrows()
    }

    report = {
        "received_from_kafka": int(len(received)),
        "test_records_added": int(len(tests)),
        "total_validated": int(len(typed)),
        "valid_records": int(len(valid_df)),
        "invalid_records": int(len(invalid_df)),
        "invalid_real_records": real_invalid,
        "invalid_test_records": test_invalid,
        "all_test_records_detected": test_invalid == len(tests),
        "failure_cases_total": int(len(failure_cases)),
        "failure_cases_raw_pandera": int(raw_failure_count),
        "failures_per_rule": failures_per_rule,
        "test_record_errors": test_detection,
        "handling_strategy": "quarantine: invalid rows removed from main flow and saved to invalid_records.csv",
    }
    write_json(config.VALIDATION_REPORT_PATH, report)
    logger.info("Validation: %d valid / %d invalid (%d real, %d test records)",
                len(valid_df), len(invalid_df), real_invalid, test_invalid)
    return {"report": report, "valid_df": valid_df, "invalid_df": invalid_df, "failure_cases": failure_cases}


def main() -> int:
    print(banner("STAGE 3 - Pandera validation"))
    try:
        result = run_validation()
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        return 1
    report = result["report"]
    print(f"\nRecords validated : {report['total_validated']} "
          f"({report['received_from_kafka']} from Kafka + {report['test_records_added']} test records)")
    print(f"Valid records     : {report['valid_records']}")
    print(f"Invalid records   : {report['invalid_records']} "
          f"(real: {report['invalid_real_records']}, test: {report['invalid_test_records']})")
    print("\nFailures per rule:")
    for rule, count in report["failures_per_rule"].items():
        print(f"  {count:>4}  {rule}")
    print("\nDetected errors in deliberately invalid TEST records:")
    for case, errors in report["test_record_errors"].items():
        print(f"  {case}: {errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
