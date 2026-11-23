"""
Stage 4 - Data cleaning and transformation with pandas and NumPy.

Input : validated records (data/processed/valid_records.csv)
Output: data/processed/cleaned_repositories.csv  (one row per unique repository)
        + aggregation / reshaping tables and cleaning_report.json

Steps and why they are needed:
  1. Data type conversion   - CSV stores everything as text; dates must be datetimes,
                              counters integers, flags booleans, low-cardinality text categories.
  2. Missing values         - many repositories have no language or license; replacing them
                              with explicit labels keeps the rows and makes grouping possible.
  3. Text normalisation     - removes extra whitespace / inconsistent case so that equal values
                              are grouped together (e.g. owner logins, topics, licenses).
  4. Duplicates             - the same repository is returned by several overlapping topic
                              queries (e.g. machine-learning and deep-learning). One row per
                              repo_id is kept; all matching topics are preserved in search_topics.
  5. NumPy features         - log-scaled stars, ratios, ages, z-scores, popularity tiers, outliers.
  6. Multi-condition filter - "active popular" repositories.
  7. GroupBy aggregation    - statistics per language, topic and owner type.
  8. Reshaping              - pivot_table (topic x popularity tier) and melt (wide -> long).

Run standalone:  python -m src.cleaner
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src import config
from src.utils import banner, write_json

logger = config.setup_logging("cleaner")

INT_COLUMNS = ["repo_id", "stargazers_count", "forks_count", "open_issues_count", "size_kb",
               "topics_count", "search_rank"]
DATE_COLUMNS = ["created_at", "updated_at", "pushed_at", "collected_at"]
BOOL_COLUMNS = ["is_fork", "is_archived"]
CATEGORY_COLUMNS = ["owner_type", "language", "license_key", "search_topic"]


def _bool_from_any(series: pd.Series) -> pd.Series:
    mapping = {"true": True, "false": False, "1": True, "0": False}
    return series.map(lambda v: v if isinstance(v, bool) else mapping.get(str(v).strip().lower(), False)).astype(bool)


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for column in INT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    for column in DATE_COLUMNS:
        df[column] = (pd.to_datetime(df[column], errors="coerce", utc=True, format="ISO8601")
                      .dt.tz_convert(None).astype("datetime64[ns]"))
    for column in BOOL_COLUMNS:
        df[column] = _bool_from_any(df[column])
    return df


def handle_missing(df: pd.DataFrame, log: dict[str, Any]) -> pd.DataFrame:
    df = df.copy()
    missing_before = df.isna().sum()
    log["missing_values_before"] = {k: int(v) for k, v in missing_before[missing_before > 0].items()}
    log["missing_percent_before"] = {k: round(float(v) / len(df) * 100, 2)
                                     for k, v in missing_before[missing_before > 0].items()}

    df["language"] = df["language"].fillna("Unknown")
    df["license_key"] = df["license_key"].fillna("NO_LICENSE")
    df["description"] = df["description"].fillna("")
    df["default_branch"] = df["default_branch"].fillna("main")
    # pushed_at missing -> fall back to the last update time
    df["pushed_at"] = df["pushed_at"].fillna(df["updated_at"])
    rows_before = len(df)
    df = df.dropna(subset=["repo_id", "full_name", "created_at", "stargazers_count"])
    log["rows_dropped_missing_critical"] = rows_before - len(df)

    missing_after = df.drop(columns=["kafka_offset", "kafka_partition"], errors="ignore").isna().sum()
    log["missing_values_after"] = {k: int(v) for k, v in missing_after[missing_after > 0].items()}
    return df


def normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for column in ["full_name", "repo_name", "owner_login", "language", "license_key",
                   "description", "search_topic", "owner_type", "default_branch"]:
        df[column] = df[column].astype(str).str.strip()
    df["description"] = df["description"].str.replace(r"\s+", " ", regex=True)
    df["owner_login"] = df["owner_login"].str.lower()
    df["search_topic"] = df["search_topic"].str.lower()
    df["license_key"] = df["license_key"].str.upper().replace({"NOASSERTION": "OTHER"})
    df["owner_type"] = df["owner_type"].str.title()
    df["full_name_normalized"] = df["full_name"].str.lower()
    return df


def remove_duplicates(df: pd.DataFrame, log: dict[str, Any]) -> pd.DataFrame:
    exact_duplicates = int(df.duplicated(subset=[c for c in df.columns
                                                 if c not in ("kafka_offset", "kafka_partition")]).sum())
    duplicated_ids = int(df.duplicated(subset=["repo_id"]).sum())

    # Preserve the information about every topic a repository matched
    topic_map = (df.groupby("repo_id")["search_topic"]
                 .agg(lambda s: "|".join(sorted(set(s)))).rename("search_topics"))
    topic_counts = df.groupby("repo_id")["search_topic"].nunique().rename("matched_topics_count")

    deduplicated = (df.sort_values(["repo_id", "search_rank"])
                      .drop_duplicates(subset=["repo_id"], keep="first")
                      .drop(columns=["search_topics", "matched_topics_count"], errors="ignore")
                      .merge(topic_map, on="repo_id")
                      .merge(topic_counts, on="repo_id")
                      .rename(columns={"search_topic": "primary_search_topic"}))
    log["exact_duplicate_rows"] = exact_duplicates
    log["duplicate_repo_id_rows_removed"] = duplicated_ids
    log["rows_after_deduplication"] = int(len(deduplicated))
    log["repositories_in_multiple_topics"] = int((deduplicated["matched_topics_count"] > 1).sum())
    return deduplicated


def add_numpy_features(df: pd.DataFrame, log: dict[str, Any]) -> pd.DataFrame:
    df = df.copy()
    stars = df["stargazers_count"].to_numpy(dtype=np.float64)
    forks = df["forks_count"].to_numpy(dtype=np.float64)
    issues = df["open_issues_count"].to_numpy(dtype=np.float64)

    reference = df["collected_at"].to_numpy(dtype="datetime64[ns]")
    created = df["created_at"].to_numpy(dtype="datetime64[ns]")
    pushed = df["pushed_at"].to_numpy(dtype="datetime64[ns]")
    one_day = np.timedelta64(1, "D")

    age_days = np.floor((reference - created) / one_day)
    push_days = np.floor((reference - pushed) / one_day)
    age_years = np.maximum(age_days / 365.25, 1 / 365.25)       # avoid division by zero

    df["stars_log"] = np.round(np.log1p(stars), 4)
    df["forks_to_stars_ratio"] = np.round(np.where(stars > 0, forks / np.where(stars > 0, stars, 1), 0.0), 4)
    df["issues_per_1k_stars"] = np.round(np.where(stars > 0, issues / np.where(stars > 0, stars, 1) * 1000, 0.0), 2)
    df["repo_age_days"] = age_days.astype(np.int64)
    df["days_since_last_push"] = np.clip(push_days, 0, None).astype(np.int64)
    df["stars_per_year"] = np.round(stars / age_years, 2)
    df["stars_zscore"] = np.round((stars - stars.mean()) / stars.std(), 4)
    df["created_year"] = df["created_at"].dt.year.astype(int)

    df["popularity_tier"] = np.select(
        [stars >= 50_000, stars >= 25_000, stars >= 10_000],
        ["Mega (50k+)", "Very high (25k-50k)", "High (10k-25k)"],
        default="Popular (<10k)",
    )
    q1, q3 = np.percentile(stars, [25, 75])
    iqr = q3 - q1
    df["is_star_outlier"] = (stars > q3 + 1.5 * iqr) | (stars < q1 - 1.5 * iqr)

    log["numpy_statistics"] = {
        "stars_mean": round(float(np.mean(stars)), 2),
        "stars_median": float(np.median(stars)),
        "stars_std": round(float(np.std(stars)), 2),
        "stars_q1": float(q1), "stars_q3": float(q3), "stars_iqr": float(iqr),
        "stars_p90": float(np.percentile(stars, 90)),
        "forks_median": float(np.median(forks)),
        "pearson_corr_log_stars_log_forks": round(float(np.corrcoef(np.log1p(stars), np.log1p(forks))[0, 1]), 4),
        "mean_repo_age_years": round(float(np.mean(age_days) / 365.25), 2),
        "star_outliers": int(df["is_star_outlier"].sum()),
    }
    return df


def apply_filters(df: pd.DataFrame, log: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    active_popular_mask = (
        (df["stargazers_count"] >= config.POPULAR_STARS_THRESHOLD)
        & (df["days_since_last_push"] <= config.ACTIVE_PUSH_WINDOW_DAYS)
        & (~df["is_archived"])
        & (~df["is_fork"])
    )
    df["is_active_popular"] = active_popular_mask.astype(bool)
    active_popular = df.loc[active_popular_mask]

    python_security = df.loc[(df["language"] == "Python")
                             & (df["search_topics"].str.contains("cybersecurity"))
                             & (df["stargazers_count"] >= config.POPULAR_STARS_THRESHOLD)]
    log["filter_active_popular"] = {
        "conditions": f"stars >= {config.POPULAR_STARS_THRESHOLD} AND pushed within "
                      f"{config.ACTIVE_PUSH_WINDOW_DAYS} days AND not archived AND not fork",
        "rows": int(len(active_popular)),
        "share_percent": round(len(active_popular) / len(df) * 100, 2),
    }
    log["filter_python_cybersecurity_popular"] = int(len(python_security))
    active_popular.to_csv(config.PROCESSED_DIR / "filtered_active_popular.csv", index=False)
    return df, active_popular


def build_aggregations(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    language_stats = (df.groupby("language", observed=True)
                      .agg(repo_count=("repo_id", "count"),
                           total_stars=("stargazers_count", "sum"),
                           mean_stars=("stargazers_count", "mean"),
                           median_stars=("stargazers_count", "median"),
                           median_forks=("forks_count", "median"),
                           mean_age_years=("repo_age_days", lambda s: s.mean() / 365.25),
                           active_popular_share=("is_active_popular", "mean"))
                      .round(2).sort_values("repo_count", ascending=False))

    topic_stats = (df.groupby("primary_search_topic", observed=True)
                   .agg(repo_count=("repo_id", "count"),
                        mean_stars=("stargazers_count", "mean"),
                        median_stars=("stargazers_count", "median"),
                        max_stars=("stargazers_count", "max"),
                        median_forks=("forks_count", "median"),
                        median_open_issues=("open_issues_count", "median"),
                        archived_repos=("is_archived", "sum"))
                   .round(2).sort_values("median_stars", ascending=False))

    owner_stats = (df.groupby("owner_type", observed=True)
                   .agg(repo_count=("repo_id", "count"),
                        median_stars=("stargazers_count", "median"),
                        median_forks=("forks_count", "median"))
                   .round(2))

    # Reshaping 1: pivot_table (topic x popularity tier)
    tier_order = ["Popular (<10k)", "High (10k-25k)", "Very high (25k-50k)", "Mega (50k+)"]
    topic_tier_pivot = (df.pivot_table(index="primary_search_topic", columns="popularity_tier",
                                       values="repo_id", aggfunc="count", fill_value=0,
                                       observed=True)
                        .reindex(columns=tier_order, fill_value=0))

    # Reshaping 2: melt (wide metrics -> long format, used for plotting/comparison)
    topic_metrics_long = (topic_stats.reset_index()[["primary_search_topic", "median_stars", "median_forks",
                                                     "median_open_issues"]]
                          .melt(id_vars="primary_search_topic", var_name="metric", value_name="value"))

    return {"language_stats": language_stats, "topic_stats": topic_stats, "owner_stats": owner_stats,
            "topic_tier_pivot": topic_tier_pivot, "topic_metrics_long": topic_metrics_long}


def run_cleaning(valid_df: pd.DataFrame | None = None) -> dict[str, Any]:
    if valid_df is None:
        valid_df = pd.read_csv(config.VALID_CSV_PATH)
    log: dict[str, Any] = {"rows_input": int(len(valid_df))}

    df = convert_types(valid_df)
    log["dtypes_after_conversion"] = {c: str(t) for c, t in df.dtypes.items()}
    df = handle_missing(df, log)
    df = normalize_text(df)
    df = remove_duplicates(df, log)
    df = add_numpy_features(df, log)
    df, _active = apply_filters(df, log)

    for column in ["owner_type", "language", "license_key", "primary_search_topic", "popularity_tier"]:
        df[column] = df[column].astype("category")
    df = df.drop(columns=["kafka_offset", "kafka_partition"], errors="ignore").sort_values(
        "stargazers_count", ascending=False).reset_index(drop=True)

    tables = build_aggregations(df)
    for name, table in tables.items():
        table.to_csv(config.PROCESSED_DIR / f"{name}.csv")
    df.to_csv(config.CLEANED_CSV_PATH, index=False)

    log["rows_output"] = int(len(df))
    log["columns_output"] = list(df.columns)
    write_json(config.CLEANING_REPORT_PATH, log)
    logger.info("Cleaning: %d valid rows -> %d unique repositories (%d duplicate rows removed)",
                log["rows_input"], log["rows_output"], log["duplicate_repo_id_rows_removed"])
    return {"report": log, "cleaned_df": df, "tables": tables}


def main() -> int:
    print(banner("STAGE 4 - Cleaning and transformation (pandas + NumPy)"))
    try:
        result = run_cleaning()
    except FileNotFoundError as exc:
        logger.error("%s. Run the validator first.", exc)
        return 1
    log, tables = result["report"], result["tables"]
    print(f"\nInput rows: {log['rows_input']} -> output rows: {log['rows_output']}")
    print(f"Missing values before cleaning: {log['missing_values_before']}")
    print(f"Duplicate repo_id rows removed: {log['duplicate_repo_id_rows_removed']}")
    print(f"Active popular repositories   : {log['filter_active_popular']['rows']}")
    print("\nTop languages (GroupBy):")
    print(tables["language_stats"].head(8).to_string())
    print("\nPivot table (topic x popularity tier):")
    print(tables["topic_tier_pivot"].to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
