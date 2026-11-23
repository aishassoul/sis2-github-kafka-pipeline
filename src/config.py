"""
Central configuration of the SIS 2 pipeline.

Every value can be overridden with an environment variable, so the
instructor can run the project without editing source code.
No secrets are stored here: the optional GitHub token is read from the
GITHUB_TOKEN environment variable (see .env.example).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines) so python-dotenv is not required."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


# --------------------------------------------------------------------------- #
# Paths (all relative to the repository root -> no hard-coded personal paths)
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parent.parent
_load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
API_RESPONSES_DIR = RAW_DIR / "api_responses"
PROCESSED_DIR = DATA_DIR / "processed"
FIGURES_DIR = PROJECT_ROOT / "figures"
LOGS_DIR = PROJECT_ROOT / "logs"
SQL_DIR = PROJECT_ROOT / "sql"

RAW_JSON_PATH = RAW_DIR / "raw_records.json"          # flattened records (types preserved)
RAW_CSV_PATH = RAW_DIR / "raw_data.csv"               # same records as CSV (original data)
COLLECTION_REPORT_PATH = RAW_DIR / "collection_report.json"

PRODUCER_REPORT_PATH = PROCESSED_DIR / "producer_report.json"
CONSUMED_JSON_PATH = PROCESSED_DIR / "consumed_records.json"
CONSUMED_CSV_PATH = PROCESSED_DIR / "consumed_records.csv"
CONSUMER_REPORT_PATH = PROCESSED_DIR / "consumer_report.json"

VALID_CSV_PATH = PROCESSED_DIR / "valid_records.csv"
INVALID_CSV_PATH = PROCESSED_DIR / "invalid_records.csv"
VALIDATION_ERRORS_CSV_PATH = PROCESSED_DIR / "validation_errors.csv"
VALIDATION_REPORT_PATH = PROCESSED_DIR / "validation_report.json"

CLEANED_CSV_PATH = PROCESSED_DIR / "cleaned_repositories.csv"
CLEANING_REPORT_PATH = PROCESSED_DIR / "cleaning_report.json"

DB_PATH = DATA_DIR / "output.db"
VERIFICATION_SQL_PATH = SQL_DIR / "verification_queries.sql"
SCHEMA_SQL_PATH = SQL_DIR / "schema.sql"
LOAD_REPORT_PATH = PROCESSED_DIR / "load_report.json"
SQL_RESULTS_PATH = PROCESSED_DIR / "sql_verification_results.txt"

EDA_REPORT_PATH = PROCESSED_DIR / "eda_statistics.json"
ANALYSIS_SUMMARY_PATH = PROCESSED_DIR / "analysis_summary.md"
PIPELINE_SUMMARY_JSON = PROCESSED_DIR / "pipeline_summary.json"
PIPELINE_SUMMARY_MD = PROCESSED_DIR / "pipeline_summary.md"

# --------------------------------------------------------------------------- #
# GitHub REST API
# --------------------------------------------------------------------------- #
GITHUB_API_BASE = "https://api.github.com"
GITHUB_SEARCH_ENDPOINT = f"{GITHUB_API_BASE}/search/repositories"
GITHUB_API_VERSION = "2022-11-28"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip() or None   # optional

# Technology domains (GitHub topics) used as search queries.
# Overlapping domains (machine-learning / deep-learning / data-science) are
# chosen on purpose: the same repository can be returned by several queries,
# which produces real duplicates that must be handled during cleaning.
SEARCH_TOPICS = [
    t.strip()
    for t in os.getenv(
        "SEARCH_TOPICS",
        "machine-learning,deep-learning,data-science,cybersecurity,devops,blockchain",
    ).split(",")
    if t.strip()
]
MIN_STARS = int(os.getenv("MIN_STARS", "100"))
RESULTS_PER_PAGE = int(os.getenv("RESULTS_PER_PAGE", "100"))   # GitHub maximum = 100
PAGES_PER_TOPIC = int(os.getenv("PAGES_PER_TOPIC", "1"))
SORT_BY = os.getenv("SORT_BY", "stars")
SORT_ORDER = os.getenv("SORT_ORDER", "desc")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
PAUSE_BETWEEN_REQUESTS_SECONDS = float(os.getenv("PAUSE_BETWEEN_REQUESTS_SECONDS", "2.5"))
MIN_REQUIRED_RECORDS = 200

# --------------------------------------------------------------------------- #
# Apache Kafka
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github_repositories")
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
KAFKA_CONSUMER_IDLE_TIMEOUT_SECONDS = int(os.getenv("KAFKA_CONSUMER_IDLE_TIMEOUT_SECONDS", "30"))
KAFKA_CONSUMER_MAX_RUNTIME_SECONDS = int(os.getenv("KAFKA_CONSUMER_MAX_RUNTIME_SECONDS", "180"))
MESSAGE_TYPE_RECORD = "RECORD"
MESSAGE_TYPE_END = "END_OF_STREAM"

# --------------------------------------------------------------------------- #
# Validation / cleaning parameters
# --------------------------------------------------------------------------- #
ALLOWED_OWNER_TYPES = ["User", "Organization"]
GITHUB_LAUNCH_DATE = "2007-10-01"          # GitHub went live in 2008; no repo can be older
ACTIVE_PUSH_WINDOW_DAYS = 365              # "actively maintained" = pushed in the last year
POPULAR_STARS_THRESHOLD = 10000           # ~ median of the collected repositories


def ensure_directories() -> None:
    for directory in (RAW_DIR, API_RESPONSES_DIR, PROCESSED_DIR, FIGURES_DIR, LOGS_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def setup_logging(name: str) -> logging.Logger:
    """Console + file logging shared by every module."""
    ensure_directories()
    root = logging.getLogger()
    if not root.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                                datefmt="%H:%M:%S")
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        file_handler = logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8")
        file_handler.setFormatter(fmt)
        root.addHandler(console)
        root.addHandler(file_handler)
        root.setLevel(logging.INFO)
    return logging.getLogger(name)
