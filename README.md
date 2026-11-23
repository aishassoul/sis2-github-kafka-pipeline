# SIS 2 — Data Streaming and Validation Pipeline with Apache Kafka and Pandera

**Course:** Data Collection & Preparation
**Group members:** Aisha Kuantay, Arina Bakyt, Tomiris Muratova, Ranida Sagyntayeva
**Data source:** [GitHub REST API](https://docs.github.com/en/rest) — `GET /search/repositories`

## 1. Project overview

The project collects real data about the most-starred GitHub repositories in six
technology domains (machine-learning, deep-learning, data-science, cybersecurity,
devops, blockchain), streams the records through **Apache Kafka**, validates them with
**Pandera**, cleans and transforms them with **pandas** and **NumPy**, stores them in
**SQLite3** and performs **exploratory data analysis** with Matplotlib/Seaborn.

```
GitHub REST API ──► api_client.py ──► data/raw/ (original data)
                                          │
                                          ▼
                    producer.py ──JSON──► Kafka topic "github_repositories" ──JSON──► consumer.py
                                                                                          │
                                                                                          ▼
     analysis.py ◄── SQLite3 data/output.db ◄── loader.py ◄── cleaner.py ◄── validator.py (Pandera)
         │
         ▼
     figures/*.png + data/processed/analysis_summary.md
```

| Stage | Module | Output |
|---|---|---|
| 1. Collection | `src/api_client.py` | `data/raw/raw_data.csv`, `raw_records.json`, `api_responses/*.json` |
| 2a. Kafka Producer | `src/producer.py` | messages in topic `github_repositories`, `producer_report.json` |
| 2b. Kafka Consumer | `src/consumer.py` | `consumed_records.json/.csv`, `consumer_report.json` |
| 3. Validation | `src/validator.py` | `valid_records.csv`, `invalid_records.csv`, `validation_errors.csv`, `validation_report.json` |
| 4. Cleaning | `src/cleaner.py` | `cleaned_repositories.csv`, aggregation / pivot tables, `cleaning_report.json` |
| 5. Storage | `src/loader.py` + `sql/` | `data/output.db`, `load_report.json`, `sql_verification_results.txt` |
| 6. EDA | `src/analysis.py` | `figures/*.png`, `eda_statistics.json`, `analysis_summary.md` |
| Orchestration | `main.py` | `pipeline_summary.json/.md` |

## 2. Software requirements

* macOS or Linux (Windows: use WSL or the Docker option)
* **Python 3.10 – 3.13**
* **Java 17+** (needed by the Kafka broker)
* Internet access (GitHub API and Kafka download)
* No Docker needed. A `docker-compose.yml` is included only as an optional alternative.

## 3. Installation

```bash
git clone <repository-url>
cd sis2-github-kafka-pipeline

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. API configuration

The GitHub search API works **without a key** (limit: 10 search requests per minute; the
pipeline needs only 6 requests). Optionally, create a token to raise the limit:

```bash
cp .env.example .env
# then put GITHUB_TOKEN=<your token> into .env   (.env is in .gitignore - never commit it)
```

Request used for every domain (see `src/api_client.py`):

| Parameter | Value |
|---|---|
| Endpoint | `https://api.github.com/search/repositories` |
| `q` | `topic:<domain> stars:>=100` |
| `sort` / `order` | `stars` / `desc` |
| `per_page` | `100` |
| `page` | `1` |
| Headers | `Accept: application/vnd.github+json`, `X-GitHub-Api-Version: 2022-11-28` |

6 domains × 100 results = **600 records**. All values can be changed in `.env`.

## 5. Kafka broker setup and startup (no Docker)

**Step 1. Install Java 17+** (skip if `java -version` already shows 17 or newer):

```bash
brew install openjdk@21                         # Homebrew
# or, with Anaconda:  conda install -c conda-forge openjdk=21
```

**Step 2. Download Kafka and format its storage (once):**

```bash
bash scripts/setup_kafka.sh
```

This downloads Apache Kafka 4.1 into `./.kafka/` (ignored by git), creates a KRaft
single-node configuration (broker and controller in one process, no ZooKeeper) and
formats the storage directory `./.kafka/data`.

**Step 3. Start the broker. Keep this terminal open:**

```bash
bash scripts/start_kafka.sh
```

The broker listens on `localhost:9092`.

*Alternative with Docker:* `docker compose up -d`.

## 6. Kafka topic configuration

| Setting | Value |
|---|---|
| Topic name | `github_repositories` (env `KAFKA_TOPIC`) |
| Partitions | 1 (keeps message order, so END_OF_STREAM always comes last) |
| Replication factor | 1 (single broker) |

The producer creates the topic automatically. You can also create or inspect it manually:

```bash
bash scripts/kafka_topic.sh create      # create topic
bash scripts/kafka_topic.sh describe    # partitions, leader
bash scripts/kafka_topic.sh peek        # print the first 3 JSON messages
bash scripts/kafka_topic.sh count       # offsets = number of stored messages
```

## 7. Running the pipeline

Open a **second terminal** (the broker keeps running in the first one), activate the venv, then run either:

### Option A: complete pipeline with one command

```bash
python main.py
```

### Option B: every stage separately, in execution order

```bash
python -m src.api_client     # 1. collect >= 200 records from the GitHub API
python -m src.producer       # 2a. publish the records to Kafka (JSON)
python -m src.consumer       # 2b. consume the messages (stops at END_OF_STREAM)
python -m src.validator      # 3. Pandera validation (+ 4 deliberately invalid test records)
python -m src.cleaner        # 4. cleaning & transformation
python -m src.loader --reset # 5. SQLite: load twice and verify idempotency
python -m src.analysis       # 6. EDA and figures
```

Useful flags: `python main.py --skip-collect` reuses the saved raw data with no API calls.
`python main.py --keep-db` keeps the existing database.

## 8. Database generation and verification

`src/loader.py` creates `data/output.db` from `sql/schema.sql`:

* `repositories`: one row per repository, `repo_id INTEGER PRIMARY KEY`, `full_name UNIQUE`, CHECK constraints.
* `repository_topics`: link table with composite `PRIMARY KEY (repo_id, search_topic)`.

Records are written with an **UPSERT** (`INSERT ... ON CONFLICT(repo_id) DO UPDATE`).
The loader loads the same dataset **twice** and compares the row counts and a SHA-256
checksum of the tables. Repeated loading creates no duplicates and does not change the
data.

Verification queries are in `sql/verification_queries.sql`. Their results are saved to
`data/processed/sql_verification_results.txt`. You can also run them manually:

```bash
sqlite3 data/output.db < sql/verification_queries.sql
sqlite3 data/output.db "SELECT COUNT(*) FROM repositories;"
```

## 9. Analysis results and figures

`python -m src.analysis` reads the data **from SQLite** and creates:

| Figure | Content |
|---|---|
| `figures/fig1_stars_distribution.png` | Histogram of stars (log scale) |
| `figures/fig2_top_languages.png` | Top-10 languages by number of repositories |
| `figures/fig3_stars_by_topic_boxplot.png` | Stars per technology domain |
| `figures/fig4_stars_vs_forks.png` | Stars vs forks scatter (log-log) |
| `figures/fig5_created_per_year.png` | Creation year by domain |
| `figures/fig6_topic_tier_heatmap.png` | Pivot table domain × popularity tier |

The findings are written to `data/processed/analysis_summary.md`.

## 10. Expected output

The end of `python main.py` prints the verification summary (the exact numbers depend
on the day of collection):

```
PIPELINE VERIFICATION SUMMARY
  collected api records             600
  published to kafka                600
  consumed from kafka               600
  test records added                  4
  validated total                   604
  valid records                     ...
  invalid records                   ...   (4 test records + real invalid rows, if any)
  records after cleaning            ...   (< valid: repositories found by several topics are merged)
  records stored in sqlite          ...   (= records after cleaning)
  [OK] at_least_200_collected ... [OK] idempotent_reload ... [OK] no_duplicates_in_db
All checks passed: True
```

## 11. How to verify successful execution

1. `data/processed/pipeline_summary.md`: every automatic check shows `yes`.
2. `data/processed/producer_report.json` → `published_messages` ≥ 200, `failed_messages` = 0.
3. `data/processed/consumer_report.json` → `records_consumed` = published, `stop_reason` = `END_OF_STREAM received`.
4. `data/processed/validation_report.json` → `all_test_records_detected: true`.
5. `data/processed/load_report.json` → `idempotent: true`, `duplicate_repo_ids_in_db: 0`.
6. `figures/` contains 6 PNG files.
7. `bash scripts/kafka_topic.sh peek` shows the JSON messages stored in Kafka.

## 12. Error handling (summary)

| Component | Handled situations |
|---|---|
| API client | timeouts, connection errors, HTTP 5xx (retry with exponential back-off), 403/429 rate limit (waits for reset), 422 invalid query, invalid JSON, fewer than 200 records |
| Producer | broker unreachable (clear error), topic creation, local queue full (`BufferError`), per-message delivery callbacks, serialisation errors, flush timeout |
| Consumer | broker errors, malformed JSON, messages from other runs, duplicate deliveries, three termination conditions (END_OF_STREAM / idle timeout / max runtime) |
| Validator | all rule violations collected with `lazy=True`; invalid rows quarantined, not dropped silently |
| Loader | transaction with rollback on error, UPSERT against duplicates, CHECK constraints |

## 13. Repository structure

```
├── README.md
├── requirements.txt
├── main.py                     # runs the complete pipeline
├── .env.example                # safe example configuration
├── docker-compose.yml          # optional Kafka via Docker
├── scripts/                    # Kafka setup / start / topic tools
├── sql/
│   ├── schema.sql
│   └── verification_queries.sql
├── src/
│   ├── config.py   utils.py
│   ├── api_client.py  producer.py  consumer.py
│   ├── validator.py   cleaner.py   loader.py   analysis.py
├── data/
│   ├── raw/                    # original API data (raw_data.csv, api_responses/)
│   ├── processed/              # stage outputs and reports
│   └── output.db
├── figures/
├── docs/data_dictionary.md
└── report/project_report.pdf
```

## 14. References

* GitHub REST API: https://docs.github.com/en/rest/search/search#search-repositories
* Apache Kafka quickstart: https://kafka.apache.org/quickstart
* confluent-kafka-python: https://docs.confluent.io/kafka-clients/python/current/overview.html
* Pandera: https://pandera.readthedocs.io
* pandas: https://pandas.pydata.org/docs/ · NumPy: https://numpy.org/doc/
* SQLite UPSERT: https://www.sqlite.org/lang_upsert.html
* Seaborn: https://seaborn.pydata.org · Matplotlib: https://matplotlib.org
