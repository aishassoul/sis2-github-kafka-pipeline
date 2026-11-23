# Data dictionary

Source: GitHub REST API `GET /search/repositories` (one record = one repository returned for one search topic).

## 1. Raw / consumed record (`data/raw/raw_data.csv`, Kafka message `payload`)

| Column | Type | Kind | Source field | Description |
|---|---|---|---|---|
| repo_id | integer | identifier | `id` | Unique GitHub repository id |
| full_name | string | categorical | `full_name` | `owner/repository` |
| repo_name | string | categorical | `name` | Repository name |
| owner_login | string | categorical | `owner.login` | Owner account name |
| owner_type | string | categorical | `owner.type` | `User` or `Organization` |
| description | string | text | `description` | Short description (may be empty) |
| language | string | categorical | `language` | Main programming language (may be missing) |
| license_key | string | categorical | `license.spdx_id` | SPDX license id (may be missing) |
| stargazers_count | integer | numerical | `stargazers_count` | Number of stars |
| forks_count | integer | numerical | `forks_count` | Number of forks |
| open_issues_count | integer | numerical | `open_issues_count` | Open issues + pull requests |
| size_kb | integer | numerical | `size` | Repository size in KB |
| topics_count | integer | numerical | `len(topics)` | Number of topics on the repository |
| is_fork | boolean | categorical | `fork` | Repository is a fork |
| is_archived | boolean | categorical | `archived` | Repository is archived (read-only) |
| default_branch | string | categorical | `default_branch` | Default branch name |
| html_url | string | text | `html_url` | Web URL |
| created_at | datetime (UTC) | date/time | `created_at` | Creation time |
| updated_at | datetime (UTC) | date/time | `updated_at` | Last update of repository object |
| pushed_at | datetime (UTC) | date/time | `pushed_at` | Last push (commit) |
| search_topic | string | categorical | request parameter | Domain (topic) the record was found by |
| search_rank | integer | numerical | position | Rank inside the search result (1 = most stars) |
| collected_at | datetime (UTC) | date/time | pipeline | Collection timestamp |
| kafka_offset / kafka_partition | integer | technical | Kafka | Position of the message in the topic (consumer output only) |

## 2. Columns added during cleaning (`cleaned_repositories.csv`, SQLite `repositories`)

| Column | Type | How it is computed |
|---|---|---|
| primary_search_topic | category | `search_topic` of the best-ranked occurrence |
| search_topics | string | All matched topics joined with `\|` |
| matched_topics_count | integer | Number of topics the repository was found by |
| full_name_normalized | string | `full_name` in lower case (CSV only) |
| stars_log | float | `np.log1p(stars)` |
| forks_to_stars_ratio | float | `forks / stars` (0 if stars = 0) |
| issues_per_1k_stars | float | `open_issues / stars * 1000` |
| repo_age_days | integer | `collected_at - created_at` in days |
| days_since_last_push | integer | `collected_at - pushed_at` in days |
| stars_per_year | float | `stars / age_in_years` |
| stars_zscore | float | `(stars - mean) / std` (CSV only) |
| created_year | integer | Year of `created_at` |
| popularity_tier | category | `np.select`: Popular (<10k), High (10k-25k), Very high (25k-50k), Mega (50k+) |
| is_star_outlier | boolean | Stars outside `Q1 - 1.5·IQR … Q3 + 1.5·IQR` |
| is_active_popular | boolean | stars ≥ 10 000 AND pushed ≤ 365 days ago AND not archived AND not fork |
| first_loaded_at | text | SQLite only: time of first insert (never overwritten) |

## 3. Missing-value conventions after cleaning

| Column | Replacement |
|---|---|
| language | `Unknown` |
| license_key | `NO_LICENSE` (and `NOASSERTION` → `OTHER`) |
| description | empty string |
| pushed_at | `updated_at` |

## 4. SQLite tables

* `repositories`: PRIMARY KEY `repo_id`, UNIQUE `full_name`, CHECK constraints on counts, owner_type and flags.
* `repository_topics (repo_id, search_topic)`: composite PRIMARY KEY, FOREIGN KEY → `repositories.repo_id`.
