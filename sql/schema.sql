-- SIS 2 - SQLite3 schema
-- One row per unique GitHub repository + a link table for the topics it matched.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS repositories (
    repo_id               INTEGER PRIMARY KEY,                -- GitHub repository id (natural key)
    full_name             TEXT    NOT NULL UNIQUE,            -- owner/repository
    repo_name             TEXT    NOT NULL,
    owner_login           TEXT    NOT NULL,
    owner_type            TEXT    NOT NULL CHECK (owner_type IN ('User', 'Organization')),
    description           TEXT,
    language              TEXT    NOT NULL,
    license_key           TEXT    NOT NULL,
    stargazers_count      INTEGER NOT NULL CHECK (stargazers_count >= 0),
    forks_count           INTEGER NOT NULL CHECK (forks_count >= 0),
    open_issues_count     INTEGER NOT NULL CHECK (open_issues_count >= 0),
    size_kb               INTEGER NOT NULL CHECK (size_kb >= 0),
    topics_count          INTEGER NOT NULL,
    is_fork               INTEGER NOT NULL CHECK (is_fork IN (0, 1)),
    is_archived           INTEGER NOT NULL CHECK (is_archived IN (0, 1)),
    default_branch        TEXT,
    html_url              TEXT    NOT NULL,
    created_at            TEXT    NOT NULL,                   -- ISO-8601 UTC
    updated_at            TEXT    NOT NULL,
    pushed_at             TEXT,
    primary_search_topic  TEXT    NOT NULL,
    search_topics         TEXT    NOT NULL,                   -- all matched topics, '|' separated
    matched_topics_count  INTEGER NOT NULL,
    stars_log             REAL,
    forks_to_stars_ratio  REAL,
    issues_per_1k_stars   REAL,
    repo_age_days         INTEGER,
    days_since_last_push  INTEGER,
    stars_per_year        REAL,
    popularity_tier       TEXT,
    is_star_outlier       INTEGER CHECK (is_star_outlier IN (0, 1)),
    is_active_popular     INTEGER CHECK (is_active_popular IN (0, 1)),
    created_year          INTEGER,
    collected_at          TEXT    NOT NULL,
    first_loaded_at       TEXT    NOT NULL                    -- set once, never overwritten
);

CREATE TABLE IF NOT EXISTS repository_topics (
    repo_id       INTEGER NOT NULL REFERENCES repositories (repo_id) ON DELETE CASCADE,
    search_topic  TEXT    NOT NULL,
    PRIMARY KEY (repo_id, search_topic)                       -- prevents duplicate links
);

CREATE INDEX IF NOT EXISTS idx_repositories_language ON repositories (language);
CREATE INDEX IF NOT EXISTS idx_repositories_topic    ON repositories (primary_search_topic);
