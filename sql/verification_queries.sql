-- SIS 2 - SQL verification queries
-- Each query starts with a "-- name:" line; src/loader.py runs all of them
-- and saves the results to data/processed/sql_verification_results.txt.
-- They can also be executed manually:  sqlite3 data/output.db < sql/verification_queries.sql

-- name: 1_row_counts
SELECT COUNT(*)                  AS total_rows,
       COUNT(DISTINCT repo_id)   AS distinct_repo_ids,
       COUNT(DISTINCT full_name) AS distinct_full_names
FROM repositories;

-- name: 2_duplicate_repo_ids (expected: no rows)
SELECT repo_id, COUNT(*) AS occurrences
FROM repositories
GROUP BY repo_id
HAVING COUNT(*) > 1;

-- name: 3_missing_required_values (expected: all zeros)
SELECT SUM(full_name IS NULL)        AS null_full_name,
       SUM(language IS NULL)         AS null_language,
       SUM(created_at IS NULL)       AS null_created_at,
       SUM(stargazers_count IS NULL) AS null_stars
FROM repositories;

-- name: 4_value_range_check (expected: 0)
SELECT COUNT(*) AS rows_with_invalid_values
FROM repositories
WHERE stargazers_count < 0 OR forks_count < 0 OR open_issues_count < 0
   OR owner_type NOT IN ('User', 'Organization')
   OR updated_at < created_at;

-- name: 5_top_10_repositories_by_stars
SELECT full_name, language, stargazers_count, forks_count, primary_search_topic
FROM repositories
ORDER BY stargazers_count DESC
LIMIT 10;

-- name: 6_repositories_per_language
SELECT language, COUNT(*) AS repo_count, ROUND(AVG(stargazers_count), 0) AS avg_stars
FROM repositories
GROUP BY language
ORDER BY repo_count DESC
LIMIT 10;

-- name: 7_repositories_per_topic_link_table
SELECT search_topic, COUNT(*) AS linked_repositories
FROM repository_topics
GROUP BY search_topic
ORDER BY linked_repositories DESC;

-- name: 8_orphan_topic_links (expected: 0)
SELECT COUNT(*) AS orphan_links
FROM repository_topics rt
LEFT JOIN repositories r ON r.repo_id = rt.repo_id
WHERE r.repo_id IS NULL;

-- name: 9_popularity_tier_distribution
SELECT popularity_tier, COUNT(*) AS repo_count
FROM repositories
GROUP BY popularity_tier
ORDER BY MIN(stargazers_count);

-- name: 10_repositories_created_per_year
SELECT created_year, COUNT(*) AS repo_count
FROM repositories
GROUP BY created_year
ORDER BY created_year;
