"""
Stage 1 - Data collection from the GitHub REST API.

Endpoint : GET https://api.github.com/search/repositories
Query    : q="topic:<domain> stars:>=<MIN_STARS>", sort=stars, order=desc,
           per_page=100, page=1..PAGES_PER_TOPIC

For every technology domain (GitHub topic) the most-starred repositories are
requested. Each repository is flattened into one record that contains
numerical (stars, forks, issues, size), categorical (language, owner type,
license, topic) and date/time (created_at, updated_at, pushed_at) fields.

The original API responses are preserved untouched in data/raw/api_responses/
and the flattened (still uncleaned) records in data/raw/raw_data.csv and
data/raw/raw_records.json.

Run standalone:  python -m src.api_client
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from src import config
from src.utils import banner, write_json

logger = config.setup_logging("api_client")


class GitHubAPIError(RuntimeError):
    """Raised when the GitHub API cannot return usable data."""


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": config.GITHUB_API_VERSION,
        "User-Agent": "sis2-data-pipeline-student-project",
    })
    if config.GITHUB_TOKEN:
        session.headers["Authorization"] = f"Bearer {config.GITHUB_TOKEN}"
        logger.info("Using GITHUB_TOKEN from environment (higher rate limit).")
    else:
        logger.info("No GITHUB_TOKEN set - using unauthenticated requests (10 search requests/min).")
    return session


def build_query_params(topic: str, page: int) -> dict[str, Any]:
    return {
        "q": f"topic:{topic} stars:>={config.MIN_STARS}",
        "sort": config.SORT_BY,
        "order": config.SORT_ORDER,
        "per_page": config.RESULTS_PER_PAGE,
        "page": page,
    }


def _rate_limit_wait_seconds(response: requests.Response) -> float | None:
    """Return how long to wait if the response is a rate-limit response."""
    retry_after = response.headers.get("Retry-After")
    if retry_after and retry_after.isdigit():
        return float(retry_after)
    if response.headers.get("X-RateLimit-Remaining") == "0":
        reset = response.headers.get("X-RateLimit-Reset")
        if reset and reset.isdigit():
            return max(float(reset) - time.time(), 0) + 1
    return None


def fetch_search_page(session: requests.Session, topic: str, page: int) -> dict[str, Any]:
    """Send one search request with retries and explicit error handling."""
    params = build_query_params(topic, page)
    last_error = "unknown error"

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = session.get(config.GITHUB_SEARCH_ENDPOINT, params=params,
                                   timeout=config.REQUEST_TIMEOUT_SECONDS)
        except requests.exceptions.Timeout:
            last_error = f"timeout after {config.REQUEST_TIMEOUT_SECONDS}s"
        except requests.exceptions.ConnectionError as exc:
            last_error = f"connection error: {exc}"
        except requests.exceptions.RequestException as exc:
            last_error = f"request error: {exc}"
        else:
            status = response.status_code
            if status == 200:
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise GitHubAPIError(f"Invalid JSON for topic={topic} page={page}: {exc}") from exc
                if "items" not in payload:
                    raise GitHubAPIError(f"Unexpected response structure (no 'items') for {topic}")
                logger.info("topic=%-17s page=%d -> HTTP 200, %d items (total_count=%s, rate-limit remaining=%s)",
                            topic, page, len(payload["items"]), payload.get("total_count"),
                            response.headers.get("X-RateLimit-Remaining"))
                return payload
            if status in (403, 429):
                wait = _rate_limit_wait_seconds(response)
                if wait is not None and wait <= 75:
                    logger.warning("Rate limit hit (HTTP %d). Waiting %.0f s before retry...", status, wait)
                    time.sleep(wait)
                    continue
                raise GitHubAPIError(f"HTTP {status}: rate limit / forbidden. Set GITHUB_TOKEN or retry later. "
                                     f"Message: {response.text[:200]}")
            if status == 422:
                raise GitHubAPIError(f"HTTP 422 - invalid query for topic={topic}: {response.text[:200]}")
            if 500 <= status < 600:
                last_error = f"server error HTTP {status}"
            else:
                raise GitHubAPIError(f"Unsuccessful response HTTP {status}: {response.text[:200]}")

        backoff = 2 ** attempt
        logger.warning("Attempt %d/%d failed for topic=%s page=%d (%s). Retrying in %ds...",
                       attempt, config.MAX_RETRIES, topic, page, last_error, backoff)
        time.sleep(backoff)

    raise GitHubAPIError(f"All {config.MAX_RETRIES} attempts failed for topic={topic} page={page}: {last_error}")


def flatten_repository(item: dict[str, Any], topic: str, rank: int, collected_at: str) -> dict[str, Any]:
    """Select the useful fields of one repository object (no cleaning yet)."""
    owner = item.get("owner") or {}
    license_info = item.get("license") or {}
    return {
        "repo_id": item.get("id"),
        "full_name": item.get("full_name"),
        "repo_name": item.get("name"),
        "owner_login": owner.get("login"),
        "owner_type": owner.get("type"),
        "description": item.get("description"),
        "language": item.get("language"),
        "license_key": license_info.get("spdx_id"),
        "stargazers_count": item.get("stargazers_count"),
        "forks_count": item.get("forks_count"),
        "open_issues_count": item.get("open_issues_count"),
        "size_kb": item.get("size"),
        "topics_count": len(item.get("topics") or []),
        "is_fork": item.get("fork"),
        "is_archived": item.get("archived"),
        "default_branch": item.get("default_branch"),
        "html_url": item.get("html_url"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "pushed_at": item.get("pushed_at"),
        "search_topic": topic,
        "search_rank": rank,
        "collected_at": collected_at,
    }


def collect_repositories() -> list[dict[str, Any]]:
    config.ensure_directories()
    session = build_session()
    collected_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    records: list[dict[str, Any]] = []
    request_log: list[dict[str, Any]] = []
    failed_requests = 0

    for topic in config.SEARCH_TOPICS:
        rank = 0
        for page in range(1, config.PAGES_PER_TOPIC + 1):
            params = build_query_params(topic, page)
            try:
                payload = fetch_search_page(session, topic, page)
            except GitHubAPIError as exc:
                failed_requests += 1
                logger.error("Skipping topic=%s page=%d: %s", topic, page, exc)
                request_log.append({"topic": topic, "page": page, "params": params,
                                    "status": "failed", "error": str(exc)})
                break

            # Preserve the original, unmodified API response
            write_json(config.API_RESPONSES_DIR / f"{topic}_page{page}.json", payload)
            items = payload.get("items", [])
            request_log.append({"topic": topic, "page": page, "params": params, "status": "ok",
                                "items": len(items), "total_count": payload.get("total_count"),
                                "incomplete_results": payload.get("incomplete_results")})
            for item in items:
                rank += 1
                records.append(flatten_repository(item, topic, rank, collected_at))
            if len(items) < config.RESULTS_PER_PAGE:
                break
            time.sleep(config.PAUSE_BETWEEN_REQUESTS_SECONDS)
        time.sleep(config.PAUSE_BETWEEN_REQUESTS_SECONDS)

    if len(records) < config.MIN_REQUIRED_RECORDS:
        raise GitHubAPIError(f"Only {len(records)} records collected; at least "
                             f"{config.MIN_REQUIRED_RECORDS} are required. Check network / rate limit.")

    write_json(config.RAW_JSON_PATH, records)
    pd.DataFrame(records).to_csv(config.RAW_CSV_PATH, index=False)
    report = {
        "endpoint": config.GITHUB_SEARCH_ENDPOINT,
        "collected_at": collected_at,
        "topics": config.SEARCH_TOPICS,
        "min_stars": config.MIN_STARS,
        "per_page": config.RESULTS_PER_PAGE,
        "pages_per_topic": config.PAGES_PER_TOPIC,
        "total_records": len(records),
        "unique_repo_ids": len({r["repo_id"] for r in records}),
        "failed_requests": failed_requests,
        "requests": request_log,
    }
    write_json(config.COLLECTION_REPORT_PATH, report)
    logger.info("Collected %d records (%d unique repositories) from %d topics. Saved to %s",
                len(records), report["unique_repo_ids"], len(config.SEARCH_TOPICS), config.RAW_CSV_PATH)
    return records


def main() -> int:
    print(banner("STAGE 1 - GitHub REST API data collection"))
    try:
        records = collect_repositories()
    except GitHubAPIError as exc:
        logger.error("Data collection failed: %s", exc)
        return 1
    df = pd.DataFrame(records)
    print(f"\nCollected records: {len(df)} | columns: {df.shape[1]}")
    print(df[["repo_id", "full_name", "language", "stargazers_count", "created_at", "search_topic"]]
          .head(5).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
