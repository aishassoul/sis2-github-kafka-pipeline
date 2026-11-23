"""
Stage 6 - Exploratory data analysis (EDA) and visualisation.

The data is read back FROM THE SQLITE DATABASE (not from CSV), which proves that
the stored data is complete and usable.

Research questions:
  RQ1  Which programming languages dominate popular repositories across domains?
  RQ2  Which technology domain has the most popular repositories (median stars)?
  RQ3  How strongly are stars and forks related?
  RQ4  When were today's popular repositories created?
  RQ5  How are popularity tiers distributed across domains (and how many are actively maintained)?

Outputs: figures/*.png, data/processed/eda_statistics.json, data/processed/analysis_summary.md

Run standalone:  python -m src.analysis
"""

from __future__ import annotations

import sqlite3
from typing import Any

import matplotlib

matplotlib.use("Agg")                      # render to files, no window required
import matplotlib.pyplot as plt          # noqa: E402
from matplotlib.ticker import MaxNLocator  # noqa: E402
import numpy as np                       # noqa: E402
import pandas as pd                      # noqa: E402
import seaborn as sns                    # noqa: E402

from src import config                   # noqa: E402
from src.utils import banner, write_json # noqa: E402

logger = config.setup_logging("analysis")
sns.set_theme(style="whitegrid", context="notebook")
TIER_ORDER = ["Popular (<10k)", "High (10k-25k)", "Very high (25k-50k)", "Mega (50k+)"]


def load_from_database() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not config.DB_PATH.exists():
        raise FileNotFoundError(f"{config.DB_PATH} not found. Run the loader first.")
    with sqlite3.connect(config.DB_PATH) as connection:
        repos = pd.read_sql_query("SELECT * FROM repositories", connection,
                                  parse_dates=["created_at", "updated_at", "pushed_at", "collected_at"])
        topics = pd.read_sql_query(
            "SELECT rt.search_topic, r.repo_id, r.stargazers_count, r.forks_count, r.language "
            "FROM repository_topics rt JOIN repositories r USING (repo_id)", connection)
    return repos, topics


def spearman_correlation(x: pd.Series, y: pd.Series) -> float:
    """Spearman rho = Pearson correlation of the ranks (no SciPy needed)."""
    return float(np.corrcoef(x.rank().to_numpy(), y.rank().to_numpy())[0, 1])


def _save(fig: plt.Figure, filename: str) -> str:
    path = config.FIGURES_DIR / filename
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return str(path.relative_to(config.PROJECT_ROOT))


def describe_dataset(df: pd.DataFrame, topics: pd.DataFrame) -> dict[str, Any]:
    numeric_columns = ["stargazers_count", "forks_count", "open_issues_count", "size_kb",
                       "repo_age_days", "days_since_last_push", "stars_per_year", "forks_to_stars_ratio"]
    describe = df[numeric_columns].describe().round(2)
    categorical = {
        column: {"unique": int(df[column].nunique()),
                 "top": str(df[column].mode().iloc[0]),
                 "top_frequency": int(df[column].value_counts().iloc[0])}
        for column in ["language", "owner_type", "license_key", "primary_search_topic", "popularity_tier"]
    }
    stats = {
        "shape": {"rows": int(df.shape[0]), "columns": int(df.shape[1])},
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "numeric_describe": describe.to_dict(),
        "skewness": df[numeric_columns].skew().round(3).to_dict(),
        "categorical_summary": categorical,
        "language_top10": df["language"].value_counts().head(10).to_dict(),
        "owner_type_share_percent": (df["owner_type"].value_counts(normalize=True) * 100).round(2).to_dict(),
        "license_top5": df["license_key"].value_counts().head(5).to_dict(),
        "topic_link_counts": topics["search_topic"].value_counts().to_dict(),
        "topic_median_stars": topics.groupby("search_topic")["stargazers_count"].median()
                                    .sort_values(ascending=False).to_dict(),
        "tier_counts": df["popularity_tier"].value_counts().reindex(TIER_ORDER, fill_value=0).to_dict(),
        "active_popular_count": int(df["is_active_popular"].sum()),
        "active_popular_percent": round(float(df["is_active_popular"].mean() * 100), 2),
        "archived_count": int(df["is_archived"].sum()),
        "pearson_log_stars_log_forks": round(float(np.corrcoef(np.log1p(df["stargazers_count"]),
                                                                np.log1p(df["forks_count"]))[0, 1]), 3),
        "spearman_stars_forks": round(spearman_correlation(df["stargazers_count"], df["forks_count"]), 3),
        "created_per_year": df["created_year"].value_counts().sort_index().to_dict(),
    }
    return stats


def figure_stars_distribution(df: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(9, 5))
    log_stars = np.log10(df["stargazers_count"].clip(lower=1))
    sns.histplot(log_stars, bins=30, color="#4C72B0", ax=ax, label="Repositories")
    median = np.log10(df["stargazers_count"].median())
    ax.axvline(median, color="#C44E52", linestyle="--", linewidth=2,
               label=f"Median = {df['stargazers_count'].median():,.0f} stars")
    ticks = np.arange(np.floor(log_stars.min()), np.ceil(log_stars.max()) + 1)
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{10 ** t:,.0f}" for t in ticks])
    ax.set_title("Figure 1. Distribution of repository stars (log scale)")
    ax.set_xlabel("Stars (log10 scale)")
    ax.set_ylabel("Number of repositories")
    ax.legend()
    return _save(fig, "fig1_stars_distribution.png")


def figure_top_languages(df: pd.DataFrame) -> str:
    top = (df.groupby("language").agg(repo_count=("repo_id", "count"),
                                      median_stars=("stargazers_count", "median"))
           .sort_values("repo_count", ascending=False).head(10).reset_index())
    fig, ax = plt.subplots(figsize=(9, 5.5))
    sns.barplot(data=top, y="language", x="repo_count", color="#55A868", ax=ax)
    for i, row in top.iterrows():
        ax.text(row["repo_count"] + 0.5, i, f"median {row['median_stars']:,.0f} stars", va="center", fontsize=9)
    ax.set_title("Figure 2. Top 10 programming languages by number of repositories")
    ax.set_xlabel("Number of repositories")
    ax.set_ylabel("Primary language")
    ax.set_xlim(0, top["repo_count"].max() * 1.3)
    return _save(fig, "fig2_top_languages.png")


def figure_stars_by_topic(topics: pd.DataFrame) -> str:
    order = topics.groupby("search_topic")["stargazers_count"].median().sort_values(ascending=False).index
    fig, ax = plt.subplots(figsize=(10, 5.5))
    sns.boxplot(data=topics, x="search_topic", y="stargazers_count", order=order, hue="search_topic",
                palette="Set2", legend=False, showfliers=True, ax=ax)
    ax.set_yscale("log")
    ax.set_title("Figure 3. Stars by technology domain (search topic)")
    ax.set_xlabel("Technology domain (GitHub topic)")
    ax.set_ylabel("Stars (log scale)")
    ax.tick_params(axis="x", rotation=20)
    return _save(fig, "fig3_stars_by_topic_boxplot.png")


def figure_stars_vs_forks(df: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(9, 6))
    sns.scatterplot(data=df, x="stargazers_count", y="forks_count", hue="popularity_tier",
                    hue_order=[t for t in TIER_ORDER if t in set(df["popularity_tier"])],
                    palette="viridis", alpha=0.75, s=35, ax=ax)
    ax.set_xscale("log")
    ax.set_yscale("log")
    rho = spearman_correlation(df["stargazers_count"], df["forks_count"])
    ax.set_title(f"Figure 4. Stars vs forks (log-log), Spearman ρ = {rho:.2f}")
    ax.set_xlabel("Stars (log scale)")
    ax.set_ylabel("Forks (log scale)")
    ax.legend(title="Popularity tier")
    return _save(fig, "fig4_stars_vs_forks.png")


def figure_created_per_year(topics: pd.DataFrame, df: pd.DataFrame) -> str:
    merged = topics.merge(df[["repo_id", "created_year"]], on="repo_id")
    per_year = merged.pivot_table(index="created_year", columns="search_topic", values="repo_id",
                                  aggfunc="count", fill_value=0)
    fig, ax = plt.subplots(figsize=(10, 5.5))
    per_year.plot(ax=ax, marker="o", linewidth=1.8)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_title("Figure 5. Creation year of popular repositories by domain")
    ax.set_xlabel("Year the repository was created")
    ax.set_ylabel("Number of repositories")
    ax.legend(title="Domain", bbox_to_anchor=(1.01, 1), loc="upper left")
    return _save(fig, "fig5_created_per_year.png")


def figure_topic_tier_heatmap(topics: pd.DataFrame, df: pd.DataFrame) -> str:
    merged = topics.merge(df[["repo_id", "popularity_tier"]], on="repo_id")
    pivot = (merged.pivot_table(index="search_topic", columns="popularity_tier", values="repo_id",
                                aggfunc="count", fill_value=0)
             .reindex(columns=TIER_ORDER, fill_value=0))
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="YlGnBu", cbar_kws={"label": "Repositories"}, ax=ax)
    ax.set_title("Figure 6. Repositories per domain and popularity tier (pivot table)")
    ax.set_xlabel("Popularity tier")
    ax.set_ylabel("Technology domain")
    return _save(fig, "fig6_topic_tier_heatmap.png")


def _correlation_strength(rho: float) -> str:
    size = abs(rho)
    label = "very weak" if size < 0.2 else "weak" if size < 0.4 else "moderate" if size < 0.6 \
        else "strong" if size < 0.8 else "very strong"
    return f"a {label} {'positive' if rho >= 0 else 'negative'}"


def write_summary(stats: dict[str, Any], figures: list[str]) -> str:
    lang = stats["language_top10"]
    top_lang, top_lang_n = next(iter(lang.items()))
    rows = stats["shape"]["rows"]
    topic_medians = stats["topic_median_stars"]
    best_topic, best_median = next(iter(topic_medians.items()))
    worst_topic, worst_median = list(topic_medians.items())[-1]
    years = stats["created_per_year"]
    peak_year = max(years, key=years.get)
    stars = stats["numeric_describe"]["stargazers_count"]
    tiers = stats["tier_counts"]

    lines = [
        "# EDA summary (generated automatically from data/output.db)",
        "",
        f"* Dataset: **{rows} unique repositories**, {stats['shape']['columns']} columns.",
        f"* Stars: mean {stars['mean']:,.0f}, median {stars['50%']:,.0f}, max {stars['max']:,.0f} "
        f"(skewness {stats['skewness']['stargazers_count']}) -> heavily right-skewed, so log scales are used.",
        "",
        "## Findings per research question",
        f"1. **RQ1 (languages):** {top_lang} is the most common language with {top_lang_n} repositories "
        f"({top_lang_n / rows * 100:.1f}%). Top-3: {', '.join(list(lang)[:3])}. (Figure 2)",
        f"2. **RQ2 (domains):** {best_topic} has the highest median stars ({best_median:,.0f}); "
        f"{worst_topic} the lowest ({worst_median:,.0f}). (Figure 3)",
        f"3. **RQ3 (stars vs forks):** Spearman correlation = {stats['spearman_stars_forks']}, "
        f"Pearson on log values = {stats['pearson_log_stars_log_forks']} -> "
        f"{_correlation_strength(stats['spearman_stars_forks'])} relationship between stars and forks. "
        "(Figure 4)",
        f"4. **RQ4 (creation year):** most of the collected repositories were created in {peak_year} "
        f"({years[peak_year]} repositories). (Figure 5)",
        f"5. **RQ5 (tiers & activity):** tiers = {tiers}; {stats['active_popular_count']} repositories "
        f"({stats['active_popular_percent']}%) are active and popular ({config.POPULAR_STARS_THRESHOLD:,}+ stars, "
        f"pushed within {config.ACTIVE_PUSH_WINDOW_DAYS} days, "
        f"not archived, not a fork); {stats['archived_count']} are archived. (Figures 1, 6)",
        f"6. **Owners:** {stats['owner_type_share_percent']} (% of repositories by owner type).",
        "",
        "## Figures",
        *[f"* `{f}`" for f in figures],
    ]
    text = "\n".join(lines) + "\n"
    config.ANALYSIS_SUMMARY_PATH.write_text(text, encoding="utf-8")
    return text


def run_analysis() -> dict[str, Any]:
    df, topics = load_from_database()
    stats = describe_dataset(df, topics)
    figures = [
        figure_stars_distribution(df),
        figure_top_languages(df),
        figure_stars_by_topic(topics),
        figure_stars_vs_forks(df),
        figure_created_per_year(topics, df),
        figure_topic_tier_heatmap(topics, df),
    ]
    stats["figures"] = figures
    write_json(config.EDA_REPORT_PATH, stats)
    summary = write_summary(stats, figures)
    logger.info("EDA finished: %d figures saved to %s", len(figures), config.FIGURES_DIR)
    return {"stats": stats, "summary": summary, "df": df}


def main() -> int:
    print(banner("STAGE 6 - Exploratory data analysis"))
    try:
        result = run_analysis()
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 1
    df = result["df"]
    print(f"\nShape: {df.shape}")
    print("\nData types:\n" + df.dtypes.value_counts().to_string())
    print("\nDescriptive statistics:")
    print(df[["stargazers_count", "forks_count", "open_issues_count", "repo_age_days"]].describe().round(1).to_string())
    print("\n" + result["summary"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
