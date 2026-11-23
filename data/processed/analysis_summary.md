# EDA summary (generated automatically from data/output.db)

* Dataset: **510 unique repositories**, 35 columns.
* Stars: mean 21,594, median 11,988, max 260,770 (skewness 3.951) -> heavily right-skewed, so log scales are used.

## Findings per research question
1. **RQ1 (languages):** Python is the most common language with 166 repositories (32.5%). Top-3: Python, Go, Unknown. (Figure 2)
2. **RQ2 (domains):** machine-learning has the highest median stars (31,614); blockchain the lowest (5,472). (Figure 3)
3. **RQ3 (stars vs forks):** Spearman correlation = 0.784, Pearson on log values = 0.774 -> a strong positive relationship between stars and forks. (Figure 4)
4. **RQ4 (creation year):** most of the collected repositories were created in 2018 (66 repositories). (Figure 5)
5. **RQ5 (tiers & activity):** tiers = {'Popular (<10k)': 216, 'High (10k-25k)': 153, 'Very high (25k-50k)': 95, 'Mega (50k+)': 46}; 234 repositories (45.88%) are active and popular (10,000+ stars, pushed within 365 days, not archived, not a fork); 29 are archived. (Figures 1, 6)
6. **Owners:** {'Organization': 64.51, 'User': 35.49} (% of repositories by owner type).

## Figures
* `figures/fig1_stars_distribution.png`
* `figures/fig2_top_languages.png`
* `figures/fig3_stars_by_topic_boxplot.png`
* `figures/fig4_stars_vs_forks.png`
* `figures/fig5_created_per_year.png`
* `figures/fig6_topic_tier_heatmap.png`
