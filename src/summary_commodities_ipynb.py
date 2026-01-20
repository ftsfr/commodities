# %%
"""
# Commodities Returns Summary

This notebook summarizes the commodity returns dataset, which contains monthly
returns for various commodities constructed from GSCI indices and Bloomberg
futures data.

## Data Sources

- **Bloomberg**: GSCI excess return indices, commodity futures prices
- **WRDS**: Futures contract data
- **He-Kelly-Manela**: Commodity factor reference for matching

## Methodology

Commodity returns are matched to He-Kelly-Manela commodity factors using
correlation-based optimal pair matching (Hungarian algorithm).
"""

# %%
import sys
from pathlib import Path

sys.path.insert(1, "./src/")

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"

# %%
"""
## Load Data
"""

# %%
# Load FTSFR dataset
df = pd.read_parquet(DATA_DIR / "ftsfr_commodities_returns.parquet")

print("=== Commodities Returns Dataset ===")
print(f"Shape: {df.shape}")
print(f"Date range: {df['ds'].min()} to {df['ds'].max()}")
print(f"Unique commodities: {df['unique_id'].nunique()}")
print(f"\nCommodities: {sorted(df['unique_id'].unique())}")

# %%
"""
## Summary Statistics
"""

# %%
# Pivot to wide format for statistics
df_wide = df.pivot(index="ds", columns="unique_id", values="y")
print("Commodity Returns Summary Statistics")
print(df_wide.describe().T)

# %%
"""
## Time Series Plot
"""

# %%
fig, ax = plt.subplots(figsize=(14, 8))

for uid in df["unique_id"].unique()[:10]:  # Plot first 10 commodities
    subset = df[df["unique_id"] == uid].sort_values("ds")
    ax.plot(subset["ds"], subset["y"], label=uid, linewidth=0.8, alpha=0.7)

ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
ax.set_xlabel("Date")
ax.set_ylabel("Return")
ax.set_title("Commodity Returns (Sample)")
ax.legend(loc="upper right", fontsize=8)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# %%
"""
## Return Distribution
"""

# %%
fig, ax = plt.subplots(figsize=(10, 6))
ax.hist(df["y"], bins=100, alpha=0.7, color="steelblue")
ax.axvline(df["y"].mean(), color="red", linestyle="--", label=f"Mean: {df['y'].mean():.4f}")
ax.set_xlabel("Return")
ax.set_ylabel("Frequency")
ax.set_title("Distribution of Commodity Returns")
ax.legend()
plt.tight_layout()
plt.show()

# %%
"""
## Correlation Matrix
"""

# %%
if len(df_wide.columns) > 1:
    corr_matrix = df_wide.corr()

    fig, ax = plt.subplots(figsize=(12, 10))
    sns.heatmap(corr_matrix, annot=False, cmap="coolwarm", center=0, ax=ax,
                xticklabels=True, yticklabels=True)
    ax.set_title("Correlation Matrix: Commodity Returns")
    plt.tight_layout()
    plt.show()

# %%
"""
## Monthly Statistics
"""

# %%
df["ds"] = pd.to_datetime(df["ds"])
df["month"] = df["ds"].dt.to_period("M")

monthly_stats = df.groupby("unique_id")["y"].agg(["mean", "std", "min", "max", "count"])
print("Statistics by Commodity:")
print(monthly_stats.sort_values("mean", ascending=False))

# %%
"""
## Data Quality Check
"""

# %%
print("=== Data Quality ===")
print(f"Missing values: {df['y'].isna().sum()}")
print(f"Infinite values: {(~df['y'].apply(lambda x: -1e10 < x < 1e10)).sum()}")

# Per commodity coverage
coverage = df.groupby("unique_id")["ds"].count()
print(f"\nRecords per commodity:")
print(coverage.sort_values())
