"""
Create FTSFR-standardized datasets from commodity returns data.

Outputs:
- ftsfr_commodities_returns.parquet: Commodity returns in long format (unique_id, ds, y)
"""

import sys

sys.path.insert(1, "./src/")

import calc_commodities_returns
import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"


def main():
    """Create FTSFR datasets from commodity returns."""
    print("Calculating commodity returns...")
    df = calc_commodities_returns.load_commodities_returns(data_dir=DATA_DIR)

    print(f"Unique commodities: {df['unique_id'].nunique()}")
    print(f"Total records: {len(df)}")

    df.reset_index(inplace=True, drop=True)
    df = df.dropna()

    print("Saving FTSFR dataset...")
    df.to_parquet(DATA_DIR / "ftsfr_commodities_returns.parquet")

    print(f"\nFinal dataset: {len(df)} records")
    print("\nDone!")


if __name__ == "__main__":
    main()
