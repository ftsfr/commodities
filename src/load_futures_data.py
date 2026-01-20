"""
Load futures data from various sources.
"""

import sys
from pathlib import Path

sys.path.insert(1, "./src/")

import pandas as pd
import pull_bbg_active_commodities
import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"


def load_futures_returns(data_dir=DATA_DIR):
    path = data_dir / "futures_returns.parquet"
    return pd.read_parquet(path)


def load_lme_metals(data_dir=DATA_DIR):
    path = data_dir / "lme_metals.parquet"
    return pd.read_parquet(path)


def load_gsci_data(data_dir=DATA_DIR):
    """Load GSCI data and compute monthly returns."""
    df = pd.read_parquet(data_dir / "gsci_indices.parquet")
    df["Date"] = pd.to_datetime(df["index"])
    df = df.sort_values("Date")
    df = df.drop(columns="index")

    df_monthly = df.groupby(df["Date"].dt.to_period("M")).last().reset_index(drop=True)
    index_cols = [col for col in df_monthly.columns if col.endswith("_PX_LAST")]

    for col in index_cols:
        df_monthly[col + "_Return"] = df_monthly[col].pct_change()

    df_monthly["yyyymm"] = df_monthly["Date"].dt.strftime("%Y%m")
    df_return = df_monthly.drop(columns=index_cols).set_index("yyyymm")

    return df_return


def load_commodities_future(data_dir=DATA_DIR):
    df = pd.read_parquet(data_dir / "commodity_futures.parquet")
    return df


def load_active_commodities(data_dir=DATA_DIR):
    """Load active commodities and compute monthly returns."""
    df = pull_bbg_active_commodities.load_active_commodities_prices(data_dir=data_dir)
    df["Date"] = pd.to_datetime(df["index"])
    df = df.sort_values("Date")
    df = df.drop(columns="index")

    df_monthly = df.groupby(df["Date"].dt.to_period("M")).last().reset_index(drop=True)
    index_cols = [col for col in df_monthly.columns if col.endswith("_PX_LAST")]

    for col in index_cols:
        df_monthly[col + "_Return"] = df_monthly[col].pct_change()

    df_monthly["yyyymm"] = df_monthly["Date"].dt.strftime("%Y%m")
    df_return = df_monthly.drop(columns=index_cols).set_index("yyyymm")

    return df_return
