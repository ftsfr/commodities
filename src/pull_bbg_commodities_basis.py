"""
pull_bbg_commodities_basis.py

Pulls both futures prices and spot (or spot-proxy) prices for commodities
so basis can be computed: Basis = Futures - Spot.

What this module pulls:
- Generic futures (1st/2nd/3rd nearby) for energy, agriculture, livestock, metals
- LME metals spot and 3-month forward prices for base metals
- GSCI Excess Return indices
- Precious metals USD spot for Gold, Silver, Platinum, Palladium
- Spot proxies for commodities lacking reliable spot series
"""

import sys
from pathlib import Path
import warnings

sys.path.insert(1, "./src/")

import pandas as pd
import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"
END_DATE = pd.Timestamp.today().strftime("%Y-%m-%d")


def _warn_on_sparse_series(
    df: pd.DataFrame,
    tickers: list[str],
    fields: list[str],
    start_date: str,
    end_date: str,
    coverage_threshold: float = 0.7,
) -> None:
    """Raise a warning for each series whose non-null coverage is below threshold."""
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)

    if "index" in df.columns:
        date_values = pd.to_datetime(df["index"], errors="coerce")
    else:
        if isinstance(df.index, pd.DatetimeIndex):
            date_values = df.index
        else:
            date_values = pd.to_datetime(df.index, errors="coerce")

    if df.shape[0] == 0:
        for tkr in tickers:
            for fld in fields:
                warnings.warn(
                    f"Bloomberg series returned empty data: ticker='{tkr}', field='{fld}'",
                    RuntimeWarning,
                )
        return

    is_multi = isinstance(df.columns, pd.MultiIndex)
    expected_cols_flat = {f"{t}_{fld}": (t, fld) for t in tickers for fld in fields}
    expected_cols_multi = {(t, fld): (t, fld) for t in tickers for fld in fields}

    in_range = (date_values >= start_ts) & (date_values <= end_ts)

    if in_range.sum() == 0:
        for tkr in tickers:
            for fld in fields:
                warnings.warn(
                    f"No rows returned in requested date range for ticker='{tkr}', field='{fld}'",
                    RuntimeWarning,
                )
        return

    if is_multi:
        for key in expected_cols_multi:
            if key not in df.columns:
                tkr, fld = key
                warnings.warn(
                    f"Bloomberg series missing from result: ticker='{tkr}', field='{fld}'",
                    RuntimeWarning,
                )
                continue
            series = df.loc[in_range, key]
            total = series.shape[0]
            non_null = series.notna().sum()
            coverage = (non_null / total) if total else 0.0
            if coverage < coverage_threshold:
                tkr, fld = key
                warnings.warn(
                    f"Low coverage ({coverage:.1%}) for ticker='{tkr}', field='{fld}' between {start_ts.date()} and {end_ts.date()}",
                    RuntimeWarning,
                )
    else:
        for col_flat, (tkr, fld) in expected_cols_flat.items():
            if col_flat not in df.columns:
                warnings.warn(
                    f"Bloomberg series missing from result: ticker='{tkr}', field='{fld}'",
                    RuntimeWarning,
                )
                continue
            series = df.loc[in_range, col_flat]
            total = series.shape[0]
            non_null = series.notna().sum()
            coverage = (non_null / total) if total else 0.0
            if coverage < coverage_threshold:
                warnings.warn(
                    f"Low coverage ({coverage:.1%}) for ticker='{tkr}', field='{fld}' between {start_ts.date()} and {end_ts.date()}",
                    RuntimeWarning,
                )


def pull_commodity_futures(start_date="1950-01-01", end_date=END_DATE):
    """Fetch historical commodity futures prices from Bloomberg."""
    from xbbg import blp

    commodity_futures_tickers = [
        # Energy
        "CO1 Comdty", "CO2 Comdty", "CO3 Comdty",  # Brent Crude
        "QS1 Comdty", "QS2 Comdty", "QS3 Comdty",  # Gasoil
        "CL1 Comdty", "CL2 Comdty", "CL3 Comdty",  # WTI Crude
        "RB1 Comdty", "RB2 Comdty", "RB3 Comdty",  # Gasoline (RBOB)
        "XB1 Comdty", "XB2 Comdty", "XB3 Comdty",  # RBOB Gasoline
        "HO1 Comdty", "HO2 Comdty", "HO3 Comdty",  # Heating Oil
        "NG1 Comdty", "NG2 Comdty", "NG3 Comdty",  # Natural Gas
        "HU1 Comdty", "HU2 Comdty", "HU3 Comdty",  # Unleaded Gas
        # Agriculture
        "CT1 Comdty", "CT2 Comdty", "CT3 Comdty",  # Cotton
        "KC1 Comdty", "KC2 Comdty", "KC3 Comdty",  # Coffee
        "CC1 Comdty", "CC2 Comdty", "CC3 Comdty",  # Cocoa
        "SB1 Comdty", "SB2 Comdty", "SB3 Comdty",  # Sugar
        "LB1 Comdty", "LB2 Comdty", "LB3 Comdty",  # Lumber
        "O 1 Comdty", "O 2 Comdty", "O 3 Comdty",  # Oats
        "JO1 Comdty", "JO2 Comdty", "JO3 Comdty",  # Orange Juice
        "RR1 Comdty", "RR2 Comdty", "RR3 Comdty",  # Rough Rice
        "SM1 Comdty", "SM2 Comdty", "SM3 Comdty",  # Soybean Meal
        "WC1 Comdty", "WC2 Comdty", "WC3 Comdty",  # Canola
        "S 1 Comdty", "S 2 Comdty", "S 3 Comdty",  # Soybeans
        "KW1 Comdty", "KW2 Comdty", "KW3 Comdty",  # Kansas Wheat
        "C 1 Comdty", "C 2 Comdty", "C 3 Comdty",  # Corn
        "W 1 Comdty", "W 2 Comdty", "W 3 Comdty",  # Wheat
        # Livestock
        "LH1 Comdty", "LH2 Comdty", "LH3 Comdty",  # Lean Hogs
        "FC1 Comdty", "FC2 Comdty", "FC3 Comdty",  # Feeder Cattle
        "LC1 Comdty", "LC2 Comdty", "LC3 Comdty",  # Live Cattle
        # Metals
        "AL1 Comdty", "AL2 Comdty", "AL3 Comdty",  # Aluminium
        "HG1 Comdty", "HG2 Comdty", "HG3 Comdty",  # Copper
        "GC1 Comdty", "GC2 Comdty", "GC3 Comdty",  # Gold
        "SI1 Comdty", "SI2 Comdty", "SI3 Comdty",  # Silver
        "PA1 Comdty", "PA2 Comdty", "PA3 Comdty",  # Palladium
        "PL1 Comdty", "PL2 Comdty", "PL3 Comdty",  # Platinum
    ]

    fields = ["PX_LAST"]

    df = blp.bdh(
        tickers=commodity_futures_tickers,
        flds=fields,
        start_date=start_date,
        end_date=end_date,
    )

    if not df.empty and isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{t[0]}_{t[1]}" for t in df.columns]
        df.reset_index(inplace=True)

    _warn_on_sparse_series(
        df=df,
        tickers=commodity_futures_tickers,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
    )

    return df


def pull_lme_metals(start_date="1950-01-01", end_date=END_DATE):
    """Fetch LME metals spot and 3-month forward prices from Bloomberg."""
    from xbbg import blp

    lme_metals_tickers = [
        "LMAHDY Comdty", "LMAHDS03 Comdty",  # Aluminum
        "LMNIDY Comdty", "LMNIDS03 Comdty",  # Nickel
        "LMPBDY Comdty", "LMPBDS03 Comdty",  # Lead
        "LMZSDY Comdty", "LMZSDS03 Comdty",  # Zinc
        "LMCADY Comdty", "LMCADS03 Comdty",  # Copper
    ]

    fields = ["PX_LAST"]

    df = blp.bdh(
        tickers=lme_metals_tickers,
        flds=fields,
        start_date=start_date,
        end_date=end_date,
    )

    if not df.empty and isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{t[0]}_{t[1]}" for t in df.columns]
        df.reset_index(inplace=True)

    _warn_on_sparse_series(
        df=df,
        tickers=lme_metals_tickers,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
    )

    return df


def pull_precious_metals_spot(start_date="1950-01-01", end_date=END_DATE):
    """Fetch USD spot prices for precious metals."""
    from xbbg import blp

    tickers = [
        "XAUUSD Curncy",  # Gold spot USD
        "XAGUSD Curncy",  # Silver spot USD
        "XPTUSD Curncy",  # Platinum spot USD
        "XPDUSD Curncy",  # Palladium spot USD
    ]

    fields = ["PX_LAST"]

    df = blp.bdh(tickers=tickers, flds=fields, start_date=start_date, end_date=end_date)

    if not df.empty and isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{t[0]}_{t[1]}" for t in df.columns]
        df.reset_index(inplace=True)

    _warn_on_sparse_series(
        df=df,
        tickers=tickers,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
    )

    return df


def build_spot_proxies_from_futures_df(df_futures: pd.DataFrame) -> pd.DataFrame:
    """Construct spot proxies by extracting 1st generic futures series."""
    if df_futures is None or df_futures.empty:
        return df_futures

    date_col = "index" if "index" in df_futures.columns else df_futures.columns[0]
    proxy_cols = [
        c
        for c in df_futures.columns
        if c != date_col and c.endswith("1 Comdty_PX_LAST")
    ]

    cols = [date_col] + proxy_cols
    return df_futures[cols].copy()


def pull_gsci_indices(start_date="1950-01-01", end_date=END_DATE):
    """Fetch Goldman Sachs Commodity Index excess returns from Bloomberg."""
    from xbbg import blp

    gsci_indices_tickers = [
        "SPGCBRP Index",  # Brent Crude Oil
        "SPGCGOP Index",  # Gasoil
        "SPGCCLP Index",  # WTI Crude
        "SPGCHUP Index",  # Unleaded Gasoline
        "SPGCHOP Index",  # Heating Oil
        "SPGCNGP Index",  # Natural Gas
        "SPGCCTP Index",  # Cotton
        "SPGCKCP Index",  # Coffee
        "SPGCCCP Index",  # Cocoa
        "SPGCSBP Index",  # Sugar
        "SPGCSOP Index",  # Soybeans
        "SPGCKWP Index",  # Kansas Wheat
        "SPGCCNP Index",  # Corn
        "SPGCWHP Index",  # Wheat
        "SPGCLHP Index",  # Lean Hogs
        "SPGCFCP Index",  # Feeder Cattle
        "SPGCLCP Index",  # Live Cattle
        "SPGCGCP Index",  # Gold
        "SPGCSIP Index",  # Silver
        "SPGCIAP Index",  # Aluminum
        "SPGCIKP Index",  # Nickel
        "SPGCILP Index",  # Lead
        "SPGCIZP Index",  # Zinc
        "SPGCICP Index",  # Copper
    ]

    fields = ["PX_LAST"]

    df = blp.bdh(
        tickers=gsci_indices_tickers,
        flds=fields,
        start_date=start_date,
        end_date=end_date,
    )

    if not df.empty and isinstance(df.columns, pd.MultiIndex):
        df.columns = [f"{t[0]}_{t[1]}" for t in df.columns]
        df.reset_index(inplace=True)

    _warn_on_sparse_series(
        df=df,
        tickers=gsci_indices_tickers,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
    )

    return df


def load_commodity_futures(data_dir=DATA_DIR):
    """Load commodity futures prices from parquet file."""
    path = data_dir / "commodity_futures.parquet"
    return pd.read_parquet(path)


def load_lme_metals(data_dir=DATA_DIR):
    """Load LME metals spot and 3-month forward prices from parquet file."""
    path = data_dir / "lme_metals.parquet"
    return pd.read_parquet(path)


def load_precious_metals_spot(data_dir=DATA_DIR):
    """Load precious metals USD spot prices from parquet file."""
    path = data_dir / "precious_metals_spot.parquet"
    return pd.read_parquet(path)


def load_gsci_indices(data_dir=DATA_DIR):
    """Load Goldman Sachs Commodity Index excess returns from parquet file."""
    path = data_dir / "gsci_indices.parquet"
    return pd.read_parquet(path)


def load_commodity_spot_proxies(data_dir=DATA_DIR):
    """Load spot proxies (1st generic futures) from parquet file."""
    path = data_dir / "commodity_spot_proxies.parquet"
    return pd.read_parquet(path)


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Pull and save commodity futures data
    commodity_futures_df = pull_commodity_futures()
    commodity_futures_df.to_parquet(DATA_DIR / "commodity_futures.parquet")

    # Derive and save spot proxies from 1st generic futures
    spot_proxies_df = build_spot_proxies_from_futures_df(commodity_futures_df)
    spot_proxies_df.to_parquet(DATA_DIR / "commodity_spot_proxies.parquet")

    # Pull and save LME metals data
    lme_metals_df = pull_lme_metals()
    lme_metals_df.to_parquet(DATA_DIR / "lme_metals.parquet")

    # Pull and save precious metals USD spot data
    precious_spot_df = pull_precious_metals_spot()
    precious_spot_df.to_parquet(DATA_DIR / "precious_metals_spot.parquet")

    # Pull and save GSCI indices data
    gsci_indices_df = pull_gsci_indices()
    gsci_indices_df.to_parquet(DATA_DIR / "gsci_indices.parquet")
