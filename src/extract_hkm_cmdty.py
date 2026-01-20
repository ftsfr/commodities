"""
Extract commodity factors from He-Kelly-Manela dataset.

This module loads the HKM dataset from the local _data directory.
"""

import sys

sys.path.insert(1, "./src/")

import pandas as pd
import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"


def load_he_kelly_manela_all(data_dir=DATA_DIR):
    """Load the complete HKM dataset."""
    path = data_dir / "ftsfr_he_kelly_manela_all.parquet"
    return pd.read_parquet(path)


def extract_hkm_cmdty(data_dir=DATA_DIR):
    """Extract commodity columns from He-Kelly-Manela dataset."""
    he_kelly_manela = load_he_kelly_manela_all(data_dir)

    col_lst = ["yyyymm"]
    for i in range(1, 10):
        col_lst.append(f"Commod_0{i}")
    for i in range(10, 24):
        col_lst.append(f"Commod_{i}")

    hkm_df = he_kelly_manela[col_lst].dropna(axis=0)
    hkm_df["yyyymm"] = hkm_df["yyyymm"].astype(int).astype(str)
    hkm_df = hkm_df.set_index("yyyymm")
    return hkm_df
