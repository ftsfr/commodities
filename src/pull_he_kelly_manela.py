"""
Download He-Kelly-Manela factor data.

This module downloads the He-Kelly-Manela factors and test portfolios data
from the original source and stores it locally.
"""

import sys
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import urllib3

import chartbook

# Suppress SSL warnings when verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"
URL = "https://asaf.manela.org/papers/hkm/intermediarycapitalrisk/He_Kelly_Manela_Factors.zip"


def pull_he_kelly_manela(data_dir=DATA_DIR):
    """
    Download the He-Kelly-Manela factors and test portfolios data.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    response = requests.get(URL, verify=False)
    zip_file = BytesIO(response.content)
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(data_dir)


def load_he_kelly_manela_all(data_dir=DATA_DIR):
    """Load the complete HKM dataset with all factors and test assets."""
    path = data_dir / "He_Kelly_Manela_Factors_And_Test_Assets_monthly.csv"
    _df = pd.read_csv(path)
    _df["date"] = pd.to_datetime(_df["yyyymm"], format="%Y%m")
    return _df


if __name__ == "__main__":
    data_dir = DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)
    pull_he_kelly_manela(data_dir=data_dir)
    print(f"Downloaded He-Kelly-Manela data to {data_dir}")
