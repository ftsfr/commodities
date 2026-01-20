"""
Create HKM dataset in wide format for commodity replication.

This script loads the He-Kelly-Manela data and saves it in wide format
(preserving Commod_01 through Commod_23 columns) for use by extract_hkm_cmdty.py.
"""

import sys

sys.path.insert(1, "./src/")

import pull_he_kelly_manela
import chartbook

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"


def main():
    """Create HKM dataset in wide format."""
    print("Loading He-Kelly-Manela data...")
    df = pull_he_kelly_manela.load_he_kelly_manela_all(data_dir=DATA_DIR)

    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns[:10])}...")

    # Save in wide format (preserving original structure)
    output_path = DATA_DIR / "ftsfr_he_kelly_manela_all.parquet"
    df.to_parquet(output_path)
    print(f"Saved wide-format HKM data to {output_path}")


if __name__ == "__main__":
    main()
