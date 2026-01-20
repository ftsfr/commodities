"""
Doit build file for Commodities pipeline.

Run with: doit
"""

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import chartbook

sys.path.insert(1, "./src/")


# Bloomberg Terminal check - runs at module load time
def _check_bloomberg_terminal():
    """Check Bloomberg Terminal availability with env var override."""
    # Skip prompt if environment variable is set
    if os.environ.get("BLOOMBERG_TERMINAL_OPEN", "").lower() in ("true", "1", "yes"):
        print("BLOOMBERG_TERMINAL_OPEN=True detected, skipping prompt...")
        return True

    # Interactive prompt
    response = input("Do you have the Bloomberg terminal open in the background? [Y/n]: ")
    if response.lower() in ('n', 'no'):
        raise SystemExit(
            "\nBloomberg Terminal not available. Exiting.\n"
            "Tip: Set BLOOMBERG_TERMINAL_OPEN=True to skip this prompt."
        )
    return True


_check_bloomberg_terminal()

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"
OUTPUT_DIR = BASE_DIR / "_output"
OS_TYPE = "nix" if platform.system() != "Windows" else "windows"


def jupyter_execute_notebook(notebook):
    """Execute a Jupyter notebook and save output."""
    return (
        f"jupyter nbconvert --execute --to notebook "
        f'--ClearMetadataPreprocessor.enabled=True --inplace "{notebook}"'
    )


def jupyter_to_html(notebook, output_dir):
    """Convert notebook to HTML."""
    return (
        f'jupyter nbconvert --to html --output-dir="{output_dir}" "{notebook}"'
    )


def task_config():
    """Create necessary directories."""
    return {
        "actions": [
            f'mkdir -p "{DATA_DIR}"' if OS_TYPE == "nix" else f'if not exist "{DATA_DIR}" mkdir "{DATA_DIR}"',
            f'mkdir -p "{OUTPUT_DIR}"' if OS_TYPE == "nix" else f'if not exist "{OUTPUT_DIR}" mkdir "{OUTPUT_DIR}"',
            f'mkdir -p "{OUTPUT_DIR}/_notebook_build"' if OS_TYPE == "nix" else f'if not exist "{OUTPUT_DIR}/_notebook_build" mkdir "{OUTPUT_DIR}/_notebook_build"',
        ],
        "verbosity": 2,
    }


def task_pull_wrds_futures():
    """Pull futures data from WRDS."""
    return {
        "actions": ["python src/pull_wrds_futures.py"],
        "verbosity": 2,
        "task_dep": ["config"],
    }


def task_pull_bbg_active_commodities():
    """Pull active commodities from Bloomberg."""
    return {
        "actions": ["python src/pull_bbg_active_commodities.py"],
        "verbosity": 2,
        "task_dep": ["config"],
    }


def task_pull_bbg_commodities_basis():
    """Pull commodities basis data from Bloomberg."""
    return {
        "actions": ["python src/pull_bbg_commodities_basis.py"],
        "verbosity": 2,
        "task_dep": ["config"],
    }


def task_pull_hkm():
    """Pull He-Kelly-Manela factor data."""
    return {
        "actions": ["python src/pull_he_kelly_manela.py"],
        "verbosity": 2,
        "task_dep": ["config"],
    }


def task_format_hkm():
    """Create wide-format HKM parquet file."""
    return {
        "actions": ["python src/create_hkm_datasets.py"],
        "verbosity": 2,
        "task_dep": ["pull_hkm"],
    }


def task_calc():
    """Calculate commodity returns and create FTSFR datasets."""
    return {
        "actions": ["python src/create_ftsfr_datasets.py"],
        "verbosity": 2,
        "task_dep": ["pull_bbg_commodities_basis", "format_hkm"],
    }


def task_run_notebooks():
    """Execute summary notebooks."""
    notebooks = [
        "src/summary_commodities_ipynb.py",
    ]

    actions = []
    for nb_py in notebooks:
        nb_name = Path(nb_py).stem
        nb_ipynb = OUTPUT_DIR / "_notebook_build" / f"{nb_name}.ipynb"

        # Convert py to ipynb
        actions.append(f'ipynb-py-convert "{nb_py}" "{nb_ipynb}"')
        # Execute notebook
        actions.append(jupyter_execute_notebook(nb_ipynb))
        # Convert to HTML
        actions.append(jupyter_to_html(nb_ipynb, OUTPUT_DIR / "_notebook_build"))

    return {
        "actions": actions,
        "verbosity": 2,
        "task_dep": ["calc"],
    }


def task_generate_pipeline_site():
    """Generate pipeline documentation site."""
    return {
        "actions": ["chartbook build -f"],
        "verbosity": 2,
        "task_dep": ["run_notebooks"],
    }
