from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# Setup path to ensure imports work from anywhere
# Go up to the 'bursa_dataset_and_cleaning' directory and add it to the path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sql_inject.common.logging_config import get_logger

# ── Setup logging ──
logger = get_logger(__name__)

# ── Parse CLI args ──
parser = argparse.ArgumentParser()
parser.add_argument(
    "--replace", action="store_true", help="Replace data instead of appending"
)
args = parser.parse_args()

REPLACE_MODE: bool = args.replace
scripts_dir: Path = Path(
    __file__
).parent  # Updated: scripts are now in the same directory

scripts: list[str] = [
    "profile_injection.py",
    "management_injection.py",
    "ownership_injection.py",
    "top_10_injection.py",
    "insider_injection.py",
    "cashflow_injection.py",
    "income_injection.py",
    "balance_inject.py",  # Fixed: was "balance_injection.py"
]

for script in scripts:
    script_path: Path = scripts_dir / script
    logger.info("🚀 Running: %s", script_path)

    cmd: list[str] = ["python", str(script_path)]
    if REPLACE_MODE:
        cmd.append("--replace")

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.error("❌ Error in %s:\n%s", script, result.stderr)
