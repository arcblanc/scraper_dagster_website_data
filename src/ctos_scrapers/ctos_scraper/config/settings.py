import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ─── BASE DIRECTORY ───
BASE_DIR = Path(os.getenv("CTOS_BASE_DIR", Path(__file__).resolve().parents[1]))

# ─── AWS CONFIG ───
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# ─── SHARED OUTPUT BASE ───
OUTPUT_DIR = BASE_DIR / "outputs"

# ─── ROB CONFIG ───
ROB = {
    "registration_table": os.getenv(
        "ROB_COMPANY_DETAILS", "ctos_rob_registration_number"
    ),
    "url_table": os.getenv("ROB_URLS", "ctos_rob_url"),
    "dagster_project_name": "ctos_rob_scraper",
}

# ─── ROC CONFIG ───
ROC = {
    "registration_table": os.getenv(
        "ROC_COMPANY_DETAILS", "ctos_roc_registration_number"
    ),
    "url_table": os.getenv("ROC_URLS", "ctos_roc_url"),
    "dagster_project_name": "ctos_roc_scraper",
}
