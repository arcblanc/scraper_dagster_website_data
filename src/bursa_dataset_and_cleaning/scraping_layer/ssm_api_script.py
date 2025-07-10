import logging
import time
from pathlib import Path

import pandas as pd
import requests
from rapidfuzz import fuzz
from tqdm import tqdm

# ─── LOGGING ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

# ─── PATH CONFIG ───
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = BASE_DIR / "list_of_ids" / "outputs" / "bursa_company_list.csv"
OUTPUT_CSV = (
    BASE_DIR / "scraping_layer" / "combined_csv" / "matched_companies_from_ssm.csv"
)
API_URL = "https://staging-ssm.onecredit.my/api/search"


# ─── SSM Query Function ───
def query_company(name: str) -> dict:
    try:
        response = requests.get(API_URL, params={"query": name}, timeout=10)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list) and data:
            best_match, best_score = None, 0
            for item in data:
                candidate_name = item.get("companyName", "")
                score = fuzz.ratio(name.lower(), candidate_name.lower())
                if score > best_score:
                    best_score = score
                    best_match = item

            if best_match:
                return {
                    "company_name": name,
                    "name_db": best_match.get("companyName"),
                    "companyNo": best_match.get("companyNo"),
                    "oldCompanyNo": best_match.get("oldCompanyNo"),
                    "company_type": best_match.get("entityType"),
                    "match_score": best_score,
                }

    except requests.RequestException as exc:
        logger.warning("⚠️ Request error for '%s': %s", name, exc)

    return {
        "company_name": name,
        "name_db": None,
        "companyNo": None,
        "oldCompanyNo": None,
        "company_type": None,
        "match_score": 0,
    }


# ─── Main Execution ───
def main() -> None:
    logger.info("🔄 Loading input: %s", INPUT_CSV)
    df = pd.read_csv(INPUT_CSV)
    company_names = df["company_name"].dropna().unique()
    logger.info("📊 Total companies to process: %d", len(company_names))

    # Create a set of valid company names from the current input
    valid_company_names = set(company_names)

    if OUTPUT_CSV.exists():
        existing = pd.read_csv(OUTPUT_CSV)
        # Filter existing data to only include companies from current input
        existing_filtered = existing[existing["company_name"].isin(valid_company_names)]
        already_matched = set(existing_filtered["company_name"].dropna().unique())

        removed_count = len(existing) - len(existing_filtered)
        if removed_count > 0:
            logger.info(
                "🧹 Removed %d companies that are no longer in the input list",
                removed_count,
            )

        logger.info("🔁 Skipping %d previously matched companies", len(already_matched))
    else:
        existing_filtered = pd.DataFrame()
        already_matched = set()

    to_query = [name for name in company_names if name not in already_matched]
    logger.info("📦 Querying %d new companies...", len(to_query))

    new_results = []
    successful_matches = 0
    failed_matches = 0

    # Enhanced progress tracking with detailed logging
    pbar = tqdm(to_query, desc="🔍 Matching companies", unit="company")

    for i, name in enumerate(pbar, 1):
        # Update progress bar description with current company
        pbar.set_description(
            f"🔍 Matching: {name[:30]}{'...' if len(name) > 30 else ''}"
        )

        # Log current progress every 10 companies
        if i % 10 == 0:
            logger.info(
                "📈 Progress: %d/%d companies processed (%.1f%%) - Success: %d, Failed: %d",
                i,
                len(to_query),
                (i / len(to_query) * 100),
                successful_matches,
                failed_matches,
            )

        result = query_company(name)
        new_results.append(result)

        # Track success/failure
        if result["match_score"] > 0:
            successful_matches += 1
            pbar.set_postfix(
                {
                    "Success": successful_matches,
                    "Failed": failed_matches,
                    "Score": result["match_score"],
                }
            )
        else:
            failed_matches += 1
            pbar.set_postfix(
                {"Success": successful_matches, "Failed": failed_matches, "Score": 0}
            )

        time.sleep(1.0)

    pbar.close()

    # Final summary
    logger.info("✅ Matching completed!")
    logger.info(
        "📊 Final Results: %d successful matches, %d failed matches",
        successful_matches,
        failed_matches,
    )
    logger.info(
        "📈 Success rate: %.1f%%",
        (successful_matches / len(to_query) * 100) if to_query else 0,
    )

    df_new = pd.DataFrame(new_results)
    final_df = pd.concat([existing_filtered, df_new], ignore_index=True)

    # Force companyNo to be string, but first convert to int if possible to remove .0, then back to string
    def clean_company_no(val):
        try:
            if pd.isna(val) or val is None:
                return None
            # Remove any non-numeric characters, then convert
            val_str = str(val).strip()
            # If it's already a string of digits, just return
            if val_str.isdigit():
                return val_str
            # Try to convert to int, then back to string
            return str(int(float(val_str)))
        except Exception:
            return str(val)

    final_df["companyNo"] = final_df["companyNo"].apply(clean_company_no)

    manual_override = {
        "SIME DARBY PROPERTY BERHAD": {
            "company_name": "SIME DARBY PROPERTY BERHAD",
            "name_db": "SIME DARBY PROPERTY BERHAD",
            "companyNo": "197301002148",
            "oldCompanyNo": "0015631P",
            "company_type": "Company",
            "match_score": 100,
        },
        "S P SETIA BERHAD": {
            "company_name": "S P SETIA BERHAD",
            "name_db": "S P SETIA BERHAD",
            "companyNo": "197401002663",
            "oldCompanyNo": "0019698X",
            "company_type": "INVESTMENT HOLDING COMPANY",
            "match_score": 100,
        },
    }

    logger.info("🔧 Applying manual overrides for %d companies", len(manual_override))
    final_df = final_df[~final_df["company_name"].isin(manual_override)]
    manual_df = pd.DataFrame(manual_override.values())
    final_df = pd.concat([final_df, manual_df], ignore_index=True)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Final validation: ensure output only contains companies from input
    final_df = final_df[final_df["company_name"].isin(valid_company_names)]

    final_df.to_csv(OUTPUT_CSV, index=False)
    logger.info("💾 Saved %d total companies to %s", len(final_df), OUTPUT_CSV.name)
    logger.info(
        "✅ Validation: All %d companies are from the input list", len(final_df)
    )


if __name__ == "__main__":
    main()
