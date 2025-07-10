"""Dagster assets for the list_of_ids layer.
Handles scraping and processing of company IDs from Bursa Malaysia.
"""

import subprocess
import sys
from pathlib import Path
from typing import NoReturn

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(
    group_name="list_of_ids",
    description="Scrapes company IDs from Bursa Malaysia main market, ACE market, and LEAP market",
    required_resource_keys={"s3"},
)
def bursa_company_list(context: AssetExecutionContext) -> str:
    """Scrape company IDs from all Bursa Malaysia markets and upload to S3."""
    context.log.info("🚀 Starting company ID scraping from Bursa Malaysia...")

    # Path to the company ID scraper
    scraper_path = (
        Path(__file__).parent.parent.parent / "list_of_ids" / "company_id_scraper.py"
    )
    context.log.info("📁 Scraper script path: %s", scraper_path)

    def raise_file_not_found(error_msg: str) -> NoReturn:
        raise FileNotFoundError(error_msg)

    if not scraper_path.exists():
        error_msg = f"Scraper script not found at {scraper_path}"
        context.log.error("❌ %s", error_msg)
        raise_file_not_found(error_msg)

    try:
        # Run the company ID scraper
        context.log.info("🚀 Starting company ID scraping...")
        result = subprocess.run(
            [sys.executable, str(scraper_path)],
            capture_output=True,
            text=True,
            check=True,
        )

        context.log.info("✅ Company ID scraping completed successfully")
        context.log.info("📤 STDOUT: %s", result.stdout)

        if result.stderr:
            context.log.warning("⚠️ STDERR: %s", result.stderr)

        # Read the generated CSV file
        output_path = (
            Path(__file__).parent.parent.parent
            / "list_of_ids"
            / "outputs"
            / "bursa_company_list.csv"
        )
        context.log.info("📖 Reading output file: %s", output_path)

        if not output_path.exists():
            error_msg = f"Output file not found at {output_path}"
            context.log.error("❌ %s", error_msg)
            raise_file_not_found(error_msg)

        company_list_df = pd.read_csv(output_path)
        context.log.info("📊 Loaded %s companies from CSV", len(company_list_df))
        context.log.info("📋 CSV columns: %s", list(company_list_df.columns))

        # Upload to S3
        s3_path = "s3://onecredit-snowflake-data/data/bursa/company_list/bursa_company_list.csv"
        context.log.info("📤 Uploading to S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            company_list_df.to_csv(f, index=False)

        context.log.info("✅ Successfully uploaded to S3")

        # Add metadata
        context.add_output_metadata(
            {
                "num_companies_scraped": MetadataValue.int(len(company_list_df)),
                "unique_companies": MetadataValue.int(
                    int(company_list_df["company_id"].nunique())
                ),
                "s3_path": MetadataValue.text(s3_path),
                "scraping_status": MetadataValue.text("success"),
            }
        )

    except subprocess.CalledProcessError as e:
        context.log.exception(
            "❌ Company ID scraping failed with exit code %s", e.returncode
        )
        context.log.exception("📤 STDOUT: %s", e.stdout)
        context.log.exception("📤 STDERR: %s", e.stderr)
        raise
    except Exception as e:
        context.log.exception("❌ Unexpected error in company ID scraping")
        context.log.exception("🔍 Error type: %s", type(e).__name__)
        raise

    return s3_path


# Asset group for list_of_ids layer
list_of_ids_assets = [bursa_company_list]
