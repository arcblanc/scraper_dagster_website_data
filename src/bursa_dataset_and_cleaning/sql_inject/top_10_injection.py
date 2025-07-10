from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Setup path to ensure base_processor import works from anywhere
# Go up to the 'bursa_dataset_and_cleaning' directory and add it to the path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sql_inject.common.base_processor import BaseSQLProcessor
from sql_inject.common.logging_config import get_logger

# ─── LOGGING CONFIG ───
logger = get_logger(__name__)


class Top10Processor(BaseSQLProcessor):
    def __init__(self) -> None:
        super().__init__(
            table_name="public_complete_top_10_investors",
            file_path=Path(__file__).resolve().parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "combined_top10.csv",
        )

    def process(self) -> None:
        logger.info("📥 Loading top 10 investors CSV...")
        self.load_csv()
        df: pd.DataFrame = self.df  # type: ignore[assignment]

        # Check if company_id column exists
        if "company_id" not in df.columns:
            logger.error(
                "❌ company_id column not found in top 10 CSV. Please re-run the profile scraper."
            )
            return

        logger.info("🔗 Loading company registration mapping...")
        registration_df = self._load_registration_mapping()
        df["company_id"] = df["company_id"].astype(str).str.strip()
        registration_df["company_id"] = (
            registration_df["company_id"].astype(str).str.strip()
        )

        logger.info("🧩 Merging registration data...")
        df = pd.merge(registration_df, df, on="company_id", how="inner")

        logger.info("🧼 Cleaning columns...")
        self.df = df
        df = self.clean_columns()

        logger.info("🧽 Dropping invalid registration numbers...")
        df["registration_number"] = df["registration_number"].astype(str).str.strip()
        df = df[~df["registration_number"].isin(["", "nan", "NaN", "None"])]
        self.df = df
        logger.info("✅ Processing complete. Ready for upload.")

    def upload_to_sql_enhanced(self, *, replace: bool = False) -> None:
        """Enhanced upload method with better deduplication for top 10 investors data"""
        if self.df is None:
            msg = "DataFrame not loaded. Run process() first."
            raise ValueError(msg)

        if replace:
            self.df.to_sql(
                self.table_name,
                self.engine,
                schema="public",
                index=False,
                if_exists="replace",
            )
            logger.info(
                "🔁 Table '%s' replaced with %d rows.", self.table_name, len(self.df)
            )
            return

        # Get existing data for deduplication
        query = text(
            f"SELECT DISTINCT registration_number, filing_date, investor_name, ownership_ FROM public.{self.table_name}"
        )
        with self.engine.connect() as conn:
            existing = conn.execute(query).fetchall()

        # Create set of existing registration_number + filing_date + investor_name + ownership_ combinations
        existing_pairs = {
            (
                str(row[0]).strip(),
                str(row[1]).strip(),
                str(row[2]).strip(),
                str(row[3]).strip(),
            )
            for row in existing
        }

        # Prepare new data for comparison
        self.df["registration_number"] = (
            self.df["registration_number"].astype(str).str.strip()
        )
        self.df["filing_date"] = self.df["filing_date"].astype(str).str.strip()
        self.df["investor_name"] = self.df["investor_name"].astype(str).str.strip()
        self.df["ownership_"] = self.df["ownership_"].astype(str).str.strip()

        # Create key for comparison
        self.df["key"] = list(
            zip(
                self.df["registration_number"],
                self.df["filing_date"],
                self.df["investor_name"],
                self.df["ownership_"],
                strict=False,
            )
        )

        original_len = len(self.df)

        # Filter out existing combinations
        new_data = self.df[~self.df["key"].isin(existing_pairs)].copy()
        new_data = new_data.drop(columns=["key"])

        new_rows = len(new_data)

        if new_rows > 0:
            # Log what's being added
            new_registrations = new_data["registration_number"].nunique()
            new_investors = new_data["investor_name"].nunique()

            logger.info("📊 Top 10 Investors Deduplication Summary:")
            logger.info("   • Original rows: %d", original_len)
            logger.info("   • Existing combinations: %d", len(existing_pairs))
            logger.info("   • New rows to add: %d", new_rows)
            logger.info("   • New registration numbers: %d", new_registrations)
            logger.info("   • New investors: %d", new_investors)

            # Show sample of what's being added
            sample_new = new_data[
                ["registration_number", "investor_name", "ownership_", "filing_date"]
            ].head(5)
            logger.info("📝 Sample new entries:")
            for _, row in sample_new.iterrows():
                logger.info(
                    "   • %s - %s (%s, %s)",
                    row["registration_number"],
                    row["investor_name"],
                    row["ownership_"],
                    row["filing_date"],
                )

            # Upload new data
            new_data.to_sql(
                self.table_name,
                self.engine,
                schema="public",
                index=False,
                if_exists="append",
            )
            logger.info(
                "✅ Successfully appended %d new rows to '%s'",
                new_rows,
                self.table_name,
            )
        else:
            logger.info(
                "ℹ️ No new rows to append to '%s' - all data already exists",
                self.table_name,
            )


if __name__ == "__main__":
    processor = Top10Processor()
    processor.process()
    logger.info("⬆️ Uploading to SQL with enhanced deduplication...")
    processor.upload_to_sql_enhanced(replace="--replace" in sys.argv)
    logger.info("✅ Upload completed.")
