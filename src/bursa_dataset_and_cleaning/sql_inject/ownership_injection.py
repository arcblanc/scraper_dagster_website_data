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


class OwnershipProcessor(BaseSQLProcessor):
    def __init__(self) -> None:
        super().__init__(
            table_name="public_complete_ownership_type",
            file_path=Path(__file__).resolve().parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "combined_ownership.csv",
        )

    def process(self) -> None:
        logger.info("📥 Loading ownership CSV...")
        self.load_csv()
        df: pd.DataFrame = self.df  # type: ignore[assignment]

        # Check if company_id column exists
        if "company_id" not in df.columns:
            logger.error(
                "❌ company_id column not found in ownership CSV. Please re-run the profile scraper."
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
        """Enhanced upload method with better deduplication for ownership data"""
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

        # Get existing data for deduplication based on the DB constraint
        key_cols = ["registration_number", "investor_name", "ownership_", "value_m_usd"]

        # Check if table exists, if not, create it and upload all data
        if not self.table_exists():
            logger.info(
                "ℹ️ Table '%s' does not exist. Creating and performing initial load.",
                self.table_name,
            )
            self.df.to_sql(
                self.table_name,
                self.engine,
                schema="public",
                index=False,
                if_exists="replace",
            )
            logger.info(
                "✅ Successfully created and loaded %d rows into '%s'.",
                len(self.df),
                self.table_name,
            )
            return

        query = text(
            f"SELECT DISTINCT {', '.join(key_cols)} FROM public.{self.table_name}"
        )
        with self.engine.connect() as conn:
            existing = conn.execute(query).fetchall()

        # Create set of existing combinations
        existing_pairs = {tuple(str(val).strip() for val in row) for row in existing}

        # Prepare new data for comparison
        for col in key_cols:
            self.df[col] = self.df[col].astype(str).str.strip()

        # Replace 'nan' string with None
        self.df = self.df.replace("nan", None)
        # Drop rows where any key column is missing or nan/empty
        self.df = self.df.dropna(subset=key_cols)
        self.df = self.df[~self.df[key_cols].isin(["", "None", "NaN"]).any(axis=1)]

        # Create key for comparison
        self.df["key"] = list(zip(*(self.df[col] for col in key_cols), strict=False))

        original_len = len(self.df)

        # Filter out existing combinations
        new_data = self.df[~self.df["key"].isin(existing_pairs)].copy()
        new_data = new_data.drop(columns=["key"])

        new_rows = len(new_data)

        if new_rows > 0:
            # Log what's being added
            new_registrations = new_data["registration_number"].nunique()
            new_investors = new_data["investor_name"].nunique()

            logger.info("📊 Ownership Deduplication Summary:")
            logger.info("   • Original rows: %d", original_len)
            logger.info("   • Existing combinations: %d", len(existing_pairs))
            logger.info("   • New rows to add: %d", new_rows)
            logger.info("   • New registration numbers: %d", new_registrations)
            logger.info("   • New investors: %d", new_investors)

            # Show sample of what's being added
            sample_new = new_data[
                ["registration_number", "investor_name", "ownership_", "value_m_usd"]
            ].head(5)
            logger.info("📝 Sample new entries:")
            for _, row in sample_new.iterrows():
                logger.info(
                    "   • %s - %s (%s, %s)",
                    row["registration_number"],
                    row["investor_name"],
                    row["ownership_"],
                    row["value_m_usd"],
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
    processor = OwnershipProcessor()
    processor.process()
    logger.info("⬆️ Uploading to SQL with enhanced deduplication...")
    processor.upload_to_sql_enhanced(replace="--replace" in sys.argv)
    logger.info("✅ Upload completed.")
