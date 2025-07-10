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


class ManagementProcessor(BaseSQLProcessor):
    def __init__(self) -> None:
        super().__init__(
            table_name="public_complete_directors_executives",
            file_path=Path(__file__).resolve().parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "combined_management.csv",
        )

    def process(self) -> None:
        logger.info("📥 Loading management CSV...")
        self.load_csv()
        df: pd.DataFrame = self.df  # type: ignore[assignment]

        # Check if company_id column exists
        if "company_id" not in df.columns:
            logger.error(
                "❌ company_id column not found in management CSV. Please re-run the profile scraper."
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

        # Filter out rows where Name is empty or null
        original_count = len(df)
        df = df.dropna(subset=["name"])
        df = df[df["name"].str.strip() != ""]
        filtered_count = len(df)

        if original_count != filtered_count:
            logger.info(
                f"🧹 Filtered out {original_count - filtered_count} rows with empty names"
            )

        self.df = df
        logger.info("✅ Processing complete. Ready for upload.")

    def upload_to_sql_enhanced(self, *, replace: bool = False) -> None:
        """Enhanced upload method with better deduplication for management data"""
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
            "SELECT DISTINCT registration_number, since, role, designation FROM public.public_complete_directors_executives"
        )
        with self.engine.connect() as conn:
            existing = conn.execute(query).fetchall()

        # Create set of existing registration_number + since + role + designation combinations
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
        self.df["since"] = self.df["since"].astype(str).str.strip()
        self.df["role"] = self.df["role"].astype(str).str.strip()
        self.df["designation"] = self.df["designation"].astype(str).str.strip()

        # Create key for comparison
        self.df["key"] = list(
            zip(
                self.df["registration_number"],
                self.df["since"],
                self.df["role"],
                self.df["designation"],
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
            new_names = new_data["name"].nunique()

            logger.info("📊 Management Deduplication Summary:")
            logger.info("   • Original rows: %d", original_len)
            logger.info("   • Existing combinations: %d", len(existing_pairs))
            logger.info("   • New rows to add: %d", new_rows)
            logger.info("   • New registration numbers: %d", new_registrations)
            logger.info("   • New names: %d", new_names)

            # Show sample of what's being added
            sample_new = new_data[
                ["registration_number", "name", "role", "designation"]
            ].head(5)
            logger.info("📝 Sample new entries:")
            for _, row in sample_new.iterrows():
                logger.info(
                    "   • %s - %s (%s, %s)",
                    row["registration_number"],
                    row["name"],
                    row["role"],
                    row["designation"],
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
    processor = ManagementProcessor()
    processor.process()
    logger.info("⬆️ Uploading to SQL with enhanced deduplication...")
    processor.upload_to_sql_enhanced(replace="--replace" in sys.argv)
    logger.info("✅ Upload completed.")
