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


class CompanyProfileProcessor(BaseSQLProcessor):
    def __init__(self) -> None:
        super().__init__(
            table_name="public_complete_company_profile",
            file_path=Path(__file__).resolve().parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "combined_profile.csv",
        )

    def process(self) -> None:
        logger.info("📥 Loading profile CSV...")
        self.load_csv()
        df: pd.DataFrame = self.df  # type: ignore[assignment]

        # Load and merge market data
        logger.info("📈 Loading and merging market data...")
        market_data_path = (
            Path(__file__).resolve().parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "market_info_sample.csv"
        )
        if market_data_path.exists():
            market_df = pd.read_csv(market_data_path, low_memory=False)

            # Ensure company_id is a string in both dataframes for a safe merge
            df["company_id"] = df["company_id"].astype(str).str.strip()
            market_df["company_id"] = market_df["company_id"].astype(str).str.strip()

            # Perform a left join to add market info to profiles
            df = pd.merge(df, market_df, on="company_id", how="left")
            logger.info("✅ Market data merged successfully.")
        else:
            logger.warning(
                "⚠️ Market data file not found, skipping merge: %s", market_data_path
            )

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

        # Ensure registration_number is a string and clean
        if "registration_number" in df.columns:
            df["registration_number"] = (
                df["registration_number"]
                .astype(str)
                .str.replace(r"\\.0$", "", regex=True)
                .str.strip()
            )
            df = df[~df["registration_number"].isin(["", "nan", "NaN", "None"])]
        self.df = df

        logger.info("🧽 Dropping invalid registration numbers...")
        if "match_score" in df.columns:
            df = df.drop(columns=["match_score"])
        if "source_file" in df.columns:
            df = df.drop(columns=["source_file"])

        self.df = df
        logger.info("✅ Processing complete. Ready for upload.")

    def upload_to_sql_enhanced(self, *, replace: bool = False) -> None:
        """Enhanced upload method with better deduplication for profile data"""
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
            f"SELECT DISTINCT registration_number FROM public.{self.table_name}"
        )
        with self.engine.connect() as conn:
            existing = conn.execute(query).fetchall()

        # Create set of existing registration numbers
        existing_registrations = {str(row[0]).strip() for row in existing}

        # Prepare new data for comparison
        self.df["registration_number"] = (
            self.df["registration_number"].astype(str).str.strip()
        )

        original_len = len(self.df)

        # Filter out existing registration numbers
        new_data = self.df[
            ~self.df["registration_number"].isin(existing_registrations)
        ].copy()

        new_rows = len(new_data)

        if new_rows > 0:
            # Log what's being added
            new_registrations = new_data["registration_number"].nunique()

            logger.info("📊 Profile Deduplication Summary:")
            logger.info("   • Original rows: %d", original_len)
            logger.info(
                "   • Existing registration numbers: %d", len(existing_registrations)
            )
            logger.info("   • New rows to add: %d", new_rows)
            logger.info("   • New registration numbers: %d", new_registrations)

            # Show sample of what's being added
            sample_new = new_data[["registration_number"]].head(5)
            logger.info("📝 Sample new entries:")
            for _, row in sample_new.iterrows():
                logger.info("   • %s", row["registration_number"])

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
    processor = CompanyProfileProcessor()
    processor.process()
    logger.info("⬆️ Uploading to SQL with enhanced deduplication...")
    processor.upload_to_sql_enhanced(replace="--replace" in sys.argv)
    logger.info("✅ Upload completed.")
