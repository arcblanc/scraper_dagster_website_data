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


class CashFlowProcessor(BaseSQLProcessor):
    def __init__(self) -> None:
        super().__init__(
            table_name="public_complete_cash_flow",
            file_path=Path(__file__).resolve().parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_cash_flow_statements.csv",
        )

    def process(self) -> None:
        logger.info("🔄 Loading CSV...")
        self.load_csv()
        df: pd.DataFrame = self.df  # type: ignore[assignment]

        logger.info("🧹 Filtering only 'Value' rows and extracting fiscal date...")
        df = df[df["Year/Type"].str.contains("Value", na=False)].copy()
        df["fiscal_date"] = df["Year/Type"].str.extract(r"(\d{1,2} \w{3} \d{4})")
        df = df.drop(columns=["Year/Type"])

        logger.info("🔗 Loading company registration mapping...")
        registration_df = self._load_registration_mapping()
        df["company_id"] = df["company_id"].astype(str).str.strip()
        registration_df["company_id"] = (
            registration_df["company_id"].astype(str).str.strip()
        )

        logger.info("🧩 Merging registration data...")
        df = pd.merge(registration_df, df, on="company_id", how="inner")

        logger.info("🧽 Cleaning column names...")
        self.df = df
        df = self.clean_columns()

        logger.info("📌 Cleaning registration numbers...")
        # Force registration number to be a clean string, removing any trailing .0
        df["registration_number"] = (
            df["registration_number"]
            .astype(str)
            .str.replace(r"\\.0$", "", regex=True)
            .str.strip()
        )
        df = df[~df["registration_number"].isin(["", "nan", "NaN", "None"])]

        logger.info("📎 Selecting target financial columns...")
        target_cols: list[str] = [
            col
            for col in df.columns
            if "depreciation" in col or col == "net_income_starting_line"
        ]
        final_cols: list[str] = ["registration_number", "fiscal_date", *target_cols]
        self.df = df[[col for col in final_cols if col in df.columns]]

        logger.info("✅ Cash flow processing complete. Rows ready: %d", len(self.df))

    def upload_to_sql_enhanced(self, *, replace: bool = False) -> None:
        """Enhanced upload method with better deduplication for cash flow data"""
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
            "SELECT DISTINCT registration_number, fiscal_date FROM public.public_complete_cash_flow"
        )
        with self.engine.connect() as conn:
            existing = conn.execute(query).fetchall()

        # Create set of existing registration_number + fiscal_date combinations
        existing_pairs = {
            (str(row[0]).strip(), str(row[1]).strip()) for row in existing
        }

        # Prepare new data for comparison
        self.df["registration_number"] = (
            self.df["registration_number"].astype(str).str.strip()
        )
        self.df["fiscal_date"] = self.df["fiscal_date"].astype(str).str.strip()

        # Create key for comparison
        self.df["key"] = list(
            zip(self.df["registration_number"], self.df["fiscal_date"], strict=False)
        )

        original_len = len(self.df)

        # Filter out existing combinations
        new_data = self.df[~self.df["key"].isin(existing_pairs)].copy()
        new_data = new_data.drop(columns=["key"])

        new_rows = len(new_data)

        if new_rows > 0:
            # Log what's being added
            new_registrations = new_data["registration_number"].nunique()
            new_fiscal_dates = new_data["fiscal_date"].nunique()

            logger.info("📊 Deduplication Summary:")
            logger.info("   • Original rows: %d", original_len)
            logger.info("   • Existing combinations: %d", len(existing_pairs))
            logger.info("   • New rows to add: %d", new_rows)
            logger.info("   • New registration numbers: %d", new_registrations)
            logger.info("   • New fiscal dates: %d", new_fiscal_dates)

            # Show sample of what's being added
            sample_new = new_data[["registration_number", "fiscal_date"]].head(5)
            logger.info("📝 Sample new entries:")
            for _, row in sample_new.iterrows():
                logger.info(
                    "   • %s - %s", row["registration_number"], row["fiscal_date"]
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
    processor = CashFlowProcessor()
    processor.process()
    logger.info("⬆️ Uploading to SQL with enhanced deduplication...")
    processor.upload_to_sql_enhanced(replace="--replace" in sys.argv)
    logger.info("✅ Upload completed.")
