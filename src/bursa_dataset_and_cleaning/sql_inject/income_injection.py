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


class IncomeProcessor(BaseSQLProcessor):
    def __init__(self) -> None:
        super().__init__(
            table_name="public_complete_income",
            file_path=Path(__file__).resolve().parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_income_statements.csv",
        )

    def process(self) -> None:
        logger.info("📥 Loading CSV...")
        self.load_csv()
        df = self.df

        logger.info("🧹 Filtering 'Value' and 'YoY %%' rows...")
        value_df = df[df["Year/Type"].str.contains("Value", na=False)].copy()
        yoy_df = df[df["Year/Type"].str.contains("YoY %", na=False)].copy()

        value_df["Fiscal Date"] = value_df["Year/Type"].str.extract(
            r"(\d{1,2} \w{3} \d{4})"
        )
        yoy_df["Fiscal Date"] = yoy_df["Year/Type"].str.extract(
            r"(\d{1,2} \w{3} \d{4})"
        )

        logger.info("🔗 Merging Value and YoY %% rows...")
        merged_df = value_df.merge(
            yoy_df,
            on=["company_id", "Fiscal Date"],
            suffixes=("", "_yoy"),
            how="left",
        ).drop(columns=["Year/Type", "Year/Type_yoy"])

        drop_cols = [
            col
            for col in merged_df.columns
            if col.endswith("_yoy")
            and not merged_df[col].astype(str).str.contains("%", na=False).any()
        ]
        merged_df = merged_df.drop(columns=drop_cols)

        logger.info("🧽 Cleaning columns and mapping registration IDs...")
        logger.info("🔗 Loading company registration mapping...")
        registration_df = self._load_registration_mapping()
        registration_df["company_id"] = (
            registration_df["company_id"].astype(str).str.strip()
        )
        merged_df["company_id"] = merged_df["company_id"].astype(str).str.strip()

        logger.info("🧩 Merging registration data...")
        df = pd.merge(registration_df, merged_df, on="company_id", how="inner")

        logger.info("🧽 Cleaning columns...")
        self.df = df
        df_cleaned = self.clean_columns()

        logger.info("🩹 Applying manual revenue patches...")
        df_patched = self._patch_manual_revenue(df_cleaned)

        logger.info("🧾 Final formatting before upload...")
        self.df = self._final_formatting(df_patched)

    def _patch_manual_revenue(self, frame: pd.DataFrame) -> pd.DataFrame:
        patches = {
            "1155": {
                "31 Dec 2024": 27907,
                "31 Dec 2023": 25650,
                "31 Dec 2022": 23702,
                "31 Dec 2021": 22249,
                "31 Dec 2020": 19670,
            },
            "1023": {
                "31 Dec 2024": 22301.154,
                "31 Dec 2023": 21014.482,
                "31 Dec 2022": 19837.516,
                "31 Dec 2021": 19512.940,
                "31 Dec 2020": 17189.003,
            },
            "5819": {
                "30 Jun 2024": 5884,
                "30 Jun 2023": 5570,
                "30 Jun 2022": 5417,
                "30 Jun 2021": 4803,
                "30 Jun 2020": 4399,
            },
            "1295": {
                "31 Dec 2024": 14040,
                "31 Dec 2023": 12949,
                "31 Dec 2022": 13065,
                "31 Dec 2021": 11305,
                "31 Dec 2020": 10045,
            },
        }
        for company_id, records in patches.items():
            for date, value in records.items():
                mask = (frame["company_id"] == company_id) & (
                    frame["fiscal_date"] == date
                )
                frame.loc[mask, "revenue"] = float(value)
        return frame

    def _final_formatting(self, frame: pd.DataFrame) -> pd.DataFrame:
        # Create a copy to avoid SettingWithCopyWarning
        frame = frame.copy()

        frame["revenue"] = pd.to_numeric(
            frame["revenue"].astype(str).str.replace(",", ""), errors="coerce"
        )
        # Force registration number to be a clean string, removing any trailing .0
        frame["registration_number"] = (
            frame["registration_number"]
            .astype(str)
            .str.replace(r"\\.0$", "", regex=True)
            .str.strip()
        )
        frame = frame[~frame["registration_number"].isin(["", "nan", "NaN", "None"])]

        frame["fiscal_date"] = pd.to_datetime(frame["fiscal_date"], errors="coerce")
        frame = frame.sort_values(
            ["company_id", "fiscal_date"], ascending=[True, False]
        )
        frame["fiscal_date"] = frame["fiscal_date"].dt.strftime("%d %b %Y")

        columns_to_keep = [
            "company_id",
            "registration_number",
            "old_registration_number",
            "revenue",
            "net_income_before_taxes",
            "net_income_after_taxes",
            "fiscal_date",
            "operating_income",
            "gross_profit",
            "cost_of_revenue",
            "basic_eps_including_extraordinary_items",
        ]
        return frame[[col for col in columns_to_keep if col in frame.columns]]

    def upload_to_sql_enhanced(self, *, replace: bool = False) -> None:
        """Enhanced upload method with better deduplication for income data"""
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
            "SELECT DISTINCT registration_number, fiscal_date FROM public.public_complete_income"
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
    processor = IncomeProcessor()
    processor.process()
    logger.info("⬆️ Uploading to SQL with enhanced deduplication...")
    processor.upload_to_sql_enhanced(replace="--replace" in sys.argv)
    logger.info("✅ Upload completed.")
