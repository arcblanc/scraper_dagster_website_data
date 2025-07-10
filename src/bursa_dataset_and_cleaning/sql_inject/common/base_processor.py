from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

# ─── LOGGING ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

SAFE_QUERIES = {
    # Financial data - uses registration_number + fiscal_date
    "public_complete_balance_sheet": text(
        "SELECT DISTINCT registration_number, fiscal_date FROM public.public_complete_balance_sheet",
    ),
    "public_complete_cash_flow": text(
        "SELECT DISTINCT registration_number, fiscal_date FROM public.public_complete_cash_flow",
    ),
    "public_complete_income": text(
        "SELECT DISTINCT registration_number, fiscal_date FROM public.public_complete_income",
    ),
    # Profile data - uses registration_number only (unique per company)
    "public_combined_profile": text(
        "SELECT DISTINCT registration_number FROM public.public_combined_profile",
    ),
    # Management data - uses registration_number + since + role + designation
    "public_complete_directors_executives": text(
        "SELECT DISTINCT registration_number, since, role, designation FROM public.public_complete_directors_executives",
    ),
    # Insider shareholder data - uses registration_number + filing_date + investor_name + ownership_
    "public_complete_individual_shareholder": text(
        "SELECT DISTINCT registration_number, filing_date, investor_name, ownership_ FROM public.public_complete_individual_shareholder",
    ),
    # Ownership data - uses registration_number + ownership_ + no_of_investors + investor_name + value_m_usd
    "public_combined_ownership": text(
        "SELECT DISTINCT registration_number, ownership_, no_of_investors, investor_name, value_m_usd FROM public.public_combined_ownership",
    ),
    # Top 10 investors data - uses registration_number + filing_date + investor_name + ownership_
    "public_combined_top10": text(
        "SELECT DISTINCT registration_number, filing_date, investor_name, ownership_ FROM public.public_combined_top10",
    ),
    # Legacy table names (keeping for backward compatibility)
    "public_complete_company_profile": text(
        "SELECT DISTINCT registration_number FROM public.public_complete_company_profile",
    ),
    "public_complete_ownership_type": text(
        "SELECT DISTINCT registration_number, ownership_, no_of_investors, investor_name, value_m_usd FROM public.public_complete_ownership_type",
    ),
    "public_complete_top_10_investors": text(
        "SELECT DISTINCT registration_number, filing_date, investor_name, ownership_ FROM public.public_complete_top_10_investors",
    ),
}


class BaseSQLProcessor:
    def __init__(self, table_name: str, file_path: str | Path) -> None:
        self.table_name = table_name
        self.file_path = Path(file_path)
        self.base_dir = Path(__file__).resolve().parents[3]
        self.df: pd.DataFrame | None = None
        self.engine = self._setup_db()

    def table_exists(self) -> bool:
        """Check if the table exists in the database."""
        inspector = inspect(self.engine)
        return inspector.has_table(self.table_name.split(".")[-1], schema="public")

    def _setup_db(self) -> Engine:
        load_dotenv()
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")
        host = os.getenv("PG_HOST")
        port = os.getenv("PG_PORT")
        database = os.getenv("PG_DATABASE")

        return create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )

    def load_csv(self) -> pd.DataFrame:
        self.df = pd.read_csv(self.file_path, dtype=str)
        return self.df

    def clean_columns(self) -> pd.DataFrame:
        if self.df is None:
            msg = "DataFrame not loaded. Run load_csv() first."
            raise ValueError(msg)

        self.df.columns = (
            self.df.columns.str.replace("-", " ", regex=False)
            .str.replace("/", " ", regex=False)
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w\s]", "", regex=True)
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
        )

        self.df.columns = (
            self.df.columns.str.strip()
            .str.lower()
            .str.replace(r"[^\w\s]", "", regex=True)
            .str.replace(r"\s+", "_", regex=True)
        )

        self.df = self.df.loc[:, ~self.df.columns.duplicated()]
        return self.df

    def _load_registration_mapping(self) -> pd.DataFrame:
        bursa_list_path = (
            self.base_dir
            / "bursa_dataset_and_cleaning"
            / "list_of_ids"
            / "outputs"
            / "bursa_company_list.csv"
        )
        matched_path = (
            self.base_dir
            / "bursa_dataset_and_cleaning"
            / "scraping_layer"
            / "combined_csv"
            / "matched_companies_from_ssm.csv"
        )

        # Explicitly set all relevant columns to string to avoid float conversion issues with NaN
        matched_dtypes = {"companyNo": str, "oldCompanyNo": str, "company_id": str}
        matched = pd.read_csv(matched_path, dtype=matched_dtypes)
        bursa_list = pd.read_csv(bursa_list_path, dtype={"company_id": str})

        # Define a robust function to clean registration numbers
        def clean_reg_num(series: pd.Series) -> pd.Series:
            # Fill NA with empty string, convert to string, then remove .0 if it's at the end
            return (
                series.fillna("")
                .astype(str)
                .str.replace(r"\\.0$", "", regex=True)
                .str.strip()
            )

        # Clean all potential registration number columns
        for col in ["companyNo", "oldCompanyNo"]:
            if col in matched.columns:
                matched[col] = clean_reg_num(matched[col])

        bursa_list["company_id"] = bursa_list["company_id"].str.strip().str.zfill(4)

        merge = matched.merge(bursa_list, on="company_name", how="inner")
        merge = merge.drop(columns="company_type", errors="ignore")
        merge = merge.rename(
            columns={
                "company_name": "company_name_bursa",
                "name_db": "company_name_api",
                "companyNo": "registration_number",
                "oldCompanyNo": "old_registration_number",
            },
        )
        # Ensure final columns are also strings
        merge["company_id"] = merge["company_id"].astype(str).str.strip()
        if "registration_number" in merge.columns:
            merge["registration_number"] = clean_reg_num(merge["registration_number"])
        if "old_registration_number" in merge.columns:
            merge["old_registration_number"] = clean_reg_num(
                merge["old_registration_number"]
            )

        return merge

    def upload_to_sql(self, *, replace: bool = False) -> None:
        if self.df is None:
            msg = "DataFrame not loaded. Run load_csv() first."
            raise ValueError(msg)

        if self.table_name not in SAFE_QUERIES:
            msg = f"Unsafe table name: {self.table_name}"
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
        else:
            query = SAFE_QUERIES[self.table_name]
            with self.engine.connect() as conn:
                existing = conn.execute(query).fetchall()

            key_columns = [
                col.strip()
                for col in query.text.split("SELECT DISTINCT")[1]
                .split("FROM")[0]
                .split(",")
            ]

            for col in key_columns:
                self.df[col] = self.df[col].astype(str).str.strip()

            self.df["key"] = list(
                zip(*(self.df[col] for col in key_columns), strict=False)
            )
            existing_pairs = {
                tuple(str(val).strip() for val in row) for row in existing
            }

            original_len = len(self.df)
            self.df = self.df[~self.df["key"].isin(existing_pairs)].drop(columns="key")
            new_rows = len(self.df)

            if new_rows > 0:
                self.df.to_sql(
                    self.table_name,
                    self.engine,
                    schema="public",
                    index=False,
                    if_exists="append",
                )
                logger.info(
                    "Appended %d new rows to '%s' (from original %d).",
                    new_rows,
                    self.table_name,
                    original_len,
                )
            else:
                logger.info("No new rows to append to '%s'.", self.table_name)
