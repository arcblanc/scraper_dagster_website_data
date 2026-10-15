"""Data extraction module for Malaysian business registry clustering."""

import logging
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    COMBINED_CSV,
    PG_DATABASE,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    ROB_CSV,
    ROB_TABLE,
    ROC_CSV,
    ROC_TABLE,
    validate_config,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


class RegistryDataExtractor:
    """Extract Malaysian business registry data from PostgreSQL."""

    def __init__(self):
        """Initialize the data extractor with database connection."""
        validate_config()
        self.connection_params = {
            "host": PG_HOST,
            "port": PG_PORT,
            "database": PG_DATABASE,
            "user": PG_USER,
            "password": PG_PASSWORD,
        }

    def get_connection(self):
        """Get a database connection."""
        try:
            conn = psycopg2.connect(**self.connection_params)
            logger.info("✅ Database connection established")
            return conn
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise

    def extract_rob_data(self, limit: int | None = None) -> pd.DataFrame:
        """Extract Registry of Businesses data."""
        start_time = time.time()
        logger.info("🔄 Starting ROB data extraction...")

        query = f"""
        SELECT
            registration_number,
            business_commenced as date_of_registration,
            state,
            business_name as company_name,
            business_type as nature_of_business
        FROM {ROB_TABLE}
        WHERE registration_number IS NOT NULL
        AND business_name IS NOT NULL
        """

        if limit:
            query += f" LIMIT {limit}"
            logger.info(f"📋 Using limit: {limit} records")

        try:
            with self.get_connection() as conn:
                logger.info("📥 Executing ROB query...")
                df = pd.read_sql_query(query, conn)
                elapsed_time = time.time() - start_time
                logger.info(
                    f"📊 Extracted {len(df):,} ROB records in {elapsed_time:.2f} seconds ({len(df) / elapsed_time:.0f} records/sec)"
                )
                return df
        except Exception as e:
            logger.error(f"❌ Failed to extract ROB data: {e}")
            raise

    def extract_roc_data(self, limit: int | None = None) -> pd.DataFrame:
        """Extract Registry of Companies data."""
        start_time = time.time()
        logger.info("🔄 Starting ROC data extraction...")

        query = f"""
        SELECT
            registration_number,
            business_commenced as date_of_registration,
            state,
            business_name as company_name,
            business_type as nature_of_business
        FROM {ROC_TABLE}
        WHERE registration_number IS NOT NULL
        AND business_name IS NOT NULL
        """

        if limit:
            query += f" LIMIT {limit}"
            logger.info(f"📋 Using limit: {limit} records")

        try:
            with self.get_connection() as conn:
                logger.info("📥 Executing ROC query...")
                df = pd.read_sql_query(query, conn)
                elapsed_time = time.time() - start_time
                logger.info(
                    f"📊 Extracted {len(df):,} ROC records in {elapsed_time:.2f} seconds ({len(df) / elapsed_time:.0f} records/sec)"
                )
                return df
        except Exception as e:
            logger.error(f"❌ Failed to extract ROC data: {e}")
            raise

    def extract_all_data(self, limit: int | None = None) -> pd.DataFrame:
        """Extract and combine both ROB and ROC data."""
        start_time = time.time()
        logger.info("🔄 Starting combined data extraction...")

        # Extract ROB data
        rob_df = self.extract_rob_data(limit)
        rob_df["source"] = "ROB"

        # Extract ROC data
        roc_df = self.extract_roc_data(limit)
        roc_df["source"] = "ROC"

        # Combine datasets
        logger.info("🔗 Combining ROB and ROC datasets...")
        combine_start = time.time()
        combined_df = pd.concat([rob_df, roc_df], ignore_index=True)
        combine_time = time.time() - combine_start
        logger.info(
            f"📊 Combined {len(rob_df):,} ROB + {len(roc_df):,} ROC = {len(combined_df):,} total records in {combine_time:.2f}s"
        )

        # Clean and validate data
        logger.info("🧹 Starting data cleaning...")
        clean_start = time.time()
        combined_df = self._clean_data(combined_df)
        clean_time = time.time() - clean_start

        total_time = time.time() - start_time
        logger.info(f"✅ Combined dataset: {len(combined_df):,} records")
        logger.info(f"⏱️  Total extraction time: {total_time:.2f} seconds")
        logger.info(f"🧹 Data cleaning time: {clean_time:.2f} seconds")

        return combined_df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate the extracted data."""
        initial_count = len(df)

        # Remove duplicates
        df = df.drop_duplicates(subset=["registration_number"])

        # Convert date_of_registration to datetime
        df["date_of_registration"] = pd.to_datetime(
            df["date_of_registration"], errors="coerce"
        )

        # Remove records with invalid dates
        df = df.dropna(subset=["date_of_registration"])

        # Clean text fields
        df["company_name"] = df["company_name"].str.strip()
        df["nature_of_business"] = df["nature_of_business"].str.strip()
        df["state"] = df["state"].str.strip()

        # Remove records with missing essential fields
        df = df.dropna(subset=["company_name", "nature_of_business"])

        # Filter out records with very short names (likely errors)
        df = df[df["company_name"].str.len() > 2]

        final_count = len(df)
        logger.info(f"🧹 Data cleaning: {initial_count} → {final_count} records")

        return df

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """Save DataFrame to CSV file."""
        start_time = time.time()
        try:
            logger.info(f"💾 Saving {len(df):,} records to {filepath}...")
            df.to_csv(filepath, index=False)
            elapsed_time = time.time() - start_time
            logger.info(
                f"✅ Saved {len(df):,} records to {filepath} in {elapsed_time:.2f} seconds"
            )
        except Exception as e:
            logger.error(f"❌ Failed to save CSV: {e}")
            raise

    def extract_and_save(self, limit: int | None = None) -> None:
        """Extract data and save to CSV files."""
        start_time = time.time()
        logger.info("🚀 Starting complete data extraction and save process...")

        # Extract ROB data
        logger.info("=" * 60)
        logger.info("📋 PHASE 1: ROB Data Extraction")
        logger.info("=" * 60)
        rob_df = self.extract_rob_data(limit)
        save_start = time.time()
        self.save_to_csv(rob_df, ROB_CSV)
        save_time = time.time() - save_start
        logger.info(f"💾 ROB data saved in {save_time:.2f} seconds")

        # Extract ROC data
        logger.info("=" * 60)
        logger.info("📋 PHASE 2: ROC Data Extraction")
        logger.info("=" * 60)
        roc_df = self.extract_roc_data(limit)
        save_start = time.time()
        self.save_to_csv(roc_df, ROC_CSV)
        save_time = time.time() - save_start
        logger.info(f"💾 ROC data saved in {save_time:.2f} seconds")

        # Create combined data from already extracted data
        logger.info("=" * 60)
        logger.info("📋 PHASE 3: Combined Data Processing")
        logger.info("=" * 60)
        logger.info("🔗 Combining ROB and ROC datasets...")
        combine_start = time.time()

        # Add source column to each dataset
        rob_df_copy = rob_df.copy()
        rob_df_copy["source"] = "ROB"
        roc_df_copy = roc_df.copy()
        roc_df_copy["source"] = "ROC"

        # Combine datasets
        combined_df = pd.concat([rob_df_copy, roc_df_copy], ignore_index=True)
        combine_time = time.time() - combine_start
        logger.info(
            f"📊 Combined {len(rob_df):,} ROB + {len(roc_df):,} ROC = {len(combined_df):,} total records in {combine_time:.2f}s"
        )

        # Clean and validate combined data
        logger.info("🧹 Starting data cleaning...")
        clean_start = time.time()
        combined_df = self._clean_data(combined_df)
        clean_time = time.time() - clean_start
        logger.info(f"🧹 Data cleaning completed in {clean_time:.2f} seconds")

        # Save combined data
        save_start = time.time()
        self.save_to_csv(combined_df, COMBINED_CSV)
        save_time = time.time() - save_start
        logger.info(f"💾 Combined data saved in {save_time:.2f} seconds")

        total_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("🎉 DATA EXTRACTION COMPLETED!")
        logger.info("=" * 60)
        logger.info("📊 Total records processed:")
        logger.info(f"   - ROB: {len(rob_df):,} records")
        logger.info(f"   - ROC: {len(roc_df):,} records")
        logger.info(f"   - Combined: {len(combined_df):,} records")
        logger.info(
            f"⏱️  Total time: {total_time:.2f} seconds ({total_time / 60:.1f} minutes)"
        )
        logger.info(
            f"📈 Average speed: {(len(rob_df) + len(roc_df)) / total_time:.0f} records/second"
        )
        logger.info("✅ Data extraction and save process completed!")


def main():
    """Main function to run data extraction."""
    try:
        extractor = RegistryDataExtractor()

        # For testing, you can limit the number of records
        # extractor.extract_and_save(limit=1000)

        # For full extraction
        extractor.extract_and_save()

    except Exception as e:
        logger.error(f"❌ Data extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
