"""Data extraction module for SSM (Suruhanjaya Syarikat Malaysia) data."""

import logging
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    PG_DATABASE,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    validate_config,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# SSM table configurations
SSM_TABLES = {
    "company_financial_records": "company_financial_records.csv",
    "company_profiles": "company_profiles.csv",
}


class SSMDataExtractor:
    """Extract SSM (Suruhanjaya Syarikat Malaysia) data from PostgreSQL."""

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
        self.data_dir = Path(__file__).resolve().parent / "ssm_data"

    def get_connection(self):
        """Get a database connection."""
        try:
            conn = psycopg2.connect(**self.connection_params)
            logger.info("✅ Database connection established")
            return conn
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise

    def get_table_count(self, table_name: str) -> int:
        """Get the total number of records in a table."""
        query = f"SELECT COUNT(*) FROM {table_name}"
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    count = cursor.fetchone()[0]
                    return count
        except Exception as e:
            logger.error(f"❌ Failed to get count for {table_name}: {e}")
            return 0

    def extract_table_data(
        self, table_name: str, limit: int | None = None
    ) -> pd.DataFrame:
        """Extract data from a specific SSM table."""
        start_time = time.time()
        logger.info(f"🔄 Starting extraction from {table_name}...")

        # Get total count first
        total_count = self.get_table_count(table_name)
        logger.info(f"📊 Total records in {table_name}: {total_count:,}")

        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
            logger.info(f"📋 Using limit: {limit:,} records")

        try:
            with self.get_connection() as conn:
                logger.info(f"📥 Executing query on {table_name}...")
                df = pd.read_sql_query(query, conn)
                elapsed_time = time.time() - start_time
                logger.info(
                    f"📊 Extracted {len(df):,} records from {table_name} in {elapsed_time:.2f} seconds ({len(df) / elapsed_time:.0f} records/sec)"
                )
                return df
        except Exception as e:
            logger.error(f"❌ Failed to extract data from {table_name}: {e}")
            raise

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

    def extract_all_ssm_data(self, limit: int | None = None) -> None:
        """Extract data from all SSM tables."""
        start_time = time.time()
        logger.info(
            "🚀 Starting SSM (Suruhanjaya Syarikat Malaysia) data extraction process..."
        )
        logger.info(f"📋 Tables to extract: {len(SSM_TABLES)}")

        total_records = 0
        successful_extractions = 0

        for table_name, csv_filename in SSM_TABLES.items():
            try:
                logger.info("=" * 60)
                logger.info(f"📋 EXTRACTING: {table_name}")
                logger.info("=" * 60)

                # Extract data
                df = self.extract_table_data(table_name, limit)

                # Save to CSV
                csv_path = self.data_dir / csv_filename
                self.save_to_csv(df, csv_path)

                total_records += len(df)
                successful_extractions += 1

                logger.info(f"✅ Successfully processed {table_name} -> {csv_filename}")

            except Exception as e:
                logger.error(f"❌ Failed to process {table_name}: {e}")
                continue

        total_time = time.time() - start_time

        logger.info("=" * 60)
        logger.info("🎉 SSM DATA EXTRACTION COMPLETED!")
        logger.info("=" * 60)
        logger.info("📊 Summary:")
        logger.info(
            f"   - Tables processed: {successful_extractions}/{len(SSM_TABLES)}"
        )
        logger.info(f"   - Total records extracted: {total_records:,}")
        logger.info(
            f"⏱️  Total time: {total_time:.2f} seconds ({total_time / 60:.1f} minutes)"
        )
        if total_records > 0:
            logger.info(
                f"📈 Average speed: {total_records / total_time:.0f} records/second"
            )

        if successful_extractions == len(SSM_TABLES):
            logger.info("✅ All SSM tables extracted successfully!")
        else:
            logger.warning(
                f"⚠️  {len(SSM_TABLES) - successful_extractions} tables failed to extract"
            )

    def extract_single_table(self, table_name: str, limit: int | None = None) -> None:
        """Extract data from a single SSM table."""
        if table_name not in SSM_TABLES:
            logger.error(f"❌ Table {table_name} not found in SSM_TABLES configuration")
            return

        csv_filename = SSM_TABLES[table_name]
        logger.info(f"🚀 Starting single table extraction: {table_name}")

        try:
            # Extract data
            df = self.extract_table_data(table_name, limit)

            # Save to CSV
            csv_path = self.data_dir / csv_filename
            self.save_to_csv(df, csv_path)

            logger.info(f"✅ Successfully extracted {table_name} -> {csv_filename}")

        except Exception as e:
            logger.error(f"❌ Failed to extract {table_name}: {e}")


def main():
    """Main function to run SSM data extraction."""
    try:
        extractor = SSMDataExtractor()

        # Check command line arguments
        if len(sys.argv) > 1:
            if sys.argv[1] == "--single" and len(sys.argv) > 2:
                # Extract single table
                table_name = sys.argv[2]
                limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
                extractor.extract_single_table(table_name, limit)
            elif sys.argv[1] == "--limit" and len(sys.argv) > 2:
                # Extract all tables with limit
                limit = int(sys.argv[2])
                extractor.extract_all_ssm_data(limit)
            else:
                logger.error("❌ Invalid arguments. Usage:")
                logger.error(
                    "  python ssm_extractor.py                    # Extract all tables"
                )
                logger.error(
                    "  python ssm_extractor.py --limit 1000      # Extract all with limit"
                )
                logger.error(
                    "  python ssm_extractor.py --single table_name [limit]  # Extract single table"
                )
        else:
            # Extract all tables
            extractor.extract_all_ssm_data()

    except Exception as e:
        logger.error(f"❌ SSM data extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
