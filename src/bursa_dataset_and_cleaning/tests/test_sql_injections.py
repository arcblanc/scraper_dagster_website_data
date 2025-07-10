#!/usr/bin/env python3
"""Test script for enhanced SQL injection processors
Tests balance sheet, cash flow, and income statement injection scripts
"""

import logging
import sys
from pathlib import Path
from typing import Any

print("--- SCRIPT START ---")

# Setup path to ensure imports work
ROOT_DIR = (
    Path(__file__).resolve().parents[1]
)  # Go up 1 level to bursa_dataset_and_cleaning
sys.path.insert(0, str(ROOT_DIR))

# Import the processors
from sql_inject.balance_inject import BalanceSheetProcessor
from sql_inject.cashflow_injection import CashFlowProcessor
from sql_inject.income_injection import IncomeProcessor
from sql_inject.insider_injection import InsiderProcessor
from sql_inject.management_injection import ManagementProcessor
from sql_inject.ownership_injection import OwnershipProcessor
from sql_inject.profile_injection import CompanyProfileProcessor
from sql_inject.top_10_injection import Top10Processor

# ─── LOGGING ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


class SQLInjectionTester:
    """Test class for SQL injection processors"""

    def __init__(self):
        self.test_results = {}

    def test_processor_initialization(
        self, processor_class: Any, processor_name: str
    ) -> bool:
        """Test if processor can be initialized and CSV file exists"""
        try:
            logger.info(f"🔍 Testing {processor_name} initialization...")
            processor = processor_class()

            # Check if CSV file exists
            if not processor.file_path.exists():
                logger.error(
                    f"❌ {processor_name}: CSV file not found: {processor.file_path}"
                )
                return False

            logger.info(f"✅ {processor_name}: Initialized successfully")
            logger.info(f"   • CSV file: {processor.file_path}")
            logger.info(f"   • Table name: {processor.table_name}")
            return True

        except Exception as e:
            logger.error(f"❌ {processor_name}: Initialization failed: {e}")
            return False

    def test_data_processing(self, processor_class: Any, processor_name: str) -> bool:
        """Test if processor can load and process data"""
        try:
            logger.info(f"🔄 Testing {processor_name} data processing...")
            processor = processor_class()

            # Test data loading
            df_original = processor.load_csv()
            logger.info(f"   • Original CSV rows: {len(df_original)}")

            # Test data processing
            processor.process()
            df_processed = processor.df

            if df_processed is None or len(df_processed) == 0:
                logger.error(f"❌ {processor_name}: No data after processing")
                return False

            logger.info(f"   • Processed rows: {len(df_processed)}")
            logger.info(f"   • Columns: {list(df_processed.columns)}")

            # If testing the Company Profile, check for merged market data columns
            if "Company Profile" in processor_name:
                logger.info("   • Verifying market data columns for Company Profile...")
                expected_market_cols = ["market_cap_mil", "volume", "source_url"]
                missing_cols = [
                    col
                    for col in expected_market_cols
                    if col not in df_processed.columns
                ]
                if missing_cols:
                    logger.error(
                        f"❌ {processor_name}: Missing merged market data columns: {missing_cols}"
                    )
                    return False
                logger.info(
                    f"✅ {processor_name}: Successfully found merged market data columns."
                )

            # Check for required columns
            required_cols = ["registration_number"]
            if (
                "financial" in processor_name.lower()
            ):  # Financial processors need fiscal_date
                required_cols.append("fiscal_date")

            missing_cols = [
                col for col in required_cols if col not in df_processed.columns
            ]

            if missing_cols:
                logger.error(
                    f"❌ {processor_name}: Missing required columns: {missing_cols}"
                )
                return False

            logger.info(f"✅ {processor_name}: Data processing successful")
            return True

        except Exception as e:
            logger.error(f"❌ {processor_name}: Data processing failed: {e}")
            return False

    def test_deduplication_logic(
        self, processor_class: Any, processor_name: str
    ) -> bool:
        """Test the enhanced deduplication logic"""
        try:
            logger.info(f"🧮 Testing {processor_name} deduplication logic...")
            processor = processor_class()
            processor.process()

            # Test the enhanced upload method (without actually uploading)
            if not hasattr(processor, "upload_to_sql_enhanced"):
                logger.error(f"❌ {processor_name}: Enhanced upload method not found")
                return False

            # Check if the method exists and is callable
            if not callable(processor.upload_to_sql_enhanced):
                logger.error(
                    f"❌ {processor_name}: Enhanced upload method is not callable"
                )
                return False

            logger.info(f"✅ {processor_name}: Enhanced upload method available")
            return True

        except Exception as e:
            logger.error(f"❌ {processor_name}: Deduplication logic test failed: {e}")
            return False

    def test_data_quality(self, processor_class: Any, processor_name: str) -> bool:
        """Test data quality checks"""
        try:
            logger.info(f"📊 Testing {processor_name} data quality...")
            processor = processor_class()
            processor.process()
            df = processor.df

            # Check that registration_number column exists
            if "registration_number" not in df.columns:
                logger.error(
                    f"❌ {processor_name}: 'registration_number' column not found"
                )
                return False

            # Ensure registration_number is string type
            if df["registration_number"].dtype != "object":
                logger.error(
                    f"❌ {processor_name}: registration_number is not string type (current: {df['registration_number'].dtype})"
                )
                return False

            # Check for trailing .0 in registration numbers - THIS IS THE KEY TEST
            trailing_zero_mask = (
                df["registration_number"].astype(str).str.endswith(".0")
            )
            if trailing_zero_mask.any():
                logger.error(
                    f"❌ {processor_name}: Found registration numbers ending with '.0'."
                )
                offending_samples = (
                    df[trailing_zero_mask]["registration_number"].head(3).tolist()
                )
                logger.error(f"   • Offending samples: {offending_samples}")
                return False
            logger.info(
                f"✅ {processor_name}: All registration numbers are clean (no trailing '.0')."
            )

            # Define key columns based on processor type
            key_columns = ["registration_number"]
            if (
                "financial" in processor_name.lower()
                or "income" in processor_name.lower()
                or "balance" in processor_name.lower()
                or "cash" in processor_name.lower()
            ):
                key_columns.append("fiscal_date")
            elif "management" in processor_name.lower():
                key_columns.extend(["since", "role", "designation"])
            elif "insider" in processor_name.lower():
                key_columns.extend(["transaction_date", "name", "ownership_"])
            elif "top 10" in processor_name.lower():
                key_columns.extend(["filing_date", "investor_name", "ownership_"])
            elif "ownership" in processor_name.lower():
                key_columns.extend(
                    ["ownership_", "no_of_investors", "investor_name", "value_m_usd"]
                )

            # Check for null values in key columns
            null_counts = df[key_columns].isnull().sum()
            for col, count in null_counts.items():
                if count > 0:
                    logger.warning(
                        f"⚠️ {processor_name}: {count} null values in '{col}'"
                    )

            # Check for empty strings
            empty_reg = (df["registration_number"] == "").sum()

            if empty_reg > 0:
                logger.warning(
                    f"⚠️ {processor_name}: {empty_reg} empty registration numbers"
                )

            # Only check fiscal_date for financial data
            if (
                "financial" in processor_name.lower()
                or "income" in processor_name.lower()
                or "balance" in processor_name.lower()
                or "cash" in processor_name.lower()
            ):
                if "fiscal_date" in df.columns:
                    empty_fiscal = (df["fiscal_date"] == "").sum()
                    if empty_fiscal > 0:
                        logger.warning(
                            f"⚠️ {processor_name}: {empty_fiscal} empty fiscal dates"
                        )

            # Check unique combinations
            unique_combinations = df[key_columns].drop_duplicates().shape[0]
            total_rows = len(df)

            if unique_combinations != total_rows:
                logger.warning(
                    f"⚠️ {processor_name}: {total_rows - unique_combinations} duplicate combinations found in source file"
                )

            logger.info(
                f"   • Unique registration numbers: {df['registration_number'].nunique()}"
            )
            for col in key_columns:
                if col != "registration_number":
                    logger.info(
                        f"   • Unique {col.replace('_', ' ')}s: {df[col].nunique()}"
                    )
            logger.info(f"   • Unique combinations: {unique_combinations}")

            # Check for incorrect float formatting (e.g., '12345.0')
            float_reg_numbers = df[
                df["registration_number"].astype(str).str.contains(r"\\.0$", na=False)
            ]
            if not float_reg_numbers.empty:
                logger.error(
                    f"❌ {processor_name}: Found {len(float_reg_numbers)} registration numbers ending in '.0'"
                )
                logger.error(
                    f"   Sample problematic numbers: {float_reg_numbers['registration_number'].head().tolist()}"
                )
                return False

            # Additional validation: Check that registration numbers are proper strings without decimal points
            decimal_pattern = df[
                df["registration_number"].astype(str).str.contains(r"\\.", na=False)
            ]
            if not decimal_pattern.empty:
                logger.error(
                    f"❌ {processor_name}: Found {len(decimal_pattern)} registration numbers with decimal points"
                )
                logger.error(
                    f"   Sample problematic numbers: {decimal_pattern['registration_number'].head().tolist()}"
                )
                return False

            # Check that registration numbers are numeric strings (no letters, only digits)
            non_numeric_pattern = df[
                ~df["registration_number"].astype(str).str.match(r"^\d+$", na=False)
            ]
            if not non_numeric_pattern.empty:
                logger.warning(
                    f"⚠️ {processor_name}: Found {len(non_numeric_pattern)} non-numeric registration numbers"
                )
                logger.warning(
                    f"   Sample non-numeric numbers: {non_numeric_pattern['registration_number'].head().tolist()}"
                )

            logger.info(f"✅ {processor_name}: Data quality check completed")
            return True

        except Exception as e:
            logger.error(f"❌ {processor_name}: Data quality test failed: {e}")
            return False

    def test_sample_data(self, processor_class: Any, processor_name: str) -> bool:
        """Test sample data extraction"""
        try:
            logger.info(f"📝 Testing {processor_name} sample data...")
            processor = processor_class()
            processor.process()
            df = processor.df

            if df is None:
                logger.error(
                    f"❌ {processor_name}: DataFrame is None after processing."
                )
                return False

            # Show sample data based on processor type
            sample_cols = ["registration_number"]
            if (
                "financial" in processor_name.lower()
                or "income" in processor_name.lower()
                or "balance" in processor_name.lower()
                or "cash" in processor_name.lower()
            ):
                sample_cols.append("fiscal_date")
            elif "profile" in processor_name.lower():
                sample_cols.append("company_name_bursa")
            elif "management" in processor_name.lower():
                sample_cols.extend(["name", "role", "designation"])
            elif "insider" in processor_name.lower():
                sample_cols.extend(["name", "ownership_", "transaction_date"])
            elif "top 10" in processor_name.lower():
                sample_cols.extend(["investor_name", "ownership_", "filing_date"])
            elif "ownership" in processor_name.lower():
                sample_cols.extend(["investor_name", "ownership_", "value_m_usd"])

            sample = df[sample_cols].head(3)
            logger.info(f"📋 {processor_name} sample data:")
            for _idx, row in sample.iterrows():
                row_str = " - ".join([str(val) for val in row.values])
                logger.info(f"   • {row_str}")

            logger.info(f"✅ {processor_name}: Sample data extracted")
            return True

        except Exception as e:
            logger.error(f"❌ {processor_name}: Sample data test failed: {e}")
            return False

    def run_all_tests(self) -> dict:
        """Run all tests for all processors"""
        processors = [
            (BalanceSheetProcessor, "Balance Sheet (Financial)"),
            (CashFlowProcessor, "Cash Flow (Financial)"),
            (IncomeProcessor, "Income Statement (Financial)"),
            (CompanyProfileProcessor, "Company Profile"),
            (ManagementProcessor, "Management"),
            (InsiderProcessor, "Insider"),
            (OwnershipProcessor, "Ownership"),
            (Top10Processor, "Top 10"),
        ]

        logger.info("🚀 Starting SQL Injection Tests...")
        logger.info("=" * 60)

        for processor_class, processor_name in processors:
            logger.info(f"🧪 TESTING {processor_name.upper()} PROCESSOR")
            logger.info("-" * 40)

            results = {
                "initialization": self.test_processor_initialization(
                    processor_class, processor_name
                ),
                "data_processing": self.test_data_processing(
                    processor_class, processor_name
                ),
                "deduplication_logic": self.test_deduplication_logic(
                    processor_class, processor_name
                ),
                "data_quality": self.test_data_quality(processor_class, processor_name),
                "sample_data": self.test_sample_data(processor_class, processor_name),
            }

            self.test_results[processor_name] = results

            # Summary for this processor
            passed = sum(results.values())
            total = len(results)
            logger.info(f"📊 {processor_name} Results: {passed}/{total} tests passed")
            logger.info("=" * 60)

        return self.test_results

    def print_summary(self):
        """Print test summary"""
        logger.info("📋 TEST SUMMARY")
        logger.info("=" * 60)

        total_passed = 0
        total_tests = 0

        for processor_name, results in self.test_results.items():
            passed = sum(results.values())
            total = len(results)
            total_passed += passed
            total_tests += total

            status = "✅ PASS" if passed == total else "❌ FAIL"
            logger.info(f"{status} {processor_name}: {passed}/{total}")

            # Show failed tests
            failed_tests = [test for test, result in results.items() if not result]
            if failed_tests:
                logger.info(f"   ❌ Failed: {', '.join(failed_tests)}")

        logger.info("-" * 60)
        overall_status = (
            "✅ ALL TESTS PASSED"
            if total_passed == total_tests
            else "❌ SOME TESTS FAILED"
        )
        logger.info(
            f"{overall_status}: {total_passed}/{total_tests} total tests passed"
        )

        return total_passed == total_tests


def main():
    """Main test function"""
    print("--- MAIN FUNCTION START ---")
    tester = SQLInjectionTester()

    try:
        # Run all tests
        tester.run_all_tests()

        # Print summary
        success = tester.print_summary()

        print("--- MAIN FUNCTION END ---")
        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("⏹️ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Test execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("--- RUNNING SCRIPT ---")
    main()
    print("--- SCRIPT END ---")
