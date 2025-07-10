#!/usr/bin/env python3
"""Test script to verify profile scraper data quality
Checks for:
1. company_id presence and string type
2. Name column extraction
3. Data completeness
4. URL presence
"""

import logging
import sys
from pathlib import Path

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


def test_company_data(company_id: str = "6254"):
    """Test data quality for a specific company"""
    base_path = (
        Path(__file__).parent.parent / "scraping_layer" / "outputs" / "profile_details"
    )

    # Define test sections and their expected columns
    test_sections = {
        "profile": {
            "file": base_path / "profile_expanded" / f"{company_id}.csv",
            "required_cols": ["company_id", "About", "company_url"],
            "name_col": None,  # Profile doesn't have a name column
            "min_rows": 1,
        },
        "management": {
            "file": base_path / "management_expanded" / f"{company_id}.csv",
            "required_cols": ["company_id", "Name", "company_url"],
            "name_col": "Name",
            "min_rows": 1,
        },
        "ownership": {
            "file": base_path / "ownership_expanded" / f"{company_id}.csv",
            "required_cols": ["company_id", "Investor Name", "company_url"],
            "name_col": "Investor Name",
            "min_rows": 1,
        },
        "top10": {
            "file": base_path / "top_10_expanded" / f"{company_id}.csv",
            "required_cols": ["company_id", "Investor Name", "company_url"],
            "name_col": "Investor Name",
            "min_rows": 1,
        },
        "insider": {
            "file": base_path / "insider_expanded" / f"{company_id}.csv",
            "required_cols": ["company_id", "Name", "company_url"],
            "name_col": "Name",
            "min_rows": 1,
        },
    }

    results = {}

    for section, config in test_sections.items():
        logger.info(f"🔍 Testing {section} data...")

        if not config["file"].exists():
            logger.error(f"❌ {section} file not found: {config['file']}")
            results[section] = {"status": "FAIL", "error": "File not found"}
            continue

        try:
            # Load data
            df = pd.read_csv(config["file"])
            logger.info(f"📊 Loaded {len(df)} rows from {section}")

            # Test 1: Check required columns
            missing_cols = [
                col for col in config["required_cols"] if col not in df.columns
            ]
            if missing_cols:
                logger.error(f"❌ {section}: Missing required columns: {missing_cols}")
                results[section] = {
                    "status": "FAIL",
                    "error": f"Missing columns: {missing_cols}",
                }
                continue

            # Test 2: Check company_id is string and consistent
            if "company_id" in df.columns:
                df["company_id"] = df["company_id"].astype(str)
                unique_ids = df["company_id"].unique()
                if len(unique_ids) != 1 or unique_ids[0] != company_id:
                    logger.error(
                        f"❌ {section}: company_id mismatch. Expected: {company_id}, Got: {unique_ids}"
                    )
                    results[section] = {
                        "status": "FAIL",
                        "error": f"company_id mismatch: {unique_ids}",
                    }
                    continue
                logger.info(
                    f"✅ {section}: company_id is correct string: {unique_ids[0]}"
                )

            # Test 3: Check minimum rows
            if len(df) < config["min_rows"]:
                logger.error(
                    f"❌ {section}: Insufficient rows. Expected: {config['min_rows']}, Got: {len(df)}"
                )
                results[section] = {
                    "status": "FAIL",
                    "error": f"Insufficient rows: {len(df)}",
                }
                continue

            # Test 4: Check name column if applicable
            if config["name_col"]:
                name_col = config["name_col"]
                if name_col in df.columns:
                    # Check for non-empty names
                    non_empty_names = df[name_col].dropna()
                    non_empty_names = non_empty_names[non_empty_names.str.strip() != ""]

                    if len(non_empty_names) == 0:
                        logger.error(
                            f"❌ {section}: No valid names found in {name_col}"
                        )
                        results[section] = {
                            "status": "FAIL",
                            "error": f"No valid names in {name_col}",
                        }
                        continue
                    logger.info(
                        f"✅ {section}: Found {len(non_empty_names)} valid names"
                    )
                    # Show sample names
                    sample_names = non_empty_names.head(3).tolist()
                    logger.info(f"📝 {section} sample names: {sample_names}")
                else:
                    logger.error(f"❌ {section}: Name column {name_col} not found")
                    results[section] = {
                        "status": "FAIL",
                        "error": f"Name column {name_col} not found",
                    }
                    continue

            # Test 5: Check company_url presence
            if "company_url" in df.columns:
                non_empty_urls = df["company_url"].dropna()
                if len(non_empty_urls) == 0:
                    logger.error(f"❌ {section}: No company_url values found")
                    results[section] = {
                        "status": "FAIL",
                        "error": "No company_url values",
                    }
                    continue
                logger.info(
                    f"✅ {section}: company_url present in {len(non_empty_urls)} rows"
                )

            # Test 6: Check data types
            logger.info(f"📋 {section} data types:")
            for col in df.columns:
                dtype = str(df[col].dtype)
                logger.info(f"   {col}: {dtype}")

            results[section] = {
                "status": "PASS",
                "rows": len(df),
                "columns": list(df.columns),
                "company_id_type": str(df["company_id"].dtype)
                if "company_id" in df.columns
                else "N/A",
            }

        except Exception as e:
            logger.error(f"❌ {section}: Error processing data: {e}")
            results[section] = {"status": "FAIL", "error": str(e)}

    return results


def test_combined_data():
    """Test the combined CSV files"""
    base_path = Path(__file__).parent.parent / "scraping_layer" / "combined_csv"

    combined_files = {
        "combined_profile.csv": ["company_id", "About", "company_url"],
        "combined_management.csv": ["company_id", "Name", "company_url"],
        "combined_ownership.csv": ["company_id", "Investor Name", "company_url"],
        "combined_top10.csv": ["company_id", "Investor Name", "company_url"],
        "combined_insider.csv": ["company_id", "Name", "company_url"],
    }

    logger.info("🔍 Testing combined CSV files...")

    missing_files = []
    existing_files = []

    for filename, required_cols in combined_files.items():
        file_path = base_path / filename

        if not file_path.exists():
            missing_files.append(filename)
            logger.warning(f"⚠️ Combined file not found: {file_path}")
            continue

        existing_files.append(filename)

        try:
            df = pd.read_csv(file_path)
            logger.info(f"📊 {filename}: {len(df)} rows")

            # Check required columns
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"❌ {filename}: Missing columns: {missing_cols}")
            else:
                logger.info(f"✅ {filename}: All required columns present")

            # Check company_id is string
            if "company_id" in df.columns:
                df["company_id"] = df["company_id"].astype(str)
                logger.info(f"✅ {filename}: company_id converted to string type")

                # Check for unique company_ids
                unique_companies = df["company_id"].nunique()
                logger.info(f"📈 {filename}: {unique_companies} unique companies")

        except Exception as e:
            logger.error(f"❌ {filename}: Error processing: {e}")

    if missing_files:
        logger.warning(f"⚠️ Missing combined files: {missing_files}")
        logger.info("💡 To generate these files, run the profile scraper:")
        logger.info("   cd ../scraping_layer && python profile_scraper.py")

    if existing_files:
        logger.info(
            f"✅ Successfully tested {len(existing_files)} existing combined files"
        )

    return len(missing_files) == 0


def test_individual_files():
    """Test individual company CSV files"""
    base_path = (
        Path(__file__).parent.parent / "scraping_layer" / "outputs" / "profile_details"
    )

    # Test each expanded folder
    expanded_folders = {
        "profile_expanded": ["company_id", "About", "company_url"],
        "management_expanded": ["company_id", "Name", "company_url"],
        "ownership_expanded": ["company_id", "Investor Name", "company_url"],
        "top_10_expanded": ["company_id", "Investor Name", "company_url"],
        "insider_expanded": ["company_id", "Name", "company_url"],
    }

    logger.info("🔍 Testing individual company CSV files...")

    total_files_tested = 0
    empty_folders = []

    for folder_name, required_cols in expanded_folders.items():
        folder_path = base_path / folder_name

        if not folder_path.exists():
            logger.warning(f"⚠️ Folder not found: {folder_path}")
            continue

        csv_files = list(folder_path.glob("*.csv"))

        if not csv_files:
            empty_folders.append(folder_name)
            logger.warning(f"⚠️ No CSV files found in {folder_name}")
            continue

        logger.info(f"📁 {folder_name}: {len(csv_files)} files")

        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                total_files_tested += 1

                # Check required columns
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logger.error(f"❌ {csv_file.name}: Missing columns: {missing_cols}")
                else:
                    logger.info(
                        f"✅ {csv_file.name}: {len(df)} rows, all required columns present"
                    )

                # Check company_id is string
                if "company_id" in df.columns:
                    df["company_id"] = df["company_id"].astype(str)

                    # Check for sample names
                    if "Name" in df.columns:
                        sample_names = df["Name"].dropna().head(3).tolist()
                        if sample_names:
                            logger.info(
                                f"📝 {csv_file.name}: Sample names: {sample_names}"
                            )

            except Exception as e:
                logger.error(f"❌ {csv_file.name}: Error processing: {e}")

    if empty_folders:
        logger.warning(f"⚠️ Empty folders: {empty_folders}")
        logger.info("💡 To generate these files, run the profile scraper:")
        logger.info("   cd ../scraping_layer && python profile_scraper.py")

    if total_files_tested > 0:
        logger.info(f"✅ Successfully tested {total_files_tested} individual CSV files")

    return total_files_tested > 0


def main():
    """Main test function"""
    logger.info("🚀 Starting profile scraper data quality tests...")

    # Test individual company data
    logger.info("=" * 60)
    logger.info("TESTING INDIVIDUAL COMPANY DATA")
    logger.info("=" * 60)

    results = test_company_data("6254")

    # Print summary
    logger.info("=" * 60)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 60)

    passed = 0
    failed = 0

    for section, result in results.items():
        status = result["status"]
        if status == "PASS":
            passed += 1
            logger.info(f"✅ {section}: PASS ({result.get('rows', 'N/A')} rows)")
        else:
            failed += 1
            logger.error(f"❌ {section}: FAIL - {result.get('error', 'Unknown error')}")

    logger.info(f"📊 Overall: {passed} passed, {failed} failed")

    # Test combined data
    logger.info("=" * 60)
    logger.info("TESTING COMBINED CSV FILES")
    logger.info("=" * 60)

    test_combined_data()

    # Test individual files
    logger.info("=" * 60)
    logger.info("TESTING INDIVIDUAL COMPANY CSV FILES")
    logger.info("=" * 60)

    test_individual_files()

    logger.info("=" * 60)
    logger.info("TEST COMPLETED")
    logger.info("=" * 60)

    if failed == 0:
        logger.info("🎉 All tests passed! Profile scraper is working correctly.")
        return 0
    logger.error(f"⚠️ {failed} tests failed. Please check the issues above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
