"""Dagster assets for the scraping_layer.
Handles URL finding and data scraping from Bursa Malaysia companies.
"""

import subprocess
import sys
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(
    group_name="scraping_layer",
    description="Finds Bursa URLs for companies using their IDs",
    deps=["bursa_company_list"],
    required_resource_keys={"s3"},
)
def company_urls(context: AssetExecutionContext) -> str:
    """Find Bursa URLs for all companies and upload to S3."""
    context.log.info("Starting URL finding for companies...")

    # Path to the URL finder
    url_finder_path = (
        Path(__file__).parent.parent.parent / "scraping_layer" / "bursa_url_finder.py"
    )

    try:
        # Run the URL finder with real-time output
        context.log.info("Running URL finder with real-time output...")
        subprocess.run(
            [sys.executable, str(url_finder_path)],
            capture_output=False,  # Don't capture output, let it display in real-time
            text=True,
            check=True,
        )
        context.log.info("URL finding completed successfully")

        # Read the generated CSV file
        output_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "cleaned"
            / "company_urls.csv"
        )
        company_urls_df = pd.read_csv(output_path)

        # Upload to S3
        s3_path = (
            "s3://onecredit-snowflake-data/data/bursa/company_urls/company_urls.csv"
        )
        context.log.info("📤 Uploading to S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            company_urls_df.to_csv(f, index=False)

        context.log.info("✅ Successfully uploaded to S3")

        # Add metadata
        context.add_output_metadata(
            {
                "num_companies_with_urls": MetadataValue.int(len(company_urls_df)),
                "columns": MetadataValue.json(list(company_urls_df.columns)),
                "sample_urls": MetadataValue.json(
                    company_urls_df.head(5).to_dict("records")
                ),
                "s3_path": MetadataValue.text(s3_path),
            }
        )
    except subprocess.CalledProcessError:
        context.log.exception("URL finding failed")
        raise
    except Exception:
        context.log.exception("Unexpected error in URL finding")
        raise
    return s3_path


@asset(
    group_name="scraping_layer",
    description="Scrapes balance sheet data from Bursa companies",
    deps=[company_urls],
    required_resource_keys={"s3"},
)
def balance_sheet_data(context: AssetExecutionContext) -> str:
    """Scrape balance sheet data for all companies and upload to S3."""
    context.log.info("Starting balance sheet scraping...")

    # Path to the balance sheet scraper
    scraper_path = (
        Path(__file__).parent.parent.parent / "scraping_layer" / "balance_sheet.py"
    )

    try:
        # Run the balance sheet scraper with real-time output
        context.log.info("Running balance sheet scraper with real-time output...")
        subprocess.run(
            [sys.executable, str(scraper_path)],
            capture_output=False,  # Don't capture output, let it display in real-time
            text=True,
            check=True,
        )
        context.log.info("Balance sheet scraping completed successfully")

        # Read the combined output file
        output_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_balance_sheets.csv"
        )
        balance_sheet_df = pd.read_csv(output_path)

        # Upload to S3
        s3_path = "s3://onecredit-snowflake-data/data/bursa/balance_sheets/complete_balance_sheets.csv"
        context.log.info("📤 Uploading to S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            balance_sheet_df.to_csv(f, index=False)

        context.log.info("✅ Successfully uploaded to S3")

        # Add metadata
        context.add_output_metadata(
            {
                "num_balance_sheet_records": MetadataValue.int(len(balance_sheet_df)),
                "columns": MetadataValue.json(list(balance_sheet_df.columns)),
                "unique_companies": MetadataValue.int(
                    int(balance_sheet_df["company_id"].nunique())
                ),
                "s3_path": MetadataValue.text(s3_path),
            }
        )
    except subprocess.CalledProcessError:
        context.log.exception("Balance sheet scraping failed")
        raise
    except Exception:
        context.log.exception("Unexpected error in balance sheet scraping")
        raise
    return s3_path


@asset(
    group_name="scraping_layer",
    description="Scrapes income statement data from Bursa companies",
    deps=[company_urls],
    required_resource_keys={"s3"},
)
def income_statement_data(context: AssetExecutionContext) -> str:
    """Scrape income statement data for all companies and upload to S3."""
    context.log.info("Starting income statement scraping...")

    # Path to the income statement scraper
    scraper_path = (
        Path(__file__).parent.parent.parent / "scraping_layer" / "income_statement.py"
    )

    try:
        # Run the income statement scraper with real-time output
        context.log.info("Running income statement scraper with real-time output...")
        subprocess.run(
            [sys.executable, str(scraper_path)],
            capture_output=False,  # Don't capture output, let it display in real-time
            text=True,
            check=True,
        )
        context.log.info("Income statement scraping completed successfully")

        # Read the combined output file
        output_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_income_statements.csv"
        )
        income_statement_df = pd.read_csv(output_path)

        # Upload to S3
        s3_path = "s3://onecredit-snowflake-data/data/bursa/income_statements/complete_income_statements.csv"
        context.log.info("📤 Uploading to S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            income_statement_df.to_csv(f, index=False)

        context.log.info("✅ Successfully uploaded to S3")

        # Add metadata
        context.add_output_metadata(
            {
                "num_income_statement_records": MetadataValue.int(
                    len(income_statement_df)
                ),
                "columns": MetadataValue.json(list(income_statement_df.columns)),
                "unique_companies": MetadataValue.int(
                    int(income_statement_df["company_id"].nunique())
                ),
                "s3_path": MetadataValue.text(s3_path),
            }
        )
    except subprocess.CalledProcessError:
        context.log.exception("Income statement scraping failed")
        raise
    except Exception:
        context.log.exception("Unexpected error in income statement scraping")
        raise
    return s3_path


@asset(
    group_name="scraping_layer",
    description="Scrapes cash flow data from Bursa companies",
    deps=[company_urls],
    required_resource_keys={"s3"},
)
def cash_flow_data(context: AssetExecutionContext) -> str:
    """Scrape cash flow data for all companies and upload to S3."""
    context.log.info("Starting cash flow scraping...")

    # Path to the cash flow scraper
    scraper_path = (
        Path(__file__).parent.parent.parent / "scraping_layer" / "cash_flow.py"
    )

    try:
        # Run the cash flow scraper with real-time output
        context.log.info("Running cash flow scraper with real-time output...")
        subprocess.run(
            [sys.executable, str(scraper_path)],
            capture_output=False,  # Don't capture output, let it display in real-time
            text=True,
            check=True,
        )
        context.log.info("Cash flow scraping completed successfully")

        # Read the combined output file
        output_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_cash_flow_statements.csv"
        )
        cash_flow_df = pd.read_csv(output_path)

        # Upload to S3
        s3_path = "s3://onecredit-snowflake-data/data/bursa/cash_flow/complete_cash_flow_statements.csv"
        context.log.info("📤 Uploading to S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            cash_flow_df.to_csv(f, index=False)

        context.log.info("✅ Successfully uploaded to S3")

        # Add metadata
        context.add_output_metadata(
            {
                "num_cash_flow_records": MetadataValue.int(len(cash_flow_df)),
                "columns": MetadataValue.json(list(cash_flow_df.columns)),
                "unique_companies": MetadataValue.int(
                    int(cash_flow_df["company_id"].nunique())
                ),
                "s3_path": MetadataValue.text(s3_path),
            }
        )
    except subprocess.CalledProcessError:
        context.log.exception("Cash flow scraping failed")
        raise
    except Exception:
        context.log.exception("Unexpected error in cash flow scraping")
        raise
    return s3_path


@asset(
    group_name="scraping_layer",
    description="Scrapes profile and management data from Bursa companies",
    deps=[company_urls],
    required_resource_keys={"s3"},
)
def profile_data(context: AssetExecutionContext) -> dict[str, str]:
    """Scrape profile and management data for all companies and upload to S3."""
    context.log.info("Starting profile data scraping...")

    # Path to the profile scraper
    scraper_path = (
        Path(__file__).parent.parent.parent / "scraping_layer" / "profile_scraper.py"
    )

    try:
        # Run the profile scraper with real-time output
        context.log.info("Running profile scraper with real-time output...")
        subprocess.run(
            [sys.executable, str(scraper_path)],
            capture_output=False,  # Don't capture output, let it display in real-time
            text=True,
            check=True,
        )
        context.log.info("Profile data scraping completed successfully")

        # Read the output files for each profile type and upload to S3
        base_path = (
            Path(__file__).parent.parent.parent / "scraping_layer" / "combined_csv"
        )
        profile_data = {}
        s3_paths = {}

        for data_type in ["profile", "management", "ownership", "top10", "insider"]:
            filename = f"combined_{data_type}.csv"
            file_path = base_path / filename

            if file_path.exists():
                df = pd.read_csv(file_path)

                # Upload to S3
                s3_path = f"s3://onecredit-snowflake-data/data/bursa/profile/{filename}"
                context.log.info("📤 Uploading %s to S3: %s", data_type, s3_path)

                s3 = context.resources.s3
                with s3.open(s3_path, "wb") as f:
                    df.to_csv(f, index=False)

                profile_data[data_type] = df
                s3_paths[data_type] = s3_path
                context.log.info("✅ Successfully uploaded %s to S3", data_type)
            else:
                context.log.warning("⚠️ File not found: %s", file_path)

        # Add metadata
        context.add_output_metadata(
            {
                "profile_types": MetadataValue.json(list(profile_data.keys())),
                "s3_paths": MetadataValue.json(s3_paths),
                "total_records": MetadataValue.int(
                    sum(len(df) for df in profile_data.values())
                ),
            }
        )

    except subprocess.CalledProcessError:
        context.log.exception("Profile data scraping failed")
        raise
    except Exception:
        context.log.exception("Unexpected error in profile data scraping")
        raise

    return s3_paths


@asset(
    group_name="scraping_layer",
    description="Scrapes market cap and volume data from Bursa companies",
    deps=[company_urls],
    required_resource_keys={"s3"},
)
def market_data(context: AssetExecutionContext) -> str:
    """Scrape market cap and volume data for all companies and upload to S3."""
    context.log.info("Starting market data scraping...")

    # Path to the market data scraper
    scraper_path = (
        Path(__file__).parent.parent.parent / "scraping_layer" / "market_capscrape.py"
    )

    try:
        # Run the market data scraper with real-time output
        context.log.info("Running market data scraper with real-time output...")
        subprocess.run(
            [sys.executable, str(scraper_path)],
            capture_output=False,  # Don't capture output, let it display in real-time
            text=True,
            check=True,
        )
        context.log.info("Market data scraping completed successfully")

        # Read the combined output file
        output_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "market_info_sample.csv"
        )
        market_data_df = pd.read_csv(output_path)

        # Upload to S3
        s3_path = "s3://onecredit-snowflake-data/data/bursa/market_data/market_info_sample.csv"
        context.log.info("📤 Uploading to S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            market_data_df.to_csv(f, index=False)

        context.log.info("✅ Successfully uploaded to S3")

        # Add metadata
        context.add_output_metadata(
            {
                "num_market_data_records": MetadataValue.int(len(market_data_df)),
                "columns": MetadataValue.json(list(market_data_df.columns)),
                "unique_companies": MetadataValue.int(
                    int(market_data_df["company_id"].nunique())
                ),
                "s3_path": MetadataValue.text(s3_path),
            }
        )
    except subprocess.CalledProcessError:
        context.log.exception("Market data scraping failed")
        raise
    except Exception:
        context.log.exception("Unexpected error in market data scraping")
        raise
    return s3_path


@asset(
    group_name="scraping_layer",
    description="Matches companies with SSM registration data",
    deps=["bursa_company_list"],
    required_resource_keys={"s3"},
)
def ssm_matched_companies(context: AssetExecutionContext) -> str:
    """Match companies with SSM registration data and upload to S3."""
    context.log.info("Starting SSM company matching...")

    # Path to the SSM API script
    ssm_script_path = (
        Path(__file__).parent.parent.parent / "scraping_layer" / "ssm_api_script.py"
    )

    try:
        # Run the SSM API script with real-time output
        context.log.info("Running SSM API script with real-time output...")
        subprocess.run(
            [sys.executable, str(ssm_script_path)],
            capture_output=False,  # Don't capture output, let it display in real-time
            text=True,
            check=True,
        )
        context.log.info("SSM company matching completed successfully")

        # Read the output file
        output_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "matched_companies_from_ssm.csv"
        )
        ssm_matched_df = pd.read_csv(output_path)

        # Upload to S3
        s3_path = "s3://onecredit-snowflake-data/data/bursa/ssm_matched/matched_companies_from_ssm.csv"
        context.log.info("📤 Uploading to S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            ssm_matched_df.to_csv(f, index=False)

        context.log.info("✅ Successfully uploaded to S3")

        # Add metadata
        context.add_output_metadata(
            {
                "num_matched_companies": MetadataValue.int(len(ssm_matched_df)),
                "columns": MetadataValue.json(list(ssm_matched_df.columns)),
                "unique_companies": MetadataValue.int(
                    int(ssm_matched_df["company_id"].nunique())
                ),
                "s3_path": MetadataValue.text(s3_path),
            }
        )
    except subprocess.CalledProcessError:
        context.log.exception("SSM company matching failed")
        raise
    except Exception:
        context.log.exception("Unexpected error in SSM company matching")
        raise
    return s3_path


# Asset group for scraping layer
scraping_layer_assets = [
    company_urls,
    balance_sheet_data,
    income_statement_data,
    cash_flow_data,
    profile_data,
    market_data,
    ssm_matched_companies,
]
