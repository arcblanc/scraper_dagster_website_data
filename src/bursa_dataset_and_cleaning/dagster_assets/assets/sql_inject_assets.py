"""Dagster assets for the sql_inject layer.
Handles database injection of scraped data.
"""

import subprocess
import sys
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, Field, MetadataValue, asset


def run_script_with_logging(
    context: AssetExecutionContext,
    script_path: Path,
    script_name: str,
    replace_table: bool = False,
) -> subprocess.CompletedProcess:
    """Run a script with real-time logging and optional --replace flag."""
    context.log.info("Running %s...", script_name)
    cmd = [sys.executable, str(script_path)]
    if replace_table:
        cmd.append("--replace")
        context.log.warning(
            "⚠️ Running with --replace flag. Table will be dropped and recreated."
        )

    # Use Popen for real-time output streaming
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        bufsize=1,  # Line-buffered
    )

    # Stream stdout
    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            context.log.info(line.strip())
        process.stdout.close()

    # Stream stderr
    if process.stderr:
        for line in iter(process.stderr.readline, ""):
            context.log.warning(line.strip())
        process.stderr.close()

    # Wait for the process to complete
    return_code = process.wait()

    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, process.args)

    context.log.info("%s completed successfully", script_name)
    # Recreate a CompletedProcess object for compatibility, though stdout/stderr are already logged
    return subprocess.CompletedProcess(process.args, return_code, stdout="", stderr="")


@asset(
    group_name="sql_inject",
    description="Injects balance sheet data into SQL database",
    deps=["balance_sheet_data"],
    required_resource_keys={"s3"},
    config_schema={
        "replace_table": Field(
            bool,
            default_value=False,
            description="If True, the destination table will be dropped and recreated.",
        )
    },
)
def balance_sheet_sql_injection(context: AssetExecutionContext) -> None:
    """Inject balance sheet data into SQL database."""
    context.log.info("Starting balance sheet SQL injection...")
    replace_table = context.op_config["replace_table"]
    injection_path = (
        Path(__file__).parent.parent.parent / "sql_inject" / "balance_inject.py"
    )

    try:
        # Download from S3 first
        s3_path = "s3://onecredit-snowflake-data/data/bursa/balance_sheets/complete_balance_sheets.csv"
        context.log.info("📥 Downloading from S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "rb") as f:
            df = pd.read_csv(f)

        context.log.info("✅ Successfully downloaded from S3: %d records", len(df))

        # Save to local temp file for the injection script
        temp_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_balance_sheets.csv"
        )
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(temp_path, index=False)

        run_script_with_logging(
            context,
            injection_path,
            "balance_sheet_sql_injection",
            replace_table=replace_table,
        )

        context.add_output_metadata(
            {
                "num_records": MetadataValue.int(len(df)),
                "columns": MetadataValue.json(list(df.columns)),
                "s3_source": MetadataValue.text(s3_path),
            }
        )
    except Exception:
        context.log.exception("Balance sheet SQL injection failed")
        raise


@asset(
    group_name="sql_inject",
    description="Injects income statement data into SQL database",
    deps=["income_statement_data"],
    required_resource_keys={"s3"},
    config_schema={
        "replace_table": Field(
            bool,
            default_value=False,
            description="If True, the destination table will be dropped and recreated.",
        )
    },
)
def income_statement_sql_injection(context: AssetExecutionContext) -> None:
    """Inject income statement data into SQL database."""
    context.log.info("Starting income statement SQL injection...")
    replace_table = context.op_config["replace_table"]
    injection_path = (
        Path(__file__).parent.parent.parent / "sql_inject" / "income_injection.py"
    )

    try:
        # Download from S3 first
        s3_path = "s3://onecredit-snowflake-data/data/bursa/income_statements/complete_income_statements.csv"
        context.log.info("📥 Downloading from S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "rb") as f:
            df = pd.read_csv(f, low_memory=False)

        context.log.info("✅ Successfully downloaded from S3: %d records", len(df))

        # Save to local temp file for the injection script
        temp_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_income_statements.csv"
        )
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(temp_path, index=False)

        run_script_with_logging(
            context,
            injection_path,
            "income_statement_sql_injection",
            replace_table=replace_table,
        )

        context.add_output_metadata(
            {
                "num_records": MetadataValue.int(len(df)),
                "table_name": MetadataValue.text("public_complete_income"),
                "s3_source": MetadataValue.text(s3_path),
            }
        )

    except Exception:
        context.log.exception("Income statement SQL injection failed")
        raise


@asset(
    group_name="sql_inject",
    description="Injects cash flow data into SQL database",
    deps=["cash_flow_data"],
    required_resource_keys={"s3"},
    config_schema={
        "replace_table": Field(
            bool,
            default_value=False,
            description="If True, the destination table will be dropped and recreated.",
        )
    },
)
def cash_flow_sql_injection(context: AssetExecutionContext) -> None:
    """Inject cash flow data into SQL database."""
    context.log.info("Starting cash flow SQL injection...")
    replace_table = context.op_config["replace_table"]
    injection_path = (
        Path(__file__).parent.parent.parent / "sql_inject" / "cashflow_injection.py"
    )

    try:
        # Download from S3 first
        s3_path = "s3://onecredit-snowflake-data/data/bursa/cash_flow/complete_cash_flow_statements.csv"
        context.log.info("📥 Downloading from S3: %s", s3_path)

        s3 = context.resources.s3
        with s3.open(s3_path, "rb") as f:
            df = pd.read_csv(f)

        context.log.info("✅ Successfully downloaded from S3: %d records", len(df))

        # Save to local temp file for the injection script
        temp_path = (
            Path(__file__).parent.parent.parent
            / "scraping_layer"
            / "combined_csv"
            / "complete_cash_flow_statements.csv"
        )
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(temp_path, index=False)

        run_script_with_logging(
            context,
            injection_path,
            "cash_flow_sql_injection",
            replace_table=replace_table,
        )

        context.add_output_metadata(
            {
                "num_records": MetadataValue.int(len(df)),
                "table_name": MetadataValue.text("public_complete_cash_flow"),
                "s3_source": MetadataValue.text(s3_path),
            }
        )
    except Exception:
        context.log.exception("Cash flow SQL injection failed")
        raise


@asset(
    group_name="sql_inject",
    description="Injects profile data into SQL database",
    deps=["profile_data", "market_data"],
    required_resource_keys={"s3"},
    config_schema={
        "replace_table": Field(
            bool,
            default_value=False,
            description="If True, the destination table will be dropped and recreated.",
        )
    },
)
def profile_sql_injection(context: AssetExecutionContext) -> None:
    """Inject profile data into SQL database."""
    context.log.info("Starting profile SQL injection...")
    replace_table = context.op_config["replace_table"]

    profile_scripts = {
        "profile": "profile_injection.py",
        "management": "management_injection.py",
        "ownership": "ownership_injection.py",
        "top10": "top_10_injection.py",
        "insider": "insider_injection.py",
    }

    table_name_map = {
        "profile": "public_complete_company_profile",
        "management": "public_complete_directors_executives",
        "ownership": "public_complete_ownership_type",
        "top10": "public_complete_top_10_investors",
        "insider": "public_complete_individual_shareholder",
    }

    total_records = 0
    processed_types = []

    for profile_type, script_name in profile_scripts.items():
        try:
            context.log.info("Processing %s data...", profile_type)

            # Download from S3
            s3_path = f"s3://onecredit-snowflake-data/data/bursa/profile/combined_{profile_type}.csv"
            context.log.info("📥 Downloading from S3: %s", s3_path)

            s3 = context.resources.s3
            with s3.open(s3_path, "rb") as f:
                df = pd.read_csv(f)

            context.log.info("✅ Successfully downloaded from S3: %d records", len(df))
            total_records += len(df)

            # Save to local temp file for the injection script
            temp_path = (
                Path(__file__).parent.parent.parent
                / "scraping_layer"
                / "combined_csv"
                / f"combined_{profile_type}.csv"
            )
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(temp_path, index=False)

            # Run the injection script
            injection_path = (
                Path(__file__).parent.parent.parent / "sql_inject" / script_name
            )
            run_script_with_logging(
                context,
                injection_path,
                f"{profile_type}_sql_injection",
                replace_table=replace_table,
            )

            processed_types.append(profile_type)
            context.log.info("✅ Successfully processed %s data", profile_type)

        except Exception:
            context.log.exception("Failed to process %s data", profile_type)
            raise

    context.add_output_metadata(
        {
            "processed_types": MetadataValue.json(processed_types),
            "total_records": MetadataValue.int(total_records),
            "table_names": MetadataValue.json(
                [table_name_map[t] for t in processed_types]
            ),
        }
    )


sql_inject_assets = [
    balance_sheet_sql_injection,
    income_statement_sql_injection,
    cash_flow_sql_injection,
    profile_sql_injection,
]
