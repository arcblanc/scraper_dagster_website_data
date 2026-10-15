# Unified Dagster Definitions for OneCredit Data Pipeline
# Combines oc_etl and ctos_scrapers assets in one location

import os

from dagster import Definitions, ScheduleDefinition, define_asset_job

# Import Bursa Malaysia (bursa_dataset_and_cleaning) assets and jobs
from bursa_dataset_and_cleaning.dagster_assets.definitions import (
    full_pipeline_job,
    list_of_ids_assets,
    list_of_ids_job,
    recreate_profile_tables_job,
    scraping_layer_assets,
    scraping_layer_job,
    sql_inject_assets,
    sql_inject_job,
)

# Import CTOS scraper assets
from ctos_scrapers.ctos_scraper.dagster_assets.orchestrator_assets import (
    orchestrate_details_scraping,
    orchestrate_url_scraping,
    retry_all_failed_urls,
)
from ctos_scrapers.ctos_scraper.dagster_assets.rob_assets import (
    scrape_registration as rob_scrape_registration,
)
from ctos_scrapers.ctos_scraper.dagster_assets.rob_assets import (
    scrape_url as rob_scrape_url,
)
from ctos_scrapers.ctos_scraper.dagster_assets.roc_assets import (
    scrape_registration as roc_scrape_registration,
)
from ctos_scrapers.ctos_scraper.dagster_assets.roc_assets import (
    scrape_url as roc_scrape_url,
)

# Import existing oc_etl assets and resources
from src.oc_etl import (
    cleaned_parquet_df,
    company_raw_assets,
    company_sanitize_assets,
    company_snow_assets,
    parsed_from_html,
)
from src.oc_etl.io_managers import s3_parquet_io_manager
from src.oc_etl.jobs.company_etl_job import company_etl_job
from src.oc_etl.resources import make_pg_resource, make_s3_resource, snowflake_resource
from src.oc_etl.schedules import daily_company_etl_schedule

# Configuration for existing oc_etl resources
snowflake_config = {
    "account": os.getenv("SNOW_ACCOUNT"),
    "user": os.getenv("SNOW_USER"),
    "password": os.getenv("SNOW_PWD"),
    "database": os.getenv("SNOW_DATABASE"),
    "warehouse": os.getenv("SNOW_WAREHOUSE"),
    "schema": os.getenv("SNOW_SCHEMA"),
}

s3_io_config = {
    "s3_bucket": os.getenv("S3_BUCKET"),
    "base_path": "data/",
}

s3_litigation_io_config = {
    "s3_bucket": "oc-litigation-data",  # hardcoded
    "base_path": "data/",
}

# Define CTOS-specific jobs
ctos_rob_job = define_asset_job(
    name="ctos_rob_scraping_job",
    selection=["rob/scrape_url", "rob/scrape_registration"],
)
ctos_roc_job = define_asset_job(
    name="ctos_roc_scraping_job",
    selection=["roc/scrape_url", "roc/scrape_registration"],
)
ctos_retry_job = define_asset_job(
    name="ctos_retry_failed_job",
    selection=["retry_all_failed_urls"],
)

# Define unified jobs that combine both systems
unified_company_job = define_asset_job(
    name="unified_company_etl_job",
    selection=[
        # Existing company ETL assets
        "raw/company/pg_recc/public/raw_company_profiles",
        "sanitized/company/pg_recc/public/sanitized_company_profiles",
        "upload_to_snowflake/company/pg_recc/public/upload_to_snow_company_profiles",
        # CTOS scraping assets
        "rob/scrape_url",
        "rob/scrape_registration",
        "roc/scrape_url",
        "roc/scrape_registration",
    ],
)

# Define schedules
ctos_rob_schedule = ScheduleDefinition(
    job=ctos_rob_job,
    cron_schedule="0 7 * * 1",  # Every Monday 7 AM
    description="Weekly ROB scraping schedule",
)
ctos_roc_schedule = ScheduleDefinition(
    job=ctos_roc_job,
    cron_schedule="0 7 * * 4",  # Every Thursday 7 AM
    description="Weekly ROC scraping schedule",
)

# Define Bursa quarterly schedule for financial data collection
bursa_quarterly_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 8 31 3,6,9,12 *",  # 8 AM on 31st of March, June, September, December
    description="Quarterly Bursa financial data collection schedule",
)


# Collect all assets from both systems
def get_all_assets() -> list:
    """Collect all assets from both oc_etl and ctos_scrapers."""
    # Existing oc_etl assets
    oc_etl_assets = [
        asset
        for asset in [
            *company_raw_assets,
            *company_sanitize_assets,
            *company_snow_assets,
            parsed_from_html,
            cleaned_parquet_df,
        ]
        if asset is not None and hasattr(asset, "keys")
    ]

    # CTOS scraper assets
    ctos_assets = [
        # Main scraping assets
        rob_scrape_url,
        rob_scrape_registration,
        roc_scrape_url,
        roc_scrape_registration,
        # Orchestration assets
        retry_all_failed_urls,
        orchestrate_url_scraping,
        orchestrate_details_scraping,
    ]

    return (
        oc_etl_assets
        + ctos_assets
        + list_of_ids_assets
        + scraping_layer_assets
        + sql_inject_assets
    )


# Only one Definitions object at module scope!
defs = Definitions(
    assets=get_all_assets(),
    jobs=[
        # Existing jobs
        company_etl_job,
        # CTOS-specific jobs
        ctos_rob_job,
        ctos_roc_job,
        ctos_retry_job,
        # Unified jobs
        unified_company_job,
        # Bursa jobs
        list_of_ids_job,
        scraping_layer_job,
        sql_inject_job,
        recreate_profile_tables_job,
        full_pipeline_job,
    ],
    schedules=[
        # Existing schedules
        daily_company_etl_schedule,
        # CTOS schedules
        ctos_rob_schedule,
        ctos_roc_schedule,
        # Bursa schedules
        bursa_quarterly_schedule,
    ],
    resources={
        # Existing resources
        "pg_recc": make_pg_resource("PG_RECC_DSN"),
        "pg_portal": make_pg_resource("PG_PORTAL_DSN"),
        "s3_io_manager": s3_parquet_io_manager.configured(s3_io_config),
        "s3_litigation_io_manager": s3_parquet_io_manager.configured(
            s3_litigation_io_config
        ),
        "s3": make_s3_resource(),
        "snowflake_resource": snowflake_resource.configured(snowflake_config),
        # Bursa-specific resource
    },
)
