"""Dagster definitions for Bursa dataset pipeline.
Organizes assets into logical groups based on the data pipeline layers.
"""

from dagster import Definitions, define_asset_job, fs_io_manager

from .assets.list_of_ids_assets import list_of_ids_assets
from .assets.scraping_layer_assets import scraping_layer_assets
from .assets.sql_inject_assets import sql_inject_assets

# Define asset groups
list_of_ids_group = list_of_ids_assets
scraping_layer_group = scraping_layer_assets
sql_inject_group = sql_inject_assets

# Define jobs for each layer
list_of_ids_job = define_asset_job(
    name="list_of_ids_job",
    selection=["bursa_company_list"],
    description="Job to scrape and process company IDs from Bursa",
)

scraping_layer_job = define_asset_job(
    name="scraping_layer_job",
    selection=[
        "company_urls",
        "balance_sheet_data",
        "income_statement_data",
        "cash_flow_data",
        "profile_data",
        "market_data",
        "ssm_matched_companies",
    ],
    description="Job to scrape financial data from Bursa companies",
)

sql_inject_job = define_asset_job(
    name="sql_inject_job",
    selection=[
        "balance_sheet_sql_injection",
        "income_statement_sql_injection",
        "cash_flow_sql_injection",
        "profile_sql_injection",
    ],
    description="Job to inject scraped data into SQL database",
)

# New job to recreate profile tables
recreate_profile_tables_job = define_asset_job(
    name="recreate_profile_tables_job",
    selection=["market_data", "profile_sql_injection"],
    config={
        "ops": {
            "profile_sql_injection": {
                "config": {
                    "replace_table": True,
                }
            }
        }
    },
    description="One-time job to recreate the profile-related tables with the correct schema.",
)

# Full pipeline job
full_pipeline_job = define_asset_job(
    name="full_bursa_pipeline",
    selection=[
        "bursa_company_list",
        "company_urls",
        "balance_sheet_data",
        "income_statement_data",
        "cash_flow_data",
        "profile_data",
        "market_data",
        "ssm_matched_companies",
        "balance_sheet_sql_injection",
        "income_statement_sql_injection",
        "cash_flow_sql_injection",
        "profile_sql_injection",
    ],
    description="Complete Bursa data pipeline from scraping to database injection",
)

# Main definitions - include ALL assets and jobs
defs = Definitions(
    assets=[
        *list_of_ids_assets,
        *scraping_layer_assets,
        *sql_inject_assets,
    ],
    jobs=[
        list_of_ids_job,
        scraping_layer_job,
        sql_inject_job,
        recreate_profile_tables_job,
        full_pipeline_job,
    ],
    resources={
        "file_io_manager": fs_io_manager,
    },
)
