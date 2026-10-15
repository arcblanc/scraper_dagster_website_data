from dagster import define_asset_job

from src.oc_etl.assets.company import (
    company_raw_assets,
    company_sanitize_assets,
    company_snow_assets,
)

company_etl_job = define_asset_job(
    name="company_etl_job",
    selection=[*company_raw_assets, *company_sanitize_assets, *company_snow_assets],
    description="Extract company profiles, drop duplicates, load the parquet.",
)
