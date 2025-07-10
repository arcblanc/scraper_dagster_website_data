from dagster import Definitions, ScheduleDefinition, define_asset_job

from ctos_scrapers.ctos_scraper.dagster_assets.orchestrator_assets import (
    resume_all_urls_from_last,
    retry_all_failed_urls,
)
from ctos_scrapers.ctos_scraper.dagster_assets.rob_assets import (
    scrape_registration as rob_scrape_registration,
)

# Import only essential assets
from ctos_scrapers.ctos_scraper.dagster_assets.rob_assets import (
    scrape_url as rob_scrape_url,
)
from ctos_scrapers.ctos_scraper.dagster_assets.roc_assets import (
    scrape_registration as roc_scrape_registration,
)
from ctos_scrapers.ctos_scraper.dagster_assets.roc_assets import (
    scrape_url as roc_scrape_url,
)

# Define basic scraping jobs
rob_job = define_asset_job(
    name="rob_scraping_job", selection=["scrape_url", "scrape_registration"]
)
roc_job = define_asset_job(
    name="roc_scraping_job", selection=["scrape_url", "scrape_registration"]
)

# Define retry/resume jobs
retry_failed_job = define_asset_job(
    name="retry_failed_job", selection=["retry_all_failed_urls"]
)
resume_from_last_job = define_asset_job(
    name="resume_from_last_job", selection=["resume_all_urls_from_last"]
)

# Optional: define schedules
rob_schedule = ScheduleDefinition(
    job=rob_job, cron_schedule="0 7 * * 5"
)  # every Friday 7 AM
roc_schedule = ScheduleDefinition(job=roc_job, cron_schedule="0 7 * * 5")  # same time

defs = Definitions(
    # Assets = only essential scraping and retry/resume assets
    assets=[
        # Main scraping assets
        rob_scrape_url,
        rob_scrape_registration,
        roc_scrape_url,
        roc_scrape_registration,
        # Retry/resume assets
        retry_all_failed_urls,
        resume_all_urls_from_last,
    ],
    # Jobs = simplified job definitions
    jobs=[
        # Basic scraping jobs
        rob_job,
        roc_job,
        # Retry/resume jobs
        retry_failed_job,
        resume_from_last_job,
    ],
    # schedules = cron_schedule every day at what o'clock
    schedules=[rob_schedule, roc_schedule],
)

# PYTHONPATH=$(pwd) dagster-webserver -m ctos_scrapers.ctos_scraper.dagster_assets.definitions
# For devcontainer: PYTHONPATH=$(pwd) dagster-webserver -m ctos_scrapers.ctos_scraper.dagster_assets.definitions
