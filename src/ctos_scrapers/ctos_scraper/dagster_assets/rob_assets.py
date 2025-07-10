import asyncio

from dagster import Config, asset

from ctos_scrapers.ctos_scraper.assets.scrape_rob import LocalROBScraper
from ctos_scrapers.ctos_scraper.assets.scrape_rob_url import (
    run_parallel_scraper as rob_url_scraper,
)
from ctos_scrapers.ctos_scraper.assets.tracking import URLScrapeTracker
from ctos_scrapers.ctos_scraper.config.logging_setup import logger


class ROBURLScraperConfig(Config):
    """Configuration for ROB URL scraper."""

    retry_failed: bool = False
    resume_from: str | None = None
    resume_from_last: bool = False


@asset(
    name="scrape_url",
    key_prefix=["rob"],
    description=(
        "Scrape ROB company listing URLs (ROB) into rob_url_master table. "
        "This is the entry point for scraping all ROB URLs."
    ),
)
def scrape_url(config: ROBURLScraperConfig) -> None:
    """Scrape ROB company listing URLs into rob_url_master table.

    Args:
        config: Configuration for the scraper
            - retry_failed: If True, only retry failed pages
            - resume_from: Resume from specific character (e.g., 'm', '5')
            - resume_from_last: If True, resume from last successful point

    """
    logger.info("🚀 Starting ROB URL scraper with config: %s", config)

    # Determine resume point
    resume_from = None
    if config.resume_from_last:
        try:
            # Catching Exception here is intentional to log any error in resume point fetching
            tracker = URLScrapeTracker("rob_tracking")
            resume_from = tracker.get_resume_point()
            logger.info("📍 Resuming from last point: %s", resume_from)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not fetch resume point: %s", str(exc))
    elif config.resume_from:
        resume_from = config.resume_from
        logger.info("📍 Resuming from specified point: %s", resume_from)

    # Run the scraper with configuration
    asyncio.run(
        rob_url_scraper(
            resume_from=resume_from,
            retry_failed=config.retry_failed,
        ),
    )


@asset(
    name="scrape_registration",
    key_prefix=["rob"],
    deps=[scrape_url],
    description=(
        "Scrape ROB company registration details and upload to PostgreSQL. "
        "This is the main asset for scraping all ROB registration details."
    ),
)
def scrape_registration() -> None:
    """Scrape ROB company details and upload to PostgreSQL."""
    # Ensure tables exist before running scraper
    from ctos_scrapers.ctos_scraper.assets import postgres_utils

    postgres_utils.ensure_url_master_table("rob_url_master")
    postgres_utils.ensure_registration_table("rob_registration")

    asyncio.run(LocalROBScraper().run())
