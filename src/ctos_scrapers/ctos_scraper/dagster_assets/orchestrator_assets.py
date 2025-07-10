import asyncio
from typing import Literal

from dagster import Config, asset

from ctos_scrapers.ctos_scraper.assets.scrape_rob import LocalROBScraper
from ctos_scrapers.ctos_scraper.assets.scrape_rob_url import (
    run_parallel_scraper as rob_url_scraper,
)
from ctos_scrapers.ctos_scraper.assets.scrape_roc import LocalROCScraper
from ctos_scrapers.ctos_scraper.assets.scrape_roc_url import (
    run_parallel_scraper as roc_url_scraper,
)
from ctos_scrapers.ctos_scraper.assets.tracking import URLScrapeTracker
from ctos_scrapers.ctos_scraper.config.logging_setup import logger


class ScraperConfig(Config):
    """Unified configuration for both ROB and ROC scrapers."""

    scraper_type: Literal["rob", "roc", "both"] = "both"
    retry_failed: bool = False
    resume_from: str | None = None
    resume_from_last: bool = False
    include_details: bool = True


class URLScraperConfig(Config):
    """Configuration for URL scraping only."""

    scraper_type: Literal["rob", "roc", "both"] = "both"
    retry_failed: bool = False
    resume_from: str | None = None
    resume_from_last: bool = False


class DetailsScraperConfig(Config):
    """Configuration for details scraping only."""

    scraper_type: Literal["rob", "roc", "both"] = "both"


@asset
def orchestrate_url_scraping(config: URLScraperConfig) -> None:
    """Orchestrate URL scraping for ROB and/or ROC with advanced configuration.

    Args:
        config: Configuration for the scraper
            - scraper_type: "rob", "roc", or "both"
            - retry_failed: If True, only retry failed pages
            - resume_from: Resume from specific character (e.g., 'm', '5')
            - resume_from_last: If True, resume from last successful point

    """
    logger.info("🎯 Starting orchestrated URL scraping with config: %s", config)
    # Determine resume point
    resume_from = None
    if config.resume_from_last:
        try:
            # Catching Exception here is intentional to log any error in resume point fetching
            if config.scraper_type in ["rob", "both"]:
                tracker = URLScrapeTracker("rob_tracking")
                resume_from = tracker.get_resume_point()
                logger.info("📍 ROB resuming from last point: %s", resume_from)
            if config.scraper_type in ["roc", "both"]:
                tracker = URLScrapeTracker("roc_tracking")
                resume_from = tracker.get_resume_point()
                logger.info("📍 ROC resuming from last point: %s", resume_from)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not fetch resume point: %s", str(exc))
    elif config.resume_from:
        resume_from = config.resume_from
        logger.info("📍 Resuming from specified point: %s", resume_from)
    # Run scrapers based on configuration
    if config.scraper_type in ["rob", "both"]:
        logger.info("🚀 Starting ROB URL scraper...")
        asyncio.run(
            rob_url_scraper(
                resume_from=resume_from,
                retry_failed=config.retry_failed,
            ),
        )
    if config.scraper_type in ["roc", "both"]:
        logger.info("🚀 Starting ROC URL scraper...")
        asyncio.run(
            roc_url_scraper(
                resume_from=resume_from,
                retry_failed=config.retry_failed,
            ),
        )


@asset
def orchestrate_details_scraping(config: DetailsScraperConfig) -> None:
    """Orchestrate details scraping for ROB and/or ROC.

    Args:
        config: Configuration for the scraper
            - scraper_type: "rob", "roc", or "both"

    """
    logger.info("🎯 Starting orchestrated details scraping with config: %s", config)
    if config.scraper_type in ["rob", "both"]:
        logger.info("🚀 Starting ROB details scraper...")
        asyncio.run(LocalROBScraper().run())
    if config.scraper_type in ["roc", "both"]:
        logger.info("🚀 Starting ROC details scraper...")
        asyncio.run(LocalROCScraper().run())


def get_resume_point_for_config(config: ScraperConfig) -> str | None:
    """Determine the resume point based on config."""
    resume_from = None
    if config.resume_from_last:
        try:
            if config.scraper_type in ["rob", "both"]:
                tracker = URLScrapeTracker("rob_tracking")
                resume_from = tracker.get_resume_point()
                logger.info("📍 ROB resuming from last point: %s", resume_from)
            if config.scraper_type in ["roc", "both"]:
                tracker = URLScrapeTracker("roc_tracking")
                resume_from = tracker.get_resume_point()
                logger.info("📍 ROC resuming from last point: %s", resume_from)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not fetch resume point: %s", str(exc))
    elif config.resume_from:
        resume_from = config.resume_from
        logger.info("📍 Resuming from specified point: %s", resume_from)
    return resume_from


def run_url_scraping(config: ScraperConfig, resume_from: str | None) -> None:
    """Run URL scraping for ROB and/or ROC."""
    if config.scraper_type in ["rob", "both"]:
        logger.info("📋 Step 1: ROB URL scraping...")
        asyncio.run(
            rob_url_scraper(
                resume_from=resume_from,
                retry_failed=config.retry_failed,
            ),
        )
    if config.scraper_type in ["roc", "both"]:
        logger.info("📋 Step 1: ROC URL scraping...")
        asyncio.run(
            roc_url_scraper(
                resume_from=resume_from,
                retry_failed=config.retry_failed,
            ),
        )


def run_details_scraping(config: ScraperConfig) -> None:
    """Run details scraping for ROB and/or ROC."""
    if config.scraper_type in ["rob", "both"]:
        logger.info("📋 Step 2: ROB details scraping...")
        asyncio.run(LocalROBScraper().run())
    if config.scraper_type in ["roc", "both"]:
        logger.info("📋 Step 2: ROC details scraping...")
        asyncio.run(LocalROCScraper().run())


@asset
def full_scraping_pipeline(config: ScraperConfig) -> dict:
    """Complete scraping pipeline with configurable options.
    This asset provides a unified interface for running the entire scraping pipeline
    with advanced configuration options for retry, resume, and selective execution.

    Args:
        config: Configuration for the scraper
            - scraper_type: "rob", "roc", or "both"
            - retry_failed: If True, only retry failed pages
            - resume_from: Resume from specific character
            - resume_from_last: If True, resume from last successful point
            - include_details: If True, also scrape details after URLs

    """
    logger.info("🎯 Starting full scraping pipeline with config: %s", config)
    resume_from = get_resume_point_for_config(config)
    run_url_scraping(config, resume_from)
    if config.include_details:
        run_details_scraping(config)
    return {
        "rob_urls": "completed"
        if config.scraper_type in ["rob", "both"]
        else "skipped",
        "roc_urls": "completed"
        if config.scraper_type in ["roc", "both"]
        else "skipped",
        "rob_details": "completed"
        if config.include_details and config.scraper_type in ["rob", "both"]
        else "skipped",
        "roc_details": "completed"
        if config.include_details and config.scraper_type in ["roc", "both"]
        else "skipped",
    }


@asset
def retry_all_failed_urls() -> None:
    """Retry all failed URL pages for both ROB and ROC with parallel processing and enhanced logging."""
    logger.info("🔁 Starting parallel retry of all failed URL pages...")
    # Retry ROB failed pages with enhanced logging
    logger.info("Step 1: Retrying ROB failed pages with parallel processing...")
    asyncio.run(rob_url_scraper(retry_failed=True))
    # Retry ROC failed pages with enhanced logging
    logger.info("Step 2: Retrying ROC failed pages with parallel processing...")
    asyncio.run(roc_url_scraper(retry_failed=True))
    logger.info("✅ Parallel retry of all failed URLs completed")


@asset
def resume_all_urls_from_last() -> None:
    """Resume URL scraping from last successful point for both ROB and ROC."""
    logger.info("🔄 Resuming all URL scraping from last point...")
    # Resume ROB from last point
    logger.info("Step 1: Resuming ROB from last point...")
    asyncio.run(rob_url_scraper(resume_from=None, retry_failed=False))
    # Resume ROC from last point
    logger.info("Step 2: Resuming ROC from last point...")
    asyncio.run(roc_url_scraper(resume_from=None, retry_failed=False))


@asset
def retry_and_resume_all_urls() -> None:
    """Retry failed URL pages and then resume from last point for both scrapers with parallel processing."""
    logger.info(
        "🔄 Starting comprehensive retry and resume operation for all scrapers..."
    )
    # Step 1: Retry failed pages for both scrapers in parallel
    logger.info("Phase 1: Retrying failed pages for both scrapers...")
    # ROB: Retry failed pages with parallel processing
    logger.info("Step 1.1: ROB - Retrying failed pages with parallel processing...")
    asyncio.run(rob_url_scraper(retry_failed=True))
    # ROC: Retry failed pages with parallel processing
    logger.info("Step 1.2: ROC - Retrying failed pages with parallel processing...")
    asyncio.run(roc_url_scraper(retry_failed=True))
    logger.info("✅ Phase 1 completed: All failed pages retried")
    # Step 2: Resume from last point for both scrapers
    logger.info("Phase 2: Resuming from last point for both scrapers...")
    # ROB: Resume from last point
    logger.info("Step 2.1: ROB - Resuming from last point...")
    asyncio.run(rob_url_scraper(resume_from=None, retry_failed=False))
    # ROC: Resume from last point
    logger.info("Step 2.2: ROC - Resuming from last point...")
    asyncio.run(roc_url_scraper(resume_from=None, retry_failed=False))
    logger.info("✅ Phase 2 completed: All scrapers resumed from last point")
    logger.info("🎉 Comprehensive retry and resume operation completed successfully")


@asset
def scrape_all_details() -> None:
    """Scrape details for both ROB and ROC."""
    logger.info("📋 Scraping all details...")
    # Scrape ROB details
    logger.info("Step 1: Scraping ROB details...")
    asyncio.run(LocalROBScraper().run())
    # Scrape ROC details
    logger.info("Step 2: Scraping ROC details...")
    asyncio.run(LocalROCScraper().run())


@asset
def get_retry_statistics() -> dict:
    """Get comprehensive retry statistics for both ROB and ROC scrapers."""
    logger.info("📊 Generating retry statistics for both scrapers...")
    # Get ROB retry statistics
    rob_tracker = URLScrapeTracker("rob_tracking")
    rob_stats = rob_tracker.get_retry_statistics()
    rob_summary = rob_tracker.get_retry_summary()
    # Get ROC retry statistics
    roc_tracker = URLScrapeTracker("roc_tracking")
    roc_stats = roc_tracker.get_retry_statistics()
    roc_summary = roc_tracker.get_retry_summary()
    logger.info("📊 ROB: %s", rob_summary)
    logger.info("📊 ROC: %s", roc_summary)
    return {
        "rob": {
            "statistics": rob_stats,
            "summary": rob_summary,
        },
        "roc": {
            "statistics": roc_stats,
            "summary": roc_summary,
        },
        "combined": {
            "total_failed": rob_stats["total_failed"] + roc_stats["total_failed"],
            "total_remaining_retries": rob_stats["remaining_retries"]
            + roc_stats["remaining_retries"],
            "total_exceeded_max": rob_stats["exceeded_max_retries"]
            + roc_stats["exceeded_max_retries"],
        },
    }
