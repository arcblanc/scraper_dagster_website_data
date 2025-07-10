import asyncio
import sys

import aiohttp
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv

from ctos_scrapers.ctos_scraper.assets import postgres_utils
from ctos_scrapers.ctos_scraper.assets.tracking import URLScrapeTracker
from ctos_scrapers.ctos_scraper.config.logging_setup import logger

load_dotenv()

LETTERS_AND_NUMS = [chr(c) for c in range(97, 123)] + [str(n) for n in range(10)]
MAX_CONCURRENCY = 10
MAX_PAGES = 5000
RETRY_LIMIT = 3
RETRY_BACKOFF = 2
EMPTY_STREAK_LIMIT = 3
HTTP_OK = 200
HTTP_TIMEOUT = 20
TRACKING_TABLE = "rob_tracking"


async def fetch_listing_page(
    session: aiohttp.ClientSession,
    char: str,
    page_num: int,
    tracker: URLScrapeTracker,
) -> list[dict[str, str]]:
    url = f"https://businessreport.ctoscredit.com.my/oneoffreport_api/malaysia-business-listing/{char}/{page_num}"
    for attempt in range(1, RETRY_LIMIT + 1):
        logger.info("🔍 Requesting: %s (Attempt %d)", url, attempt)
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
            ) as response:
                if response.status != HTTP_OK:
                    logger.warning(
                        "❌ Failed to fetch %s: HTTP %d", url, response.status
                    )
                    await asyncio.sleep(RETRY_BACKOFF * attempt)
                    continue
                text = await response.text()
                soup = BeautifulSoup(text, "html.parser")
                items = soup.select("ul.businessliststyle > li")
                results = []
                for item in items:
                    link = item.find("a") if isinstance(item, Tag) else None
                    name_span = link.find("span") if isinstance(link, Tag) else None
                    name = (
                        name_span.get_text(strip=True)
                        if isinstance(name_span, Tag)
                        else None
                    )
                    href = link.get("href", None) if isinstance(link, Tag) else None
                    href = href.strip() if isinstance(href, str) else None
                    if name and href:
                        results.append({"name": name, "url": href})
                return results
        except (TimeoutError, aiohttp.ClientError) as exc:
            logger.warning(
                "⚠️ Error on %s page %d attempt %d: %s",
                char,
                page_num,
                attempt,
                str(exc),
            )
            await asyncio.sleep(RETRY_BACKOFF * attempt)
    tracker.log_error(char, page_num, url)
    return []


async def scrape_character(
    tracker: URLScrapeTracker, char: str, sem: asyncio.Semaphore
) -> None:
    logger.info("🔤 Scraping character: %s", char)
    empty_streak = 0
    last_scraped_page = tracker.get_last_scraped_page_for_prefix(char)
    logger.info("↪️ Resuming %s from page %d", char, last_scraped_page + 1)
    async with aiohttp.ClientSession() as session:

        async def scrape_one_page(page_num: int) -> bool:
            async with sem:
                rows = await fetch_listing_page(session, char, page_num, tracker)
                if not rows:
                    logger.info("🛑 No data on %s page %d", char, page_num)
                    return False
                postgres_utils.insert_urls("rob_url_master", rows)
                logger.info(
                    "📄 [%s pg %d] Scraped and inserted %d URLs",
                    char,
                    page_num,
                    len(rows),
                )
                tracker.mark_scraped(char, page_num)
                return True

        page_num = last_scraped_page + 1
        while page_num <= MAX_PAGES:
            batch_tasks = [
                scrape_one_page(p) for p in range(page_num, page_num + MAX_CONCURRENCY)
            ]
            results = await asyncio.gather(*batch_tasks)
            if all(not r for r in results):
                logger.info(
                    "⛔ All %d pages in batch were empty, stopping '%s'",
                    MAX_CONCURRENCY,
                    char,
                )
                break
            for success in results:
                empty_streak = 0 if success else empty_streak + 1
                if empty_streak >= EMPTY_STREAK_LIMIT:
                    logger.info("⛔ 3 empty pages in a row, stopping '%s'", char)
                    return
            page_num += MAX_CONCURRENCY


async def run_parallel_scraper(
    *, resume_from: str | None = None, retry_failed: bool = False
) -> None:
    logger.info("🚀 Starting CTOS ROB URL scraping run (Postgres mode)")
    tracker = URLScrapeTracker(TRACKING_TABLE)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    if retry_failed:
        logger.info("🔁 Retrying failed pages only...")
        async with aiohttp.ClientSession() as session:
            for prefix, page, _url in tracker.get_failed_pages():
                logger.info("♻️ Retrying %s page %d", prefix, page)
                rows = await fetch_listing_page(session, prefix, page, tracker)
                if rows:
                    postgres_utils.insert_urls("rob_url_master", rows)
                    logger.info(
                        "✅ Retry success [%s pg %d]: %d inserted",
                        prefix,
                        page,
                        len(rows),
                    )
                    tracker.mark_scraped(prefix, page)
        return
    if resume_from is None:
        resume_from = tracker.get_resume_point()
    start = not resume_from
    tasks = []
    for char in LETTERS_AND_NUMS:
        if not start and char == resume_from:
            logger.info("🔁 Resuming from: '%s'", char)
            start = True
        if not start:
            continue
        tasks.append(scrape_character(tracker, char, sem))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    resume = None
    retry = "--retry-failed" in sys.argv
    if "--resume-from" in sys.argv:
        resume = sys.argv[sys.argv.index("--resume-from") + 1]
    elif "--resume-from-last" in sys.argv:
        try:
            tracker = URLScrapeTracker(TRACKING_TABLE)
            resume = tracker.get_resume_point()
        except (FileNotFoundError, OSError, ValueError) as exc:
            logger.warning("Could not fetch resume point: %s", str(exc))
    else:
        logger.info("🆕 Starting fresh scrape")
    asyncio.run(run_parallel_scraper(resume_from=resume, retry_failed=retry))
