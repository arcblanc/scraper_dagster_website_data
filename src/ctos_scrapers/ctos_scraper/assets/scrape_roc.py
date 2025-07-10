import asyncio
from typing import Any

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from more_itertools import chunked
from tqdm import tqdm

from ctos_scrapers.ctos_scraper.assets import postgres_utils
from ctos_scrapers.ctos_scraper.config.logging_setup import logger

load_dotenv()

# Constants
URL_TABLE = "roc_url_master"
REG_TABLE = "roc_registration"
MAX_CONCURRENCY = 200  # Optimal concurrency based on performance testing
BATCH_SIZE = 200  # Optimal batch size for maximum performance (1.5-1.9M URLs)
HTTP_OK = 200
HTTP_TIMEOUT = 30
REGISTRATION_PARTS_EXPECTED = 2

# Database setup moved to asset functions to avoid import-time connections


class LocalROCScraper:
    def __init__(self) -> None:
        # Get URLs that need scraping using efficient database filtering
        self.urls_to_scrape = postgres_utils.fetch_unscraped_urls(URL_TABLE, REG_TABLE)

        # Get total URLs in master for statistics
        all_urls = postgres_utils.fetch_all_urls(URL_TABLE)
        total_urls = len(all_urls)
        already_scraped = total_urls - len(self.urls_to_scrape)

        logger.info(
            "📊 Total URLs in master: %d, Already scraped: %d, Remaining: %d",
            total_urls,
            already_scraped,
            len(self.urls_to_scrape),
        )

        self.total_pushed = 0
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    def _extract_registration_numbers(
        self, value: str
    ) -> tuple[str | None, str | None]:
        if not value:
            return None, None
        # Split by '/' and strip spaces
        parts = [p.strip() for p in value.split("/") if p.strip()]
        if len(parts) == REGISTRATION_PARTS_EXPECTED:
            return parts[0], parts[1]
        if len(parts) == 1:
            return None, parts[0]
        return None, None

    def _parse_html(
        self, html: str, url: str, company_id: str
    ) -> dict[str, Any] | None:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("div.detailstable table.table-striped")
        rows = table.select("tr") if table else []
        data = {
            "id": company_id,
            "url": url,
            "business_name": None,
            "registration_number": None,
            "registration_number_old": None,
            "business_type": None,
            "business_commenced": None,
            "state": None,
        }
        for row in rows:
            th = row.select_one("th")  # type: ignore[attr-defined]
            td = row.select_one("td")  # type: ignore[attr-defined]
            if not th or not td:
                continue
            label = th.get_text(strip=True).lower()  # type: ignore[attr-defined]
            spans = td.select("span")  # type: ignore[attr-defined]
            value = (
                " ".join(span.get_text(strip=True) for span in spans)
                if spans
                else td.get_text(strip=True)
            )  # type: ignore[attr-defined]
            if "company name" in label:
                data["business_name"] = value
            elif "registration no" in label:
                old_num, new_num = self._extract_registration_numbers(value)
                data["registration_number_old"] = old_num
                data["registration_number"] = (
                    str(new_num) if new_num is not None else None
                )
            elif "nature of business" in label:
                data["business_type"] = value
            elif "date of registration" in label:
                data["business_commenced"] = value
            elif "state" in label:
                data["state"] = value
        return data if data["registration_number"] else None

    async def _scrape_one(
        self, session: aiohttp.ClientSession, company_id: str, url: str
    ) -> dict[str, Any] | None:
        async with self.semaphore:
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
                ) as response:
                    if response.status != HTTP_OK:
                        logger.warning(
                            "Non-200 response for %s: %d", url, response.status
                        )
                        return None
                    html = await response.text()
            except (TimeoutError, aiohttp.ClientError) as e:
                logger.warning("Error fetching %s: %s", url, str(e))
                return None
            return self._parse_html(html, url, company_id)

    async def run(self) -> None:
        logger.info(
            "🔁 Starting ROC scrape with %d concurrency, %d batch size, %d URLs",
            MAX_CONCURRENCY,
            BATCH_SIZE,
            len(self.urls_to_scrape),
        )
        pbar = tqdm(total=len(self.urls_to_scrape), desc="Scraping ROC", unit="url")

        # Optimized session with connection pooling and keep-alive
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENCY * 2,  # Allow more connections than concurrency
            limit_per_host=100,  # Limit connections per host
            keepalive_timeout=30,  # Keep connections alive
            enable_cleanup_closed=True,  # Clean up closed connections
            ttl_dns_cache=300,  # Cache DNS for 5 minutes
        )

        timeout = aiohttp.ClientTimeout(
            total=HTTP_TIMEOUT,
            connect=10,  # Faster connection timeout
            sock_read=20,  # Socket read timeout
        )

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            },
        ) as session:
            for chunk in chunked(self.urls_to_scrape, BATCH_SIZE):
                logger.info(
                    "🔄 Processing batch of %d URLs with %d concurrency",
                    len(chunk),
                    MAX_CONCURRENCY,
                )
                tasks = [self._scrape_one(session, url, url) for url in chunk]
                results = await asyncio.gather(*tasks)
                cleaned = [r for r in results if r is not None]
                if cleaned:
                    postgres_utils.upsert_registration_batch(REG_TABLE, cleaned)
                    self.total_pushed += len(cleaned)
                    logger.info("📥 Saved %d new items this batch", len(cleaned))
                pbar.update(len(chunk))
        pbar.close()
        logger.info(
            "🎉 Finished. Total safely pushed to Postgres: %d", self.total_pushed
        )


if __name__ == "__main__":
    logger.info("Starting scrape_roc.py main entrypoint")
    asyncio.run(LocalROCScraper().run())
