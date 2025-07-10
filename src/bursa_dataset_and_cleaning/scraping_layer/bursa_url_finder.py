import argparse
import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from playwright.async_api import Page, Playwright, async_playwright
from tqdm.asyncio import tqdm_asyncio

# ─── LOGGING CONFIG ────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── PATH CONFIG ──────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = BASE_DIR / "list_of_ids" / "outputs" / "bursa_company_list.csv"
OUTPUT_PATH = BASE_DIR / "scraping_layer" / "cleaned" / "company_urls.csv"
NO_FINANCIALS_PATH = (
    BASE_DIR / "scraping_layer" / "outputs" / "rejected" / "no_financials.csv"
)
DEBUG_HTML_DIR = BASE_DIR / "scraping_layer" / "debug_html"
SCREENSHOT_DIR = BASE_DIR / "scraping_layer" / "screenshots"

for path in [
    SCREENSHOT_DIR,
    DEBUG_HTML_DIR,
    OUTPUT_PATH.parent,
    NO_FINANCIALS_PATH.parent,
]:
    path.mkdir(parents=True, exist_ok=True)

BURSA_URL = "https://my.bursamalaysia.com/market/assets/equities/stocks"

# ─── UTILITY LOADERS ────────────────────────


def load_company_ids(*, retry_mode: bool) -> list[str]:
    logger.info("📂 Loading company IDs from: %s", INPUT_PATH)
    if retry_mode and NO_FINANCIALS_PATH.exists():
        logger.info("🔄 Retry mode: Loading from failed companies list")
        retry_df = pl.read_csv(
            NO_FINANCIALS_PATH, schema_overrides={"company_id": pl.Utf8}
        )
        company_ids = [
            cid.strip().zfill(4)
            for cid in retry_df.get_column("company_id").unique().to_list()
        ]
        logger.info("📊 Retry mode: %d companies to retry", len(company_ids))
        return company_ids

    raw_df = pl.read_csv(INPUT_PATH, schema_overrides={"company_id": pl.Utf8})
    company_ids = [
        cid.strip().zfill(4)
        for cid in raw_df.get_column("company_id").unique().to_list()
    ]
    logger.info("📊 Normal mode: %d total companies loaded", len(company_ids))
    return company_ids


def load_existing_urls() -> dict:
    if OUTPUT_PATH.exists():
        try:
            logger.info("📂 Loading existing URLs from: %s", OUTPUT_PATH)
            old = pl.read_csv(
                OUTPUT_PATH,
                schema_overrides={"company_id": pl.Utf8, "new_url": pl.Utf8},
            )
            existing_urls = {
                r["company_id"]: r["new_url"] for r in old.to_dicts() if r["new_url"]
            }
            logger.info("✅ Loaded %d existing URLs", len(existing_urls))
            return existing_urls
        except Exception as exc:  # noqa: BLE001
            logger.warning("⚠️ Failed to read existing output: %s", exc)
    else:
        logger.info("📂 No existing URLs file found, starting fresh")
    return {}


def load_bad_ids() -> set:
    if NO_FINANCIALS_PATH.exists():
        try:
            logger.info(
                "📂 Loading previously failed company IDs from: %s", NO_FINANCIALS_PATH
            )
            bad_df = pl.read_csv(
                NO_FINANCIALS_PATH, schema_overrides={"company_id": pl.Utf8}
            )
            bad_ids = set(bad_df.get_column("company_id").to_list())
            logger.info("⚠️ Loaded %d previously failed company IDs", len(bad_ids))
            return bad_ids
        except Exception as exc:  # noqa: BLE001
            logger.warning("⚠️ Failed to read bad ids: %s", exc)
    return set()


def update_failed_log(*, failed: list[str], retry_mode: bool) -> None:
    if not failed:
        logger.info("🎉 No failed companies to log!")
        return

    if retry_mode:
        logger.info("🔄 Retry mode: Processing failed companies log")
        # Only compare with old and delete if all succeeded
        old_urls = {}
        if OUTPUT_PATH.exists():
            old_df = pl.read_csv(
                OUTPUT_PATH,
                schema_overrides={"company_id": pl.Utf8, "new_url": pl.Utf8},
            )
            old_urls = {
                r["company_id"]: r["new_url"] for r in old_df.to_dicts() if r["new_url"]
            }

        new_df = pl.read_csv(
            OUTPUT_PATH, schema_overrides={"company_id": pl.Utf8, "new_url": pl.Utf8}
        )
        new_urls = {
            r["company_id"]: r["new_url"] for r in new_df.to_dicts() if r["new_url"]
        }

        newly_added = {
            cid
            for cid in new_urls
            if cid not in old_urls or old_urls[cid] != new_urls[cid]
        }
        new_failed = [cid for cid in failed if cid not in newly_added]

        if new_failed:
            pl.DataFrame({"company_id": new_failed}).write_csv(NO_FINANCIALS_PATH)
            logger.error(
                "❌ Logged %d still-failed companies to %s",
                len(new_failed),
                NO_FINANCIALS_PATH,
            )
        else:
            if NO_FINANCIALS_PATH.exists():
                NO_FINANCIALS_PATH.unlink()
            logger.info(
                "🎉 All previously failed companies have now succeeded! no_financials.csv removed."
            )
    else:
        # Always write failed ones in normal mode
        pl.DataFrame({"company_id": failed}).write_csv(NO_FINANCIALS_PATH)
        logger.warning(
            "⚠️ Logged %d newly failed companies to %s", len(failed), NO_FINANCIALS_PATH
        )


# ─── SEARCH FUNCTION ───────────────────────


async def run_search_and_navigate(
    page: Page, search_term: str, retries: int = 2
) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            logger.debug(
                "🔍 Attempt %d/%d for company %s", attempt, retries, search_term
            )
            await page.goto(BURSA_URL, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000 * attempt)

            await page.locator("#stocklistingRef i").click()
            await page.get_by_text("Stock Name").click()
            await page.get_by_text("Stock Name").click()

            search_box = page.locator("#stocklistingRef").get_by_role(
                "textbox", name="Search"
            )
            await search_box.click()
            await search_box.fill(search_term)
            await page.wait_for_timeout(1500)

            match = page.locator(f"a:has(span:text-is('{search_term}'))").first
            if await match.count() > 0:
                href = await match.get_attribute("href")
                if href:
                    url = f"https://my.bursamalaysia.com{href}"
                    logger.debug("✅ Found URL for %s: %s", search_term, url)
                    return url
        except Exception as exc:  # noqa: BLE001
            logger.warning("⚠️ Error (%s) attempt %d: %s", search_term, attempt, exc)
            await page.screenshot(path=SCREENSHOT_DIR / f"{search_term}_fail.png")
            DEBUG_HTML_DIR.joinpath(f"{search_term}.html").write_text(
                await page.content()
            )
            await page.wait_for_timeout(3000 * attempt)

    logger.error("❌ Failed after %d retries: %s", retries, search_term)
    return None


# ─── SINGLE TASK ─────────────────────


@dataclass
class ScrapeContext:
    pw: Playwright
    cid: str
    existing_map: dict
    bad_ids: set
    sem: asyncio.Semaphore
    failed: list[str]


async def scrape_single(ctx: ScrapeContext) -> dict:
    if ctx.cid in ctx.bad_ids:
        logger.debug("⏭️ Skipping %s, known to have no financials.", ctx.cid)
        return {"company_id": ctx.cid, "new_url": None}

    if ctx.existing_map.get(ctx.cid):
        logger.debug("⏭️ Skipping %s, already scraped.", ctx.cid)
        return {"company_id": ctx.cid, "new_url": ctx.existing_map[ctx.cid]}

    async with ctx.sem:
        browser = await ctx.pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        url = await run_search_and_navigate(page, ctx.cid.zfill(4))
        if url:
            logger.info("✅ Success: %s → %s", ctx.cid, url)
        else:
            ctx.failed.append(ctx.cid)
            logger.warning("❌ Failed to find URL for: %s", ctx.cid)

        await context.close()
        await browser.close()

        if not url:
            ctx.failed.append(ctx.cid)
        return {"company_id": ctx.cid, "new_url": url}


# ─── MAIN ───────────────────────


async def main(*, retry_mode: bool = False) -> None:
    logger.info("🚀 Starting Bursa URL finder...")
    logger.info("📂 Input file: %s", INPUT_PATH)
    logger.info("📂 Output file: %s", OUTPUT_PATH)
    logger.info("🔄 Retry mode: %s", retry_mode)

    all_ids = load_company_ids(retry_mode=retry_mode)
    existing_map = load_existing_urls()
    bad_ids = set() if retry_mode else load_bad_ids()

    remaining = [
        cid
        for cid in all_ids
        if (cid not in existing_map or not existing_map[cid]) and cid not in bad_ids
    ]

    if not remaining:
        logger.info(
            "🎉 All company_ids have already been scraped or rejected. Nothing new to process."
        )
        return

    logger.info("📊 Processing summary:")
    logger.info("   • Total companies: %d", len(all_ids))
    logger.info("   • Already processed: %d", len(existing_map))
    logger.info("   • Previously failed: %d", len(bad_ids))
    logger.info("   • Remaining to process: %d", len(remaining))

    results = list(existing_map.items()) if existing_map else []

    sem = asyncio.Semaphore(3)
    failed: list[str] = []
    successful_count = 0
    failed_count = 0

    try:
        async with async_playwright() as pw:
            logger.info("🌐 Launching Playwright browser...")
            tasks = [
                scrape_single(
                    ScrapeContext(
                        pw=pw,
                        cid=cid,
                        existing_map=existing_map,
                        bad_ids=bad_ids,
                        sem=sem,
                        failed=failed,
                    )
                )
                for cid in remaining
            ]

            logger.info(
                "🔄 Starting URL scraping with %d concurrent workers...", sem._value
            )
            for fut in tqdm_asyncio.as_completed(
                tasks, total=len(tasks), desc="🔍 Scraping Bursa URLs"
            ):
                r = await fut
                if r["new_url"]:
                    results.append((r["company_id"], r["new_url"]))
                    successful_count += 1
                else:
                    failed_count += 1

                # Log progress every 50 companies
                if (successful_count + failed_count) % 50 == 0:
                    logger.info(
                        "📈 Progress: %d/%d processed (%.1f%%) - Success: %d, Failed: %d",
                        successful_count + failed_count,
                        len(remaining),
                        ((successful_count + failed_count) / len(remaining) * 100),
                        successful_count,
                        failed_count,
                    )
    except KeyboardInterrupt:
        logger.info("⏹️ Interrupted by user. Saving progress...")
    finally:
        unique_results = {cid: url for cid, url in results if cid and url}

        if unique_results:
            pl.DataFrame(
                {
                    "company_id": list(unique_results.keys()),
                    "new_url": list(unique_results.values()),
                }
            ).write_csv(OUTPUT_PATH)
            logger.info("💾 Saved %d URLs to %s", len(unique_results), OUTPUT_PATH)
        else:
            logger.warning("⚠️ No valid URLs to save.")

        update_failed_log(failed=failed, retry_mode=retry_mode)

        # Final summary
        logger.info("🎯 Final Results:")
        logger.info("   • Total URLs found: %d", len(unique_results))
        logger.info("   • Failed companies: %d", len(failed))
        logger.info(
            "   • Success rate: %.1f%%",
            (len(unique_results) / len(all_ids) * 100) if all_ids else 0,
        )
        logger.info("✅ URL finding process completed!")


# ─── ENTRY ──────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--retry", action="store_true", help="Retry previously failed company_ids only"
    )
    args = parser.parse_args()

    asyncio.run(main(retry_mode=args.retry))
