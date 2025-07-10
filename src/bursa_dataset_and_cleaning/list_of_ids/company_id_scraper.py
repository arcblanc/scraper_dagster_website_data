import argparse
import asyncio
import logging
from pathlib import Path

import pandas as pd
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page, async_playwright

# ── Logging Setup ──
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ── Paths ──
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── URLs and output file mapping ──
MARKET_URLS = {
    "bursa_main.csv": "https://www.bursamalaysia.com/trade/trading_resources/listing_directory/main_market",
    "ace_market.csv": "https://www.bursamalaysia.com/trade/trading_resources/listing_directory/ace_market",
    "leap_market.csv": "https://www.bursamalaysia.com/trade/trading_resources/listing_directory/leap_market",
}
MARKET_OUTPUTS = {fname: OUTPUT_DIR / fname for fname in MARKET_URLS}
COMBINED_OUTPUT = OUTPUT_DIR / "bursa_company_list.csv"


async def scrape_market(page: Page, url: str) -> list[dict[str, str]]:
    await page.goto(url)
    frame = None
    for _ in range(30):
        for f in page.frames:
            if "listing_directory" in f.url:
                frame = f
                break
        if frame:
            break
        await asyncio.sleep(1)

    if frame is None:
        error_msg = f"❌ Could not find data iframe for {url}"
        raise RuntimeError(error_msg)

    await frame.wait_for_selector('select[name="DataTables_Table_0_length"]')
    await frame.select_option('select[name="DataTables_Table_0_length"]', value="-1")
    await frame.wait_for_timeout(2000)

    rows = await frame.query_selector_all("table#DataTables_Table_0 tbody tr")
    logger.info("🔍 Found %d rows in %s", len(rows), url)

    data = []
    for row in rows:
        try:
            anchor = await row.query_selector("td a")
            if anchor is None:
                continue
            name = await anchor.inner_text()
            href = await anchor.get_attribute("href")
            if href and "stock_code=" in href:
                company_id = href.split("stock_code=")[-1]
                data.append(
                    {"company_name": name.strip(), "company_id": company_id.strip()}
                )
        except PlaywrightError as exc:
            logger.warning("⚠️ Row parsing failed: %s", exc)
            continue
    return data


async def main(
    *,
    update_mode: bool = False,
    full_mode: bool = False,
    dry_run: bool = False,
    main_only: bool = False,
    ace_only: bool = False,
    leap_only: bool = False,
) -> None:
    all_dataframes: list[pd.DataFrame] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, slow_mo=200)
        page = await browser.new_page()

        # Determine which markets to scrape
        markets_to_scrape = {}
        if main_only:
            markets_to_scrape = {"bursa_main.csv": MARKET_URLS["bursa_main.csv"]}
        elif ace_only:
            markets_to_scrape = {"ace_market.csv": MARKET_URLS["ace_market.csv"]}
        elif leap_only:
            markets_to_scrape = {"leap_market.csv": MARKET_URLS["leap_market.csv"]}
        else:
            # Default: scrape all markets
            markets_to_scrape = MARKET_URLS

        for filename, url in markets_to_scrape.items():
            scraped = await scrape_market(page, url)
            market_df = pd.DataFrame(scraped).drop_duplicates(subset="company_id")
            output_path = MARKET_OUTPUTS[filename]
            market_df.to_csv(output_path, index=False)
            all_dataframes.append(market_df)
            logger.info("✅ Saved %d companies to %s", len(market_df), output_path.name)

        # Only create combined file if scraping all markets
        if not (main_only or ace_only or leap_only):
            new_df = pd.concat(all_dataframes, ignore_index=True).drop_duplicates(
                subset="company_id"
            )

            if update_mode and COMBINED_OUTPUT.exists():
                existing_df = pd.read_csv(COMBINED_OUTPUT, dtype=str)
                existing_ids = set(existing_df["company_id"])
                new_entries = new_df[~new_df["company_id"].isin(list(existing_ids))]

                if not new_entries.empty:
                    log_path = OUTPUT_DIR / "log_new_companies.csv"
                    if not dry_run:
                        new_entries.to_csv(log_path, index=False)
                        updated_df = pd.concat(
                            [existing_df, new_entries], ignore_index=True
                        )
                        updated_df.to_csv(COMBINED_OUTPUT, index=False)
                    logger.info(
                        "📝 Logged %d new companies to %s",
                        len(new_entries),
                        log_path.name,
                    )
                    logger.info(
                        "🆕 Added %d new companies. Total: %d",
                        len(new_entries),
                        len(existing_df) + len(new_entries),
                    )
                else:
                    logger.info("✅ No new companies to add.")

            elif full_mode or not COMBINED_OUTPUT.exists():
                if not dry_run:
                    new_df.to_csv(COMBINED_OUTPUT, index=False)
                logger.info(
                    "🧾 Full mode: saved %d companies to %s",
                    len(new_df),
                    COMBINED_OUTPUT.name,
                )

            else:
                if not dry_run:
                    new_df.to_csv(COMBINED_OUTPUT, index=False)
                logger.info(
                    "🧾 Combined and saved %d companies to %s",
                    len(new_df),
                    COMBINED_OUTPUT.name,
                )

        await browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run", action="store_true", help="Run without writing any files"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--update",
        action="store_true",
        help="Only add new company_ids to the combined CSV",
    )
    mode_group.add_argument(
        "--full",
        action="store_true",
        help="Overwrite everything (default if no flag is passed)",
    )
    mode_group.add_argument(
        "--main-only", action="store_true", help="Scrape only main market"
    )
    mode_group.add_argument(
        "--ace-only", action="store_true", help="Scrape only ACE market"
    )
    mode_group.add_argument(
        "--leap-only", action="store_true", help="Scrape only LEAP market"
    )
    args = parser.parse_args()

    asyncio.run(
        main(
            update_mode=args.update,
            full_mode=args.full,
            dry_run=args.dry_run,
            main_only=args.main_only,
            ace_only=args.ace_only,
            leap_only=args.leap_only,
        )
    )

    #
    # CLI Usage:
    # python company_id_scraper.py                      -> Default full scrape
    # python company_id_scraper.py --update            -> Append new companies only
    # python company_id_scraper.py --full              -> Overwrite everything
    # python company_id_scraper.py --dry-run           -> Simulate without writing
    # python company_id_scraper.py --update --dry-run  -> Simulate update without writing
    # python company_id_scraper.py --main-only         -> Scrape only main market
    # python company_id_scraper.py --ace-only          -> Scrape only ACE market
    # python company_id_scraper.py --leap-only         -> Scrape only LEAP market
