import argparse
import asyncio
import logging
import secrets
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm_asyncio

# ─── LOGGING SETUP ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───
BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs" / "cash_flow_expanded"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = BASE_DIR / "cleaned" / "company_urls.csv"
COMBINED_OUTPUT_PATH = BASE_DIR / "combined_csv" / "complete_cash_flow_statements.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_0)...",
    "Mozilla/5.0 (X11; Linux x86_64)...",
]


# ─── SCRAPE FUNCTION ───
async def scrape_company_cashflow(page, company_id: str, url: str) -> pd.DataFrame:
    logger.info("🔍 Scraping company_id %s — %s", company_id, url)
    try:
        await page.goto(url, timeout=60000)
        await page.get_by_role("button", name="Financials").click()
        await page.get_by_role("button", name="Close").click()

        for attempt in range(2):
            try:
                await page.get_by_role("button", name="Financials").click()
                await page.get_by_role("button", name="Statements").click()
                await page.get_by_role("button", name="Financials").click()
                await page.get_by_role("button", name="Statements").click()
                await page.get_by_role("button", name="Financials").click()
                await page.get_by_role("button", name="Statements").click()
                await page.get_by_role("button", name="Cash Flow").click()
                await page.get_by_role("button", name="Quarterly").click()
                await page.get_by_text("Annual").click()
                await page.wait_for_selector("div.stock-table-body")
                break
            except Exception as e:
                if attempt == 0:
                    await page.get_by_role("button", name="Profile").click()
                    await asyncio.sleep(1)
                else:
                    logger.error(
                        "❌ Navigation failed for company_id %s: %s", company_id, e
                    )
                    raise

        rows = page.locator("div.d-flex.stock-table-flex.w-100")
        row_count = await rows.count()

        raw_data = {}
        fiscal_years = []

        for i in range(row_count):
            row = rows.nth(i)
            cells = await row.locator("div").all_inner_texts()
            cells = [c.strip() for c in cells if c.strip()]
            if not cells:
                continue

            lines = cells[0].splitlines()
            if not lines:
                continue

            metric = lines[0]
            rest = lines[1:]

            if "Amount Standardised" in metric and not fiscal_years:
                for text in rest:
                    text = text.strip()
                    if text.lower() == "5-year trend":
                        continue
                    try:
                        date_label = " ".join(text.strip().split()[-3:])
                        datetime.strptime(date_label, "%d %b %Y")
                        fiscal_years.append(date_label)
                    except Exception:
                        fiscal_years.append(None)
                continue

            if len(rest) % 2 != 0:
                rest.append("")

            value_list = rest[::2]
            yoy_list = rest[1::2]

            value_list = [None if v in {"-", "—"} else v for v in value_list]
            yoy_list = [None if p in {"-", "—"} else p for p in yoy_list]

            for j, (val, pct) in enumerate(zip(value_list, yoy_list, strict=False)):
                if j >= len(fiscal_years) or fiscal_years[j] is None:
                    continue
                year = fiscal_years[j]
                val = val.strip() if isinstance(val, str) else val
                pct = pct.strip() if isinstance(pct, str) else pct

                if (
                    isinstance(val, str)
                    and val.endswith("%")
                    and (not pct or not (isinstance(pct, str) and pct.endswith("%")))
                ):
                    val, pct = None, val

                if isinstance(val, str) and not val.endswith("%"):
                    raw_data.setdefault((year, "Value"), {})[metric] = val
                else:
                    raw_data.setdefault((year, "Value"), {})[metric] = None

                if isinstance(pct, str) and pct.endswith("%"):
                    raw_data.setdefault((year, "YoY %"), {})[metric] = pct
                else:
                    raw_data.setdefault((year, "YoY %"), {})[metric] = None

        wide_data = defaultdict(dict)
        for (year, typ), metrics in raw_data.items():
            row_label = f"{year} {typ}"
            for metric, val in metrics.items():
                wide_data[row_label][metric] = val

        df = pd.DataFrame.from_dict(wide_data, orient="index").reset_index()
        df = df.rename(columns={"index": "Year/Type"})
        df.insert(0, "company_id", str(company_id))
        df["source_url"] = url

        for col in df.columns:
            if "Amount Standardised" in col:
                df = df.drop(columns=[col])
                break

        return df

    except Exception as e:
        logger.error("⚠️ Failed to scrape company_id %s — %s: %s", company_id, url, e)
        return pd.DataFrame()


# ─── TASK WRAPPER ───
async def scrape_wrapper(pw, sem, entry):
    cid, url = entry["company_id"], entry["new_url"]
    random_user_agent = secrets.choice(USER_AGENTS)

    async with sem:
        browser = await pw.chromium.launch(headless=True, slow_mo=300)
        context = await browser.new_context(user_agent=random_user_agent)
        page = await context.new_page()

        try:
            df_result = await scrape_company_cashflow(page, cid, url)
            if not df_result.empty:
                logger.info(
                    "Preview for company_id %s — %s:\n%s",
                    cid,
                    url,
                    df_result.head(5).to_string(),
                )
                df_result.to_csv(OUTPUTS_DIR / f"{cid}.csv", index=False)
        except Exception as e:
            logger.error("❌ Error scraping %s: %s", cid, e)
        finally:
            await context.close()
            await browser.close()
            await asyncio.sleep(6 + secrets.randbelow(10))


# ─── MAIN FUNCTION ───
async def main():
    parser = argparse.ArgumentParser(description="Scrape Bursa cash flow statements.")
    parser.add_argument(
        "--company-id", type=str, help="If set, scrape only this company ID"
    )
    args = parser.parse_args()

    df_urls = pd.read_csv(CSV_PATH, dtype=str).dropna(subset=["company_id", "new_url"])

    if args.company_id:
        df_urls = df_urls[df_urls["company_id"] == args.company_id]
        logger.info("🔍 Running in single-company mode: %s", args.company_id)
    else:
        seen_ids = {f.stem for f in OUTPUTS_DIR.glob("*.csv")}
        df_urls = df_urls[~df_urls["company_id"].isin(seen_ids)]

    companies = df_urls.to_dict("records")
    if not companies:
        logger.info("⚠️ No companies to scrape.")
        return

    sem = asyncio.Semaphore(3)
    async with async_playwright() as pw:
        tasks = [scrape_wrapper(pw, sem, entry) for entry in companies]
        for task in tqdm_asyncio.as_completed(
            tasks, total=len(tasks), desc="Scraping companies"
        ):
            await task

    csvs_to_merge = [
        f for f in OUTPUTS_DIR.glob("*.csv") if f.name != COMBINED_OUTPUT_PATH.name
    ]
    logger.info("📦 Merging %d CSVs from %s", len(csvs_to_merge), OUTPUTS_DIR)
    logger.info("📍 Combining cash flow files into: %s", COMBINED_OUTPUT_PATH)

    combined_df = pd.concat(
        (pd.read_csv(f, dtype={"company_id": str}) for f in csvs_to_merge),
        ignore_index=True,
    )

    # Ensure the combined_csv directory exists
    COMBINED_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    combined_df.to_csv(COMBINED_OUTPUT_PATH, index=False)

    logger.info("✅ Saved combined file to %s", COMBINED_OUTPUT_PATH)
    logger.info(
        "✅ Combined %d files into %s", len(csvs_to_merge), COMBINED_OUTPUT_PATH
    )


if __name__ == "__main__":
    asyncio.run(main())
