import argparse
import asyncio
import logging
import random
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm_asyncio

# ─── LOGGING ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs" / "profile_details"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = BASE_DIR / "cleaned" / "company_urls.csv"
# So this should be 5 sections that points to the outputs/profile_details
SECTIONS = ["profile", "management", "ownership", "top10", "insider"]

# Individual output directories for each section
INDIVIDUAL_OUTPUT_DIRS = {
    "profile": BASE_DIR / "outputs" / "profile_details" / "profile_expanded",
    "management": BASE_DIR / "outputs" / "profile_details" / "management_expanded",
    "ownership": BASE_DIR / "outputs" / "profile_details" / "ownership_expanded",
    "top10": BASE_DIR / "outputs" / "profile_details" / "top_10_expanded",
    "insider": BASE_DIR / "outputs" / "profile_details" / "insider_expanded",
}

# Combined output paths for each section
COMBINED_OUTPUT_PATHS = {
    "profile": BASE_DIR / "combined_csv" / "combined_profile.csv",
    "management": BASE_DIR / "combined_csv" / "combined_management.csv",
    "ownership": BASE_DIR / "combined_csv" / "combined_ownership.csv",
    "top10": BASE_DIR / "combined_csv" / "combined_top10.csv",
    "insider": BASE_DIR / "combined_csv" / "combined_insider.csv",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_0)...",
    "Mozilla/5.0 (X11; Linux x86_64)...",
]

# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────


# get_seen_ids() checks which companies have already been scraped.
# inject_id_url() adds company_id and company_url metadata to each scraped DataFrame.
def get_seen_ids():
    return {
        f.stem.split(".")[0] for f in INDIVIDUAL_OUTPUT_DIRS["profile"].glob("*.csv")
    }


def inject_id_url(df, company_id, company_url):
    df.insert(0, "company_id", company_id)
    df.insert(1, "company_url", company_url)
    return df


# ─────────────────────────────────────────────
# SCRAPER UTILITIES
# ─────────────────────────────────────────────


# Extracts table headers and rows from a given section selector on the page, returning a structured DataFrame.
async def extract_section_table(page, section_selector: str) -> pd.DataFrame:
    header_locator = page.locator(f"{section_selector} .stock-table-head > div")
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")

    headers = await header_locator.all_inner_texts()
    row_count = await row_locator.count()

    rows = []
    for i in range(row_count):
        cells = await row_locator.nth(i).locator("div").all_inner_texts()
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells, strict=False)))
        else:
            logger.warning("⚠️ Row length mismatch at index %d, skipping row.", i)

    return pd.DataFrame(rows)


# Loops through paginated tables, clicking "Next" until no more pages, aggregating data into one DataFrame.
async def paginate_section(
    page, next_button_selector: str, section_selector: str
) -> pd.DataFrame:
    all_pages = []
    while True:
        df = await extract_section_table(page, section_selector)
        if not df.empty:
            all_pages.append(df)
        try:
            next_btn = page.locator(next_button_selector)
            if await next_btn.is_enabled():
                await next_btn.click()
                await page.wait_for_timeout(1500)
            else:
                break
        except Exception:
            break
    return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()


# ─────────────────────────────────────────────
# PROFILE EXTRACTOR
# ─────────────────────────────────────────────


# Scrapes the "About", sector, contact info, and address from the profile overview page into a single-row DataFrame.
async def extract_profile_overview(page, company_id: str) -> pd.DataFrame:
    data = {"company_id": company_id}

    try:
        about = page.locator("div.contactDetails-left .contactInfo-value")
        data["About"] = await about.inner_text()
    except Exception:
        data["About"] = None

    try:
        raw_text = await page.locator("div.contactDetails-right").inner_text()
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        field_map = {
            "Sector": None,
            "Sub Sector": None,
            "Website": None,
            "Phone": None,
            "Fax": None,
        }
        for i, label in enumerate(lines):
            if label in field_map:
                field_map[label] = lines[i + 1]
        data.update(field_map)
    except Exception as e:
        logger.warning("⚠️ Failed parsing right column: %s", e)

    try:
        address_node = page.locator("a.location_pin")
        if await address_node.count() > 0:
            data["Address"] = await address_node.inner_text()
        else:
            await page.get_by_text("Address", exact=True).click()
            data["Address"] = await page.locator("a.location_pin").inner_text()
    except Exception:
        data["Address"] = None

    return pd.DataFrame([data])


# ─────────────────────────────────────────────
# MANAGEMENT EXTRACTOR
# ─────────────────────────────────────────────
async def extract_management_table(
    page, section_selector: str, company_id: str
) -> pd.DataFrame:
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    data = []
    for i in range(row_count):
        row = row_locator.nth(i)

        # Try multiple selectors for name
        name = None
        name_selectors = [
            ".nameCol",
            ".ownerCol span",
            ".investorCol",
            "div.scroll span",
            ".personCol",
        ]
        for selector in name_selectors:
            try:
                if await row.locator(selector).count() > 0:
                    name = await row.locator(selector).inner_text()
                    if name and name.strip():
                        break
            except Exception:
                continue

        # Try multiple selectors for designation
        designation = None
        designation_selectors = [
            ".designationCol",
            ".roleCol",
            ".positionCol",
            ".titleCol",
        ]
        for selector in designation_selectors:
            try:
                if await row.locator(selector).count() > 0:
                    designation = await row.locator(selector).inner_text()
                    if designation and designation.strip():
                        break
            except Exception:
                continue

        # Try multiple selectors for role
        role = None
        role_selectors = [".roleCol", ".designationCol", ".positionCol", ".titleCol"]
        for selector in role_selectors:
            try:
                if await row.locator(selector).count() > 0:
                    role = await row.locator(selector).inner_text()
                    if role and role.strip():
                        break
            except Exception:
                continue

        # Try to extract since date
        since = None
        try:
            if await row.locator(".sinceCol").count() > 0:
                since = await row.locator(".sinceCol").inner_text()
        except Exception:
            pass

        data.append(
            {
                "company_id": company_id,
                "Name": name,
                "Designation": designation,
                "Role": role,
                "Since": since,
            }
        )

    return pd.DataFrame(data)


async def paginate_management_table(
    page, section_selector: str, company_id: str
) -> pd.DataFrame:
    all_pages = []
    while True:
        df = await extract_management_table(page, section_selector, company_id)
        if not df.empty:
            all_pages.append(df)

        try:
            next_btns = page.locator("button", has_text="Next")
            found = False
            for i in range(await next_btns.count()):
                btn = next_btns.nth(i)
                if await btn.get_attribute("disabled") is None:
                    await btn.click()
                    await page.wait_for_timeout(1500)
                    found = True
                    break
            if not found:
                break
        except Exception:
            break

    return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()


# ─────────────────────────────────────────────
# OWNERSHIP EXTRACTOR
# ─────────────────────────────────────────────
async def extract_ownership_table(
    page, section_selector: str, company_id: str
) -> pd.DataFrame:
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    data = []
    for i in range(row_count):
        row = row_locator.nth(i)

        # Try multiple selectors for investor name
        investor_name = None
        name_selectors = [
            ".ownerCol span",
            ".nameCol",
            ".investorCol",
            "div.scroll span",
            ".ownerCol",
        ]
        for selector in name_selectors:
            try:
                if await row.locator(selector).count() > 0:
                    investor_name = await row.locator(selector).inner_text()
                    if investor_name and investor_name.strip():
                        break
            except Exception:
                continue

        # Extract other columns
        no_of_investors = None
        ownership_pct = None
        position_shares = None
        position_change = None
        position_change_pct = None
        position_value_change = None
        value_usd = None

        try:
            if await row.locator(".owner_idCol").count() > 0:
                no_of_investors = await row.locator(".owner_idCol").inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".ownership_percentageCol").count() > 0:
                ownership_pct = await row.locator(
                    ".ownership_percentageCol"
                ).inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".shares_heldCol").count() > 0:
                position_shares = await row.locator(".shares_heldCol").inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".shares_changedCol").count() > 0:
                position_change = await row.locator(".shares_changedCol").inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".position_changeCol").count() > 0:
                position_change_pct = await row.locator(
                    ".position_changeCol"
                ).inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".value_of_shares_changedCol").count() > 0:
                position_value_change = await row.locator(
                    ".value_of_shares_changedCol"
                ).inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".value_heldCol").count() > 0:
                value_usd = await row.locator(".value_heldCol").inner_text()
        except Exception:
            pass

        data.append(
            {
                "company_id": company_id,
                "Investor Name": investor_name,
                "No. of Investors": no_of_investors,
                "Ownership %": ownership_pct,
                "Position (M shares)": position_shares,
                "Position Change (M)": position_change,
                "Position Change (M) %": position_change_pct,
                "Position Value Change (M)": position_value_change,
                "Value (M USD)": value_usd,
            }
        )

    return pd.DataFrame(data)


# ─────────────────────────────────────────────
# TOP 10 EXTRACTOR
# ─────────────────────────────────────────────
async def extract_top10_table(
    page, section_selector: str, company_id: str
) -> pd.DataFrame:
    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    data = []
    for i in range(row_count):
        row = row_locator.nth(i)

        # Try multiple selectors for investor name
        investor_name = None
        name_selectors = [
            ".ownerCol span",
            ".nameCol",
            ".investorCol",
            "div.scroll span",
            ".ownerCol",
        ]
        for selector in name_selectors:
            try:
                if await row.locator(selector).count() > 0:
                    investor_name = await row.locator(selector).inner_text()
                    if investor_name and investor_name.strip():
                        break
            except Exception:
                continue

        # Extract other columns
        ownership_pct = None
        position_shares = None
        position_change = None
        position_change_pct = None
        position_value_change = None
        value_usd = None
        filing_date = None
        filing_source = None

        try:
            if await row.locator(".ownership_percentageCol").count() > 0:
                ownership_pct = await row.locator(
                    ".ownership_percentageCol"
                ).inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".shares_heldCol").count() > 0:
                position_shares = await row.locator(".shares_heldCol").inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".shares_changedCol").count() > 0:
                position_change = await row.locator(".shares_changedCol").inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".position_changeCol").count() > 0:
                position_change_pct = await row.locator(
                    ".position_changeCol"
                ).inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".value_of_shares_changedCol").count() > 0:
                position_value_change = await row.locator(
                    ".value_of_shares_changedCol"
                ).inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".value_heldCol").count() > 0:
                value_usd = await row.locator(".value_heldCol").inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".report_dateCol").count() > 0:
                filing_date = await row.locator(".report_dateCol").inner_text()
        except Exception:
            pass

        try:
            if await row.locator(".sourceCol").count() > 0:
                filing_source = await row.locator(".sourceCol").inner_text()
        except Exception:
            pass

        data.append(
            {
                "company_id": company_id,
                "Investor Name": investor_name,
                "Ownership %": ownership_pct,
                "Position (M Shares)": position_shares,
                "Position Change (M)": position_change,
                "Position Change (M) %": position_change_pct,
                "Position Value Change (M)": position_value_change,
                "Value (M USD)": value_usd,
                "Filing Date": filing_date,
                "Filing Source": filing_source,
            }
        )

    return pd.DataFrame(data)


# ─────────────────────────────────────────────
# INSIDER EXTRACTOR
# ─────────────────────────────────────────────
async def extract_insider_table(
    page, section_selector: str, company_id: str
) -> pd.DataFrame:
    """Extracts insider trading data with specific columns."""
    headers = [
        "Investor Name",
        "Ownership %",
        "Position (M Shares)",
        "Position Change (M)",
        "Position Change (M) %",
        "Position Value Change (M)",
        "Value (M USD)",
        "Filing Date",
        "Filing Source",
    ]

    row_locator = page.locator(f"{section_selector} .stock-table-body .stock-table-row")
    row_count = await row_locator.count()

    rows = []
    for i in range(row_count):
        cells = await row_locator.nth(i).locator("div").all_inner_texts()
        rows.append(dict(zip(headers, cells, strict=False)))

    df = pd.DataFrame(rows)
    df.insert(0, "company_id", company_id)
    return df


async def paginate_insider_table(
    page, section_selector: str, company_id: str
) -> pd.DataFrame:
    """Paginates through the insider trading table."""
    all_pages = []
    while True:
        df = await extract_insider_table(page, section_selector, company_id)
        if not df.empty:
            all_pages.append(df)

        try:
            next_btns = page.locator("button", has_text="Next")
            is_disabled = True
            for i in range(await next_btns.count()):
                btn = next_btns.nth(i)
                if await btn.get_attribute("disabled") is None:
                    await btn.click()
                    await page.wait_for_timeout(1500)
                    is_disabled = False
                    break
            if is_disabled:
                break
        except Exception:
            break

    return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()


# ─────────────────────────────────────────────
# MAIN PROFILE SCRAPER
# ─────────────────────────────────────────────


# Main orchestrator that clicks through the profile and all section tabs,
# runs respective extractors, and returns a dictionary of 5 DataFrames.
async def scrape_company_profile(page, company_id: str, url: str) -> dict:
    logger.info(f"🔍 Scraping company_id: {company_id}")

    await page.goto(url, timeout=60000)
    await page.get_by_role("button", name="Close").click()
    await page.get_by_role("button", name="Profile").click()

    profile_df = await extract_profile_overview(page, company_id)
    # Add company_url to profile data
    profile_df["company_url"] = url
    logger.info("📄 Profile Extracted: %d rows", len(profile_df))

    await page.get_by_text("NameDesignationRoleSince").click()
    await page.get_by_role("button", name="Details").first.click()
    management_df = await paginate_management_table(
        page, "div.stock-detailed-profile-manegement-table", company_id
    )
    # Add company_url to management data
    management_df["company_url"] = url
    logger.info("📊 Table 'Management' Extracted: %d rows", len(management_df))

    # Ownership Section
    try:
        await page.get_by_role("button", name="Details").first.click()

        next_items = page.get_by_role("listitem").filter(has_text="Next")
        if await next_items.count() > 1:
            await next_items.nth(1).click()

        ownership_df = await extract_ownership_table(
            page, "div:nth-child(3)", company_id
        )
        # Add company_url to ownership data
        ownership_df["company_url"] = url
        logger.info("📊 Table 'Ownership Type' Extracted: %d rows", len(ownership_df))
    except Exception as e:
        logger.warning("⚠️ Skipping Ownership section: %s", e)
        ownership_df = pd.DataFrame()

    # Top 10 Investors Section
    try:
        await page.get_by_role("button", name="Details").nth(1).click()

        next_items = page.get_by_role("listitem").filter(has_text="Next")
        if await next_items.count() > 2:
            await next_items.nth(2).click()

        top10_df = await extract_top10_table(page, "div:nth-child(4)", company_id)
        # Add company_url to top10 data
        top10_df["company_url"] = url
        logger.info("📊 Table 'Top 10 Investors' Extracted: %d rows", len(top10_df))
    except Exception as e:
        logger.warning("⚠️ Skipping Top 10 Investors section: %s", e)
        top10_df = pd.DataFrame()

    # Insider
    try:
        await page.get_by_text("Insider", exact=True).click()
        await page.wait_for_timeout(1000)
        insider_df = await paginate_insider_table(
            page, "section.insider-trading", company_id
        )
        insider_df = inject_id_url(insider_df, company_id, url)
    except Exception as e:
        logger.error("❌ Failed to scrape insider for %s: %s", company_id, e)
        insider_df = pd.DataFrame()

    return {
        "profile": profile_df,
        "management": management_df,
        "ownership": ownership_df,
        "top10": top10_df,
        "insider": insider_df,
    }


# ─────────────────────────────────────────────
# TASK WRAPPER
# ─────────────────────────────────────────────
async def scrape_wrapper(pw, sem, entry):
    cid, url = entry["company_id"], entry["new_url"]
    random_user_agent = random.choice(USER_AGENTS)

    async with sem:
        browser = await pw.chromium.launch(headless=True, slow_mo=300)
        context = await browser.new_context(user_agent=random_user_agent)
        page = await context.new_page()

        try:
            results = await scrape_company_profile(page, cid, url)

            # Save individual CSV files for each section
            for section, df in results.items():
                if not df.empty:
                    # Ensure the section directory exists
                    section_dir = INDIVIDUAL_OUTPUT_DIRS[section]
                    section_dir.mkdir(parents=True, exist_ok=True)

                    output_file = section_dir / f"{cid}.csv"
                    df.to_csv(output_file, index=False)
                    logger.info(
                        "Saved %s data for company_id %s — %d rows",
                        section,
                        cid,
                        len(df),
                    )
                else:
                    logger.warning("No %s data for company_id %s", section, cid)

        except Exception as e:
            logger.error("Error scraping %s: %s", cid, e)
        finally:
            await context.close()
            await browser.close()
            await asyncio.sleep(random.uniform(6, 15))


# ─────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Scrape Bursa company profiles.")
    parser.add_argument(
        "--company-id",
        type=str,
        help="If set, scrape only this company ID, even if its CSV already exists.",
    )
    args = parser.parse_args()

    logger.info("📖 Loading company URLs from %s", CSV_PATH)
    df_urls = pd.read_csv(CSV_PATH, dtype=str)
    df_urls = df_urls.dropna(subset=["company_id", "new_url"])
    logger.info("📊 Loaded %d companies from CSV", len(df_urls))

    if args.company_id:
        df_urls = df_urls[df_urls["company_id"] == args.company_id]
        logger.info("🔍 Running in single-company mode: %s", args.company_id)
    else:
        seen_ids = get_seen_ids()
        logger.info(
            "📁 Found %d existing profile CSV files in %s", len(seen_ids), OUTPUTS_DIR
        )
        df_urls = df_urls[~df_urls["company_id"].isin(seen_ids)]
        logger.info("🎯 %d companies remaining to scrape", len(df_urls))

    companies = df_urls.to_dict("records")
    if not companies:
        logger.warning("No companies to scrape.")
        return

    sem = asyncio.Semaphore(5)

    async with async_playwright() as pw:
        tasks = [scrape_wrapper(pw, sem, entry) for entry in companies]
        for task in tqdm_asyncio.as_completed(
            tasks, total=len(tasks), desc="Scraping companies"
        ):
            await task

    # Combine all CSV files for each section
    for section in SECTIONS:
        section_dir = INDIVIDUAL_OUTPUT_DIRS[section]
        csvs_to_merge = list(section_dir.glob("*.csv"))
        if csvs_to_merge:
            logger.info(
                "📦 Merging %d %s CSVs from %s",
                len(csvs_to_merge),
                section,
                section_dir,
            )
            logger.info(
                "📍 Combining %s files into: %s",
                section,
                COMBINED_OUTPUT_PATHS[section],
            )

            combined_df = pd.concat(
                (pd.read_csv(f, dtype={"company_id": str}) for f in csvs_to_merge),
                ignore_index=True,
            )

            # Ensure the combined_csv directory exists
            COMBINED_OUTPUT_PATHS[section].parent.mkdir(parents=True, exist_ok=True)

            combined_df.to_csv(COMBINED_OUTPUT_PATHS[section], index=False)
            logger.info(
                "✅ Saved combined %s file to %s",
                section,
                COMBINED_OUTPUT_PATHS[section],
            )
            logger.info(
                "✅ Combined %d files into %s",
                len(csvs_to_merge),
                COMBINED_OUTPUT_PATHS[section],
            )
        else:
            logger.warning("⚠️ No %s CSV files found to combine", section)


if __name__ == "__main__":
    asyncio.run(main())
