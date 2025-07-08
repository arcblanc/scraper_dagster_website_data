"""HTML Parser module for litigation data processing.
Handles all HTML parsing logic for extracting tables from S3 HTML files.
"""

import logging
import re
from io import StringIO
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup, Tag


class LitigationHTMLParser:
    """Parser for litigation HTML files containing tables and metadata."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def _get_text_by_id_pattern(self, parent: Tag, id_suffix: str) -> str | None:
        """Find span by ID pattern that ends with the given suffix within a parent tag."""
        pattern = re.compile(f".*{re.escape(id_suffix)}$")
        span = parent.find("span", attrs={"id": pattern})
        return span.get_text(strip=True) if span else None

    def extract_track_name(self, div_part: Tag) -> str:
        """Extract track name using ID pattern for LabelProceedingName."""
        try:
            return self._get_text_by_id_pattern(div_part, "LabelProceedingName") or "Unknown Track"
        except Exception:
            self.logger.exception("Failed to extract track name")
            return "Unknown Track"

    def extract_session_metadata(self, div_part: Tag) -> dict[str, str | None]:
        """Extract session-level metadata (e.g. judge, date, time, venue) from caption section."""
        metadata: dict[str, str | None] = {
            "judge_name": None,
            "hearing_day": None,
            "hearing_date": None,
            "hearing_time": None,
            "court_level": None,
            "courtroom_detail": None,
            "court_location": None,
        }

        caption_div = div_part.find("div", class_="caption text")
        if caption_div is None:
            self.logger.warning("No caption_div found in div_part — cannot extract metadata")
            return metadata

        def get(suffix: str) -> str | None:
            return self._get_text_by_id_pattern(caption_div, suffix)

        metadata["judge_name"] = get("LabelPIC")
        metadata["hearing_day"] = get("LabelDay")
        metadata["hearing_date"] = get("LabelDate")
        metadata["hearing_time"] = get("LabelProceedingStartTime")

        jurisdiction = get("LabelJurisdiction") or ""
        division = get("LabelDivision") or ""
        metadata["court_level"] = f"{jurisdiction} {division}".strip() or None

        metadata["courtroom_detail"] = get("LabelVenue")
        metadata["court_location"] = get("LabelCourtLocation")

        return metadata

    def process_div_part_tables(self, div_part: Tag, s3_key: str) -> tuple[list[pd.DataFrame], set[int]]:
        """Process all tables within a div part and return dataframes and processed table IDs."""
        dataframes = []
        processed_table_ids = set()

        track_name = self.extract_track_name(div_part)
        session_metadata = self.extract_session_metadata(div_part)

        for table in div_part.find_all("table"):
            try:
                table_df = pd.read_html(StringIO(str(table)))[0]
                table_df["Track Name"] = track_name
                for key, value in session_metadata.items():
                    table_df[key] = value
                table_df["S3 Key"] = s3_key
                dataframes.append(table_df)
                processed_table_ids.add(id(table))
            except Exception:
                self.logger.exception("Failed to parse table in div_part with track '%s'", track_name)

        return dataframes, processed_table_ids

    def find_orphan_table_title(self, table: Tag) -> str:
        """Find title for orphan tables by traversing up the DOM."""
        current_element = table

        while current_element:
            for sibling in reversed(list(current_element.previous_siblings)):
                if not isinstance(sibling, Tag):
                    continue

                # Look for bold span titles
                bold_spans = sibling.find_all("span", style=lambda x: x and "font-weight:bold" in x)
                if bold_spans:
                    return bold_spans[0].get_text(strip=True)

                # Or caption divs
                caption_divs = sibling.find_all("div", class_="caption")
                for caption_div in caption_divs:
                    text = caption_div.get_text(strip=True)
                    if text:
                        return text

            current_element = current_element.parent

        return "Unknown Section"

    def process_orphan_tables(self, soup: BeautifulSoup, processed_table_ids: set[int], s3_key: str) -> list[pd.DataFrame]:
        """Process tables that are not within div parts."""
        orphan_dataframes = []

        for table in soup.find_all("table"):
            if id(table) in processed_table_ids:
                continue

            title = self.find_orphan_table_title(table)

            try:
                first_df = pd.read_html(StringIO(str(table)))[0]
                first_df["Track Name"] = "Unknown Track"
                first_df["Section Title"] = title
                first_df["S3 Key"] = s3_key
                orphan_dataframes.append(first_df)
            except Exception:
                self.logger.exception("Failed to parse orphan table in section '%s'", title)

        return orphan_dataframes

    def parse_html_content(self, html_content: str, s3_key: str) -> list[pd.DataFrame]:
        """Parse HTML content and return all extracted dataframes."""
        soup = BeautifulSoup(html_content, "html.parser")

        all_dataframes = []
        all_processed_table_ids = set()

        # Process div parts
        for div_part in soup.find_all("div", class_="divPart"):
            dfs, ids = self.process_div_part_tables(div_part, s3_key)
            all_dataframes.extend(dfs)
            all_processed_table_ids.update(ids)

        # Process orphan tables
        all_dataframes.extend(self.process_orphan_tables(soup, all_processed_table_ids, s3_key))

        return all_dataframes


def parse_html_file(s3_key: str, s3_client: Any) -> list[pd.DataFrame]:
    """Parse a single HTML file from S3 and return all extracted dataframes.

    Args:
        s3_key: S3 object key for the HTML file
        s3_client: Boto3 S3 client instance

    Returns:
        List of pandas DataFrames extracted from the HTML file

    """
    parser = LitigationHTMLParser()

    file_obj = s3_client.get_object(Bucket="oc-litigation-data", Key=s3_key)
    html_content = file_obj["Body"].read().decode("utf-8")

    return parser.parse_html_content(html_content, s3_key)
