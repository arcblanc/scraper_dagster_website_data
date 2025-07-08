import logging
import os

import pandas as pd
from dagster import AssetExecutionContext, asset

from src.oc_etl.resources.object_store import LocalObjectStore, local_store_root

from .html_parser import LitigationHTMLParser, parse_html_file

BUCKET_NAME = "oc-litigation-data"
PREFIX = "raw/html/"


@asset(
    name="parsed_from_html",
    description="Scrape HTML files from S3 and parsed as parquet files.",
    io_manager_key="s3_litigation_io_manager",
)
def parsed_from_html(context: AssetExecutionContext) -> pd.DataFrame:
    """Main asset function to parse HTML files from S3."""
    # A local run sets S3_BUCKET to local. The hearing pages are files under
    # OC_ETL_LOCAL_ROOT, and the parser below is the same one the bucket uses.
    if os.getenv("S3_BUCKET") == "local":
        store = LocalObjectStore(local_store_root())
        parser = LitigationHTMLParser()
        html_keys = [key for key in store.list_keys(PREFIX) if key.endswith(".html")]
        local_frames = []
        for html_key in html_keys:
            local_frames.extend(parser.parse_html_content(store.read_text(html_key), html_key))
        combined_local = pd.concat(local_frames, ignore_index=True).astype(str) if local_frames else pd.DataFrame()
        context.log.info("Parsed %d total tables from %d local HTML files.", len(local_frames), len(html_keys))
        return combined_local

    import boto3

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    contents = response.get("Contents", [])

    all_dataframes = []
    logger = logging.getLogger(__name__)

    for obj in contents:
        key = obj["Key"]
        if not key.endswith(".html"):
            continue

        try:
            file_dataframes = parse_html_file(key, s3)
            all_dataframes.extend(file_dataframes)
        except Exception:
            logger.exception("Failed to parse HTML file: %s", key)

    combined_df = pd.concat(all_dataframes, ignore_index=True).astype(str) if all_dataframes else pd.DataFrame()

    context.log.info("Parsed %d total tables from %d HTML files.", len(all_dataframes), len(contents))
    context.log.info("Final dataframe shape: %s", combined_df.shape)

    return combined_df
