from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from dagster import (
    DailyPartitionsDefinition,
    asset,
    get_dagster_logger,
)

# Hearings are cleaned one calendar day at a time, from the first day the
# HTML scrape was writing parquet.
daily_partitions = DailyPartitionsDefinition(start_date="2025-07-01")


def is_informative_row(row: pd.Series) -> bool:
    values = row.dropna().astype(str).str.strip().str.lower()
    return not all(val.startswith("unknown") for val in values)


def load_column_mapping(path: str | None = None) -> dict:
    if path is None:
        # Get the directory of this file and construct the path to column_mappings.yaml
        current_dir = Path(__file__).parent
        path = str(current_dir / "column_mappings.yaml")

    with Path(path).open(encoding="utf-8") as f:
        return yaml.safe_load(f)


@asset(
    name="cleaned_parquet_df",
    description="Cleans daily parquet files parsed from HTML and removes invalid/junk data, and translates column names.",
    partitions_def=daily_partitions,
    io_manager_key="s3_litigation_io_manager",
)
def cleaned_parquet_df(
    parsed_from_html: pd.DataFrame,
) -> pd.DataFrame:
    logger = get_dagster_logger()
    raw_df = parsed_from_html
    logger.info("Starting cleaning process for raw dataframe with shape: %s", raw_df.shape)

    # Normalize column names
    raw_df.columns = [
        col.lower()
        .strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace(".", "")
        .replace("&", "and")
        .replace("__", "_")
        .replace("___", "_")
        for col in raw_df.columns
    ]

    # Drop irrelevant or placeholder columns
    columns_to_drop = [col for col in ["0", "1", "s3_key"] if col in raw_df.columns]
    raw_df = raw_df.drop(columns=columns_to_drop, errors="ignore")

    # Drop duplicates
    raw_df = raw_df.drop_duplicates()

    # Drop rows where all key fields are null
    key_fields = ["no_kes"]
    raw_df = raw_df.dropna(subset=key_fields, how="all")

    # Replace literal "nan" strings with np.nan
    raw_df = raw_df.replace(r"^\s*nan\s*$", np.nan, regex=True)

    # Remove rows where all values are 'unknown...' strings
    informative_mask = raw_df.apply(is_informative_row, axis=1)
    df_cleaned = raw_df[informative_mask].reset_index(drop=True)

    # Remove columns where all values are 'unknown...' or NaN strings
    informative_cols_mask = ~df_cleaned.apply(
        lambda col: col.dropna().astype(str).str.strip().str.lower().map(lambda x: x.startswith("unknown")).all(),
        axis=0,
    )
    df_cleaned = df_cleaned.loc[:, informative_cols_mask]

    # === Apply BM ➜ English translation ===
    column_mapping = load_column_mapping()
    translated_cols = {bm: en for bm, en in column_mapping.items() if bm in df_cleaned.columns}
    df_cleaned = df_cleaned.rename(columns=translated_cols)

    # === Trigger for unknown fields ===
    current_cols = set(df_cleaned.columns)
    expected_cols = set(column_mapping.values())
    extra_cols = current_cols - expected_cols

    if extra_cols:
        logger.error("❌ Schema change detected. Unmapped columns found: %s", sorted(extra_cols))
        msg = (
            f"Schema change detected! New columns found: {sorted(extra_cols)}. Please update column_mappings.yaml accordingly."
        )
        raise ValueError(
            msg,
        )

    logger.info("Final cleaned dataframe shape: %s", df_cleaned.shape)
    return df_cleaned
