# src/oc_etl/assets/sanitized/sanitized.py

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, AssetIn, MetadataValue, asset

from src.oc_etl.config import TABLE_CONFIG
from src.oc_etl.ops.validated_comp_op import validate_company_profiles


def make_sanitize_asset(domain: str, database: str, schema: str, table_name: str, description: str = "") -> None:
    @asset(
        name=f"sanitized_{table_name}",
        ins={f"raw_{table_name}": AssetIn(key_prefix=["raw", domain, database, schema])},
        key_prefix=["sanitized", domain, database, schema],
        description=f"Sanitizes {table_name} data from {database}.{schema}. {description}",
        required_resource_keys={"s3"},
    )
    def _sanitize_asset(context: AssetExecutionContext, **inputs: Any) -> str:
        partition_date = datetime.now(ZoneInfo("Asia/Kuala_Lumpur")).date().isoformat()

        company_df = inputs[f"raw_{table_name}"]

        if table_name == "company_profiles":
            validate_company_profiles(company_df)

        initial_count = len(company_df)
        sanitized_df = company_df.drop_duplicates()
        final_count = len(sanitized_df)

        context.log.info("%s sanitized: %d rows -> %d rows", table_name, initial_count, final_count)

        # Define S3 path
        s3_path = (
            f"s3://onecredit-snowflake-data/data/sanitized/{domain}/{database}/{schema}/{table_name}/{partition_date}.parquet"
        )

        # Convert to PyArrow table and write to S3
        table = pa.Table.from_pandas(sanitized_df)
        s3 = context.resources.s3
        with s3.open(s3_path, "wb") as f:
            pq.write_table(table, f)

        # A local store keeps the bytes under OC_ETL_LOCAL_ROOT. The warehouse
        # copy reads a filesystem path, so that path is what the next asset gets.
        written_path = s3_path
        if hasattr(s3, "_local_path"):
            written_path = str(s3._local_path(s3_path))

        msg = "✅ Written sanitized data to: " + written_path
        context.log.info(msg)

        # Add metadata to be used by downstream assets
        context.add_output_metadata(
            {
                "path": MetadataValue.text(written_path),
                "row_count": MetadataValue.int(final_count),
                "initial_count": MetadataValue.int(initial_count),
                "duplicates_removed": MetadataValue.int(initial_count - final_count),
            },
        )

        return written_path

    return _sanitize_asset


company_sanitize_assets = [
    make_sanitize_asset(
        cfg["domain"],
        cfg["database"],
        cfg["schema"],
        cfg["table"],
        cfg.get("description", ""),
    )
    for cfg in TABLE_CONFIG
    if cfg.get("domain") == "company" and cfg.get("db_type") == "postgres"
]
