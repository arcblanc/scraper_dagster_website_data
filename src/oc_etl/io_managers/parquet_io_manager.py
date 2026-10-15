# src/oc_etl/io_managers/s3_parquet_io_manager.py

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from dagster import InitResourceContext, InputContext, IOManager, OutputContext, io_manager


class S3ParquetIOManager(IOManager):
    """Stores DataFrames as Parquet files in S3 with date-based partitioning."""

    def __init__(self, s3_bucket: str, base_path: str) -> None:
        self.s3 = s3fs.S3FileSystem()
        self.s3_bucket = s3_bucket.rstrip("/")
        self.base_path = base_path.strip("/")

    def _get_partition_date(self) -> str:
        return datetime.now(ZoneInfo("Asia/Kuala_Lumpur")).date().isoformat()

    def _get_path(self, context: OutputContext) -> str:
        asset_parts = context.asset_key.path
        return f"{self.s3_bucket}/{self.base_path}/{'/'.join(asset_parts)}/{self._get_partition_date()}.parquet"

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        path = self._get_path(context)

        # Convert datetime to milliseconds
        for col in obj.select_dtypes(include=["datetime64[ns]"]).columns:
            obj[col] = obj[col].astype("datetime64[ms]")

        # 🔐 Ensure all object columns are converted to strings
        for col in obj.select_dtypes(include=["object"]).columns:
            obj[col] = obj[col].astype(str)

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(obj, preserve_index=False)

        # Write Parquet to S3
        with self.s3.open(path, "wb") as f:
            pq.write_table(table, f)

        msg = f"Uploaded parquet to s3://{path}"
        context.log.info(msg)
        context.log.info("Preview:\n%s", obj.head().to_markdown(index=False))

    def load_input(self, context: InputContext) -> pd.DataFrame:
        if context.upstream_output is None:
            err = ValueError("upstream_output is None; cannot determine path for input.")
            raise err
        path = self._get_path(context.upstream_output)
        with self.s3.open(path, "rb") as f:
            return pd.read_parquet(f)


@io_manager(config_schema={"s3_bucket": str, "base_path": str})
def s3_parquet_io_manager(init_context: InitResourceContext) -> S3ParquetIOManager:
    return S3ParquetIOManager(
        s3_bucket=init_context.resource_config["s3_bucket"],
        base_path=init_context.resource_config["base_path"],
    )
