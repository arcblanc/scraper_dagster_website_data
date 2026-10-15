"""Parquet IO manager that writes under the local object store.

The config still says s3_bucket and base_path so the existing Definitions
object can configure it. Bucket name 'local' means a directory, not AWS.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from dagster import InitResourceContext, InputContext, IOManager, OutputContext, io_manager

from src.oc_etl.resources.object_store import local_store_root


class LocalParquetIOManager(IOManager):
    def __init__(self, bucket_name: str, base_path: str) -> None:
        self.bucket_name = bucket_name.strip("/")
        self.base_path = base_path.strip("/")
        self.root = local_store_root()

    def _file_for(self, asset_key_path: list[str]) -> Path:
        # One stable file per asset so a downstream load finds the extract
        # from the same run without depending on the clock.
        relative = Path(self.bucket_name) / self.base_path / Path(*asset_key_path) / "latest.parquet"
        return self.root / relative

    def handle_output(self, context: OutputContext, company_frame: pd.DataFrame) -> None:
        destination = self._file_for(list(context.asset_key.path))
        destination.parent.mkdir(parents=True, exist_ok=True)
        company_frame.to_parquet(destination, index=False)
        context.log.info("Wrote %s rows to %s", len(company_frame), destination)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        if context.upstream_output is None:
            message = "No upstream output, so there is no parquet path to load."
            raise ValueError(message)
        source = self._file_for(list(context.upstream_output.asset_key.path))
        return pd.read_parquet(source)


@io_manager(config_schema={"s3_bucket": str, "base_path": str})
def s3_parquet_io_manager(init_context: InitResourceContext) -> LocalParquetIOManager:
    config = init_context.resource_config
    return LocalParquetIOManager(
        bucket_name=config["s3_bucket"] or "local",
        base_path=config["base_path"],
    )
