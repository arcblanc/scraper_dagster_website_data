from collections.abc import Callable

import pandas as pd
from dagster import AssetExecutionContext, asset

from src.oc_etl.config import TABLE_CONFIG


def make_company_extract_asset(
    domain: str,
    database: str,
    schema: str,
    table_name: str,
    description: str,
) -> Callable[[AssetExecutionContext], pd.DataFrame]:
    """Create a Dagster asset for extracting raw data from a database table.

    Args:
        domain: The domain of the data (e.g., 'company')
        database: The database name
        schema: The schema name
        table_name: The table name to extract from
        description: Description of the asset

    Returns:
        A Dagster asset function that extracts data from the specified table

    """
    key_prefix = ["raw", domain, database, schema]

    def quote_ident(name: str) -> str:
        """Safely quote SQL identifiers to avoid injection."""
        if not name.replace("_", "").isalnum():
            msg = f"Invalid SQL identifier: {name}"
            raise ValueError(msg)

        return f'"{name}"'

    quoted_schema = quote_ident(schema)
    quoted_table = quote_ident(table_name)

    @asset(
        name=f"raw_{table_name}",
        key_prefix=key_prefix,
        required_resource_keys={database},  # assuming resource key named after database
        io_manager_key="s3_io_manager",
        description=description,
    )
    def _extract_asset(context: AssetExecutionContext) -> pd.DataFrame:
        query = f"SELECT * FROM {quoted_schema}.{quoted_table} LIMIT %s"  # noqa: S608
        company_data = getattr(context.resources, database).fetch_df(query, params=(10,))
        context.log.info(
            "Extracted %d rows from %s.%s in database %s",
            len(company_data),
            schema,
            table_name,
            database,
        )
        return company_data

    return _extract_asset


company_raw_assets: list[Callable[[AssetExecutionContext], pd.DataFrame]] = [
    make_company_extract_asset(
        cfg["domain"],
        cfg["database"],
        cfg["schema"],
        cfg["table"],
        cfg.get("description", ""),
    )
    for cfg in TABLE_CONFIG
    if cfg.get("domain") == "company" and cfg.get("db_type") == "postgres"
]
