# src/oc_etl/assets/upload/upload_to_snowflake_assets.py
import os

from dagster import Any, AssetExecutionContext, AssetIn, AssetsDefinition, asset

from src.oc_etl.config import TABLE_CONFIG


def make_upload_to_snowflake_asset(
    domain: str,
    database: str,
    schema: str,
    table_name: str,
    description: str = "",
) -> AssetsDefinition:
    @asset(
        name=f"upload_to_snow_{table_name}",
        ins={f"sanitized_{table_name}": AssetIn(key_prefix=["sanitized", domain, database, schema])},
        key_prefix=["upload_to_snowflake", domain, database, schema],
        description=f"Uploads sanitized for {table_name} to Snowflake {database}.{schema}.{table_name}. {description}",
        required_resource_keys={"snowflake_resource"},
    )
    def _upload_asset(context: AssetExecutionContext, **inputs: Any) -> int:
        # Get the S3 path from the sanitized asset - it's now a string directly
        s3_path = inputs[f"sanitized_{table_name}"]
        msg = "📦 Loading data from S3 path:" + s3_path
        context.log.info(msg)

        snowflake_conn = context.resources.snowflake_resource
        # The local warehouse is a DuckDB file. It has no cursor, and it
        # returns the same COUNT the live load logs below.
        if type(snowflake_conn).__name__ == "LocalWarehouse":
            truncate = os.getenv("SNOW_TRUNCATE", "true").lower() == "true"
            return int(snowflake_conn.load_local_parquet(s3_path, table_name, truncate))

        full_table = f"{os.getenv('SNOW_DATABASE')}.{os.getenv('SNOW_SCHEMA')}.{table_name}"
        # A live Dagster resource may be the connector itself, or the wrapper
        # that holds the connector on _connection.
        cursor_owner = getattr(snowflake_conn, "_connection", snowflake_conn)

        with cursor_owner.cursor() as cursor:
            if os.getenv("SNOW_TRUNCATE", "true").lower() == "true":
                msg = "🔄 Truncating table" + full_table + "before loading new data"
                context.log.info(msg)
                cursor.execute(f"TRUNCATE TABLE {full_table}")

            storage_integration = os.getenv("SNOW_STORAGE_INTEGRATION")

            copy_sql = f"""
                COPY INTO {full_table}
                FROM '{s3_path}'
                STORAGE_INTEGRATION = {storage_integration}
                FILE_FORMAT = (FORMAT_NAME = 'OC_SNOWFLAKE.STAGING.MY_PARQUET_FORMAT')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE'
            """
            msg = "🔄 COPY INTO for " + full_table
            context.log.info(msg)

            try:
                cursor.execute(copy_sql)
            except Exception as e:
                msg = f"❌ COPY INTO failed for {full_table} with error: {e}"
                context.log.exception(msg)
                raise

            cursor.execute(f"SELECT COUNT(*) FROM {full_table}")  # noqa: S608
            row_count = cursor.fetchone()[0]
            msg = f"✅ Successfully loaded {row_count} rows into " + full_table
            context.log.info(msg)
            return int(row_count)

    return _upload_asset


company_snow_assets = [
    make_upload_to_snowflake_asset(
        cfg["domain"],
        cfg["database"],
        cfg["schema"],
        cfg["table"],
        cfg.get("description", ""),
    )
    for cfg in TABLE_CONFIG
    if cfg.get("domain") == "company" and cfg.get("db_type") == "postgres"
]
