"""Warehouse load, with a local SQL stand-in for Snowflake.

A real Snowflake account is used only when SNOW_ACCOUNT is set to something
other than 'dummy'. The dummy path opens a DuckDB file and runs the same
three steps the live load runs: truncate, copy the parquet in, count the rows.

DuckDB does not accept Snowflake's COPY INTO ... FILE_FORMAT clause, so the
executed statements are the DuckDB form of those steps. The Snowflake text
is still built and kept on the warehouse as `last_snowflake_sql`, which is
what a test checks without an account.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
from dagster import resource


def qualified_snowflake_name(database: str, schema: str, table_name: str) -> str:
    """DUMMY.PUBLIC.company_profiles, the name a live COPY INTO would use."""
    return f"{database}.{schema}.{table_name}"


def snowflake_load_sql(
    database: str,
    schema: str,
    table_name: str,
    parquet_path: str,
    storage_integration: str,
    truncate: bool,
) -> list[str]:
    """The statements a Snowflake session would run for one parquet load.

    COPY INTO reads the parquet through a storage integration. MATCH_BY_COLUMN_NAME
    lines the file columns up with the table when the names differ in case.
    The count is the same number the asset returns as loaded_row_count.
    """
    full_table = qualified_snowflake_name(database, schema, table_name)
    statements: list[str] = []
    if truncate:
        statements.append(f"TRUNCATE TABLE {full_table}")
    statements.append(
        f"COPY INTO {full_table} "
        f"FROM '{parquet_path}' "
        f"STORAGE_INTEGRATION = {storage_integration} "
        "FILE_FORMAT = (TYPE = PARQUET) "
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
    )
    statements.append(f"SELECT COUNT(*) FROM {full_table}")
    return statements


def _quoted_local_table(schema: str, table_name: str) -> str:
    """Quote the local schema and table. PUBLIC is a DuckDB schema we create."""
    return f'"{schema}"."{table_name}"'


def local_load_sql(schema: str, table_name: str, truncate: bool, table_exists: bool) -> list[str]:
    """DuckDB statements that perform the Snowflake truncate / copy / count.

    CREATE TABLE ... AS SELECT ... WHERE FALSE builds the column types from
    the parquet without inserting rows. DELETE is not used: DuckDB accepts
    TRUNCATE TABLE, which is the same verb as the Snowflake statement.
    The copy itself is INSERT ... SELECT read_parquet, because DuckDB has
    no STORAGE_INTEGRATION clause.
    """
    local_table = _quoted_local_table(schema, table_name)
    statements = [f'CREATE SCHEMA IF NOT EXISTS "{schema}"']
    if not table_exists:
        statements.append(
            f"CREATE TABLE {local_table} AS "
            "SELECT * FROM read_parquet(?) WHERE FALSE"
        )
    if truncate and table_exists:
        statements.append(f"TRUNCATE TABLE {local_table}")
    statements.append(f"INSERT INTO {local_table} SELECT * FROM read_parquet(?)")
    statements.append(f"SELECT COUNT(*) FROM {local_table}")
    return statements


class LocalWarehouse:
    """DuckDB file standing in for the Snowflake database DUMMY.PUBLIC."""

    def __init__(self) -> None:
        self.loaded_frames: dict[str, pd.DataFrame] = {}
        self.last_snowflake_sql: list[str] = []
        self.last_local_sql: list[str] = []
        self.database_path = _warehouse_file()

    def load_local_parquet(self, parquet_path: str, table_name: str, truncate: bool) -> int:
        """Run the local copy and return SELECT COUNT(*)."""
        database = os.getenv("SNOW_DATABASE", "DUMMY")
        schema = os.getenv("SNOW_SCHEMA", "PUBLIC")
        storage_integration = os.getenv("SNOW_STORAGE_INTEGRATION", "LOCAL_OBJECT_STORE")
        self.last_snowflake_sql = snowflake_load_sql(
            database,
            schema,
            table_name,
            parquet_path,
            storage_integration,
            truncate,
        )

        connection = duckdb.connect(self.database_path)
        try:
            table_exists = _table_exists(connection, schema, table_name)
            self.last_local_sql = local_load_sql(schema, table_name, truncate, table_exists)
            row_count = _execute_local_load(connection, self.last_local_sql, parquet_path)
            # The frame is the table the COUNT just read, including an append.
            local_table = _quoted_local_table(schema, table_name)
            self.loaded_frames[table_name] = connection.execute(
                f"SELECT * FROM {local_table}"
            ).df()
        finally:
            connection.close()

        return row_count


class SnowflakeWarehouse:
    """Live connector. Constructed only when SNOW_ACCOUNT is not dummy."""

    def __init__(self) -> None:
        import snowflake.connector

        self._connection = snowflake.connector.connect(
            user=os.environ["SNOW_USER"],
            password=os.environ["SNOW_PWD"],
            account=os.environ["SNOW_ACCOUNT"],
            warehouse=os.environ["SNOW_WAREHOUSE"],
            database=os.environ["SNOW_DATABASE"],
            schema=os.environ["SNOW_SCHEMA"],
            role=os.getenv("SNOW_ROLE"),
        )
        self.last_snowflake_sql: list[str] = []

    def load_local_parquet(self, parquet_path: str, table_name: str, truncate: bool) -> int:
        database_name = os.environ["SNOW_DATABASE"]
        schema_name = os.environ["SNOW_SCHEMA"]
        storage_integration = os.environ["SNOW_STORAGE_INTEGRATION"]
        self.last_snowflake_sql = snowflake_load_sql(
            database_name,
            schema_name,
            table_name,
            parquet_path,
            storage_integration,
            truncate,
        )
        cursor = self._connection.cursor()
        # The count statement is a query. The two statements before it are DDL.
        for statement in self.last_snowflake_sql[:-1]:
            cursor.execute(statement)
        cursor.execute(self.last_snowflake_sql[-1])
        row = cursor.fetchone()
        return int(row[0])


def _warehouse_file() -> str:
    """Path of the local DuckDB catalog, or :memory: when no stack root is set.

    An empty placeholder file (written so the path exists before DuckDB
    opens it) is removed. DuckDB refuses to open a zero-byte file as a catalog.
    """
    root = os.getenv("OC_ETL_LOCAL_ROOT")
    if not root:
        return ":memory:"
    path = Path(root) / "warehouse" / "DUMMY.duckdb"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size == 0:
        path.unlink()
    return str(path)


def _table_exists(connection: duckdb.DuckDBPyConnection, schema: str, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table_name],
    ).fetchone()
    return int(row[0]) > 0


def _execute_local_load(
    connection: duckdb.DuckDBPyConnection,
    statements: list[str],
    parquet_path: str,
) -> int:
    """Run each local statement. read_parquet binds the path as a parameter."""
    row_count = 0
    for statement in statements:
        if "read_parquet" in statement:
            result = connection.execute(statement, [parquet_path])
        else:
            result = connection.execute(statement)
        if statement.startswith("SELECT COUNT"):
            row_count = int(result.fetchone()[0])
    return row_count


@resource
def snowflake_resource(_context) -> LocalWarehouse | SnowflakeWarehouse:
    account = os.getenv("SNOW_ACCOUNT", "dummy")
    if account == "dummy":
        return LocalWarehouse()
    return SnowflakeWarehouse()
