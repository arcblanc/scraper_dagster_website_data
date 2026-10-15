"""Local Pulumi apply and the Snowflake SQL stand-in.

Neither test contacts Pulumi Cloud, AWS, or Snowflake. The Pulumi test
writes a file-backend state. The warehouse test runs DuckDB statements
that stand in for TRUNCATE, COPY INTO, and SELECT COUNT(*).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from infra.pulumi_local import preview_local_stack, up_local_stack
from src.oc_etl.resources.warehouse import LocalWarehouse, snowflake_load_sql


def test_local_pulumi_up_needs_no_cloud_account(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("PULUMI_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    plan = preview_local_stack()
    assert any(resource["type"] == "aws:ecs/cluster:Cluster" for resource in plan)
    assert any(resource["type"] == "snowflake:index/database:Database" for resource in plan)
    assert all(resource["urn"].startswith("urn:pulumi:local::onecredit::") for resource in plan)

    first = up_local_stack(tmp_path)
    second = up_local_stack(tmp_path)

    assert first["backend"] == "file"
    assert first["cloud_login"] is False
    assert len(second["resources"]) == len(first["resources"])
    assert (tmp_path / ".pulumi" / "stacks" / "local.json").is_file()
    assert (tmp_path / "object_store" / "dagster-data-bucket").is_dir()
    assert (tmp_path / "rds" / "dagster-db.txt").is_file()
    assert "postgres" in (tmp_path / "rds" / "dagster-db.txt").read_text(encoding="utf-8")


def test_local_snowflake_sql_truncate_copy_and_count(tmp_path: Path, monkeypatch) -> None:
    parquet_path = tmp_path / "company_profiles.parquet"
    pd.DataFrame(
        {
            "registration_number": ["202001000001", "202001000002"],
            "company_name": ["Example Manufacturing Sdn Bhd", "Example Logistics Sdn Bhd"],
        }
    ).to_parquet(parquet_path, index=False)

    snowflake_sql = snowflake_load_sql(
        "DUMMY",
        "PUBLIC",
        "company_profiles",
        str(parquet_path),
        "LOCAL_OBJECT_STORE",
        truncate=True,
    )
    assert any(statement.startswith("TRUNCATE TABLE DUMMY.PUBLIC.company_profiles") for statement in snowflake_sql)
    assert any("COPY INTO DUMMY.PUBLIC.company_profiles" in statement for statement in snowflake_sql)
    assert any("FILE_FORMAT = (TYPE = PARQUET)" in statement for statement in snowflake_sql)
    assert snowflake_sql[-1] == "SELECT COUNT(*) FROM DUMMY.PUBLIC.company_profiles"

    monkeypatch.setenv("OC_ETL_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("SNOW_ACCOUNT", "dummy")
    monkeypatch.setenv("SNOW_DATABASE", "DUMMY")
    monkeypatch.setenv("SNOW_SCHEMA", "PUBLIC")

    warehouse = LocalWarehouse()
    first_count = warehouse.load_local_parquet(str(parquet_path), "company_profiles", truncate=True)
    second_count = warehouse.load_local_parquet(str(parquet_path), "company_profiles", truncate=True)
    assert any("TRUNCATE TABLE" in statement for statement in warehouse.last_snowflake_sql)
    appended_count = warehouse.load_local_parquet(str(parquet_path), "company_profiles", truncate=False)

    assert first_count == 2
    assert second_count == 2
    assert appended_count == 4
    assert any(statement.startswith("SELECT COUNT(*)") for statement in warehouse.last_local_sql)
    assert (tmp_path / "warehouse" / "DUMMY.duckdb").is_file()
