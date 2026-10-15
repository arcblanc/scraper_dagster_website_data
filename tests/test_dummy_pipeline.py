"""Dummy run of the recreated company ETL and hearing parse.

Nothing here opens Postgres, S3, or Snowflake. The company table is the
built-in sample (one row repeated). The hearing page is a small HTML
string written into the local store.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster import materialize

from infra.local_stack import build_local_stack
from src.oc_etl.assets import (
    cleaned_parquet_df,
    company_raw_assets,
    company_sanitize_assets,
    company_snow_assets,
    parsed_from_html,
)
from src.oc_etl.io_managers import s3_parquet_io_manager
from src.oc_etl.jobs.company_etl_job import company_etl_job
from src.oc_etl.resources import make_pg_resource, make_s3_resource, snowflake_resource
from src.oc_etl.schedules import daily_company_etl_schedule

HEARING_PAGE = """
<div class="divPart">
  <span id="ctl00_LabelProceedingName">Civil</span>
  <div class="caption text">
    <span id="x_LabelPIC">Judge Example</span>
    <span id="x_LabelDate">2024-01-15</span>
    <span id="x_LabelDay">Monday</span>
    <span id="x_LabelProceedingStartTime">09:00</span>
    <span id="x_LabelJurisdiction">High Court</span>
    <span id="x_LabelDivision">Commercial</span>
    <span id="x_LabelVenue">Court 2</span>
    <span id="x_LabelCourtLocation">Kuala Lumpur</span>
  </div>
  <table>
    <tr><th>No. Kes</th></tr>
    <tr><td>WA-22NCC-1-01/2024</td></tr>
    <tr><td>WA-22NCC-1-01/2024</td></tr>
  </table>
</div>
"""


def test_local_stack_names_the_dummy_services(tmp_path: Path) -> None:
    plan = build_local_stack(tmp_path)
    assert plan["PG_RECC_DSN"] == "dummy://company_profiles"
    assert plan["SNOW_ACCOUNT"] == "dummy"
    assert plan["COMPANY_ETL_CRON"] == "0 0 * * *"
    assert Path(plan["OC_ETL_LOCAL_ROOT"]).is_dir()


def test_company_and_hearing_pipeline_on_dummy_data(tmp_path: Path) -> None:
    plan = build_local_stack(tmp_path)
    for key, value in plan.items():
        os.environ[key] = value

    store_root = Path(plan["OC_ETL_LOCAL_ROOT"])
    hearing_path = store_root / "raw" / "html" / "hearing.html"
    hearing_path.parent.mkdir(parents=True, exist_ok=True)
    hearing_path.write_text(HEARING_PAGE, encoding="utf-8")

    assert company_etl_job.name == "company_etl_job"
    assert daily_company_etl_schedule.cron_schedule == "0 0 * * *"

    result = materialize(
        [
            *company_raw_assets,
            *company_sanitize_assets,
            *company_snow_assets,
            parsed_from_html,
            cleaned_parquet_df,
        ],
        resources={
            "pg_recc": make_pg_resource("PG_RECC_DSN"),
            "s3": make_s3_resource(),
            "s3_io_manager": s3_parquet_io_manager.configured(
                {"s3_bucket": "local", "base_path": "data"}
            ),
            "s3_litigation_io_manager": s3_parquet_io_manager.configured(
                {"s3_bucket": "local", "base_path": "litigation"}
            ),
            "snowflake_resource": snowflake_resource,
        },
        partition_key="2025-07-01",
    )

    assert result.success

    # 3 sample profiles, one of them a copy, so the warehouse keeps 2.
    # Dagster names a prefixed asset by joining the key with double underscores.
    loaded_row_count = result.output_for_node(
        "upload_to_snowflake__company__pg_recc__public__upload_to_snow_company_profiles"
    )
    assert loaded_row_count == 2

    # Two identical hearing rows collapse to the one case number.
    cleaned = result.output_for_node("cleaned_parquet_df")
    assert list(cleaned["case_number"]) == ["WA-22NCC-1-01/2024"]
    assert cleaned.loc[0, "judge_name"] == "Judge Example"
    assert cleaned.loc[0, "court_level"] == "High Court Commercial"


def test_recreated_shell_does_not_name_the_other_contributors() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    blocked_names = ("cass" + "chwee", "nazim" + "-ashman", "nazim" + "@onecredit")
    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        if ".git" in path.parts or ".venv" in path.parts or "__pycache__" in path.parts:
            continue
        if path.suffix not in {".py", ".md", ".yml", ".yaml", ".toml", ".txt"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore").lower()
        for blocked in blocked_names:
            assert blocked not in text, f"{blocked} found in {path}"
