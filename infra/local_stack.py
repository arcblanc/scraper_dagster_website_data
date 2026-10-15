"""Local stand-in for the deployment shell.

The live cloud stack is not recreated here. This plan is the set of
directories and environment values a dummy run needs: an object store,
a dummy company database, and a dummy warehouse.
"""

from __future__ import annotations

from pathlib import Path


def build_local_stack(root: Path) -> dict[str, str]:
    store_root = root / "object_store"
    store_root.mkdir(parents=True, exist_ok=True)
    return {
        "OC_ETL_LOCAL_ROOT": str(store_root),
        "PG_RECC_DSN": "dummy://company_profiles",
        "PG_PORTAL_DSN": "dummy://company_profiles",
        "S3_BUCKET": "local",
        "SNOW_ACCOUNT": "dummy",
        "SNOW_DATABASE": "DUMMY",
        "SNOW_SCHEMA": "PUBLIC",
        "SNOW_TRUNCATE": "true",
        "COMPANY_ETL_CRON": "0 0 * * *",
        "COMPANY_ETL_TIMEZONE": "Asia/Kuala_Lumpur",
    }
