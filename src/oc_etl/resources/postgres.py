"""Postgres access, with a built-in company table for a local run.

A DSN of dummy://company_profiles never opens a server. It returns the
sample frame, including one repeated row so the sanitize step has a
duplicate to drop. A postgresql:// DSN uses SQLAlchemy.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
from dagster import resource


def sample_company_profiles() -> pd.DataFrame:
    """Three profile rows. The first row is repeated on purpose."""
    first_company = {
        "registration_number": "202001000001",
        "registration_number_old": "",
        "company_name": "Example Manufacturing Sdn Bhd",
        "company_type": "Sdn Bhd",
        "business_sector": "Manufacturing",
        "msic_code": "25110",
        "financial_year_end": "2024-12-31",
        "latest_doc_update_date": "2025-01-10",
        "website": "https://example.my",
        "naic_codes": "",
        "logo_url": "",
        "incorporation_date": "2020-01-15",
        "company_status_id": 1,
        "state": "Kuala Lumpur",
        "phone_number": "0312345678",
        "total_watchers": 2,
    }
    second_company = dict(first_company)
    second_company["registration_number"] = "202001000002"
    second_company["company_name"] = "Example Logistics Sdn Bhd"
    second_company["business_sector"] = "Logistics"
    return pd.DataFrame([first_company, first_company, second_company])


class CompanyTableClient:
    """fetch_df is the only method the extract asset calls."""

    def __init__(self, connection_url: str) -> None:
        self.connection_url = connection_url
        self._engine = None

    def fetch_df(self, query: str, params: tuple | None = None) -> pd.DataFrame:
        """Return rows for a SELECT. The dummy URL ignores the SQL text."""
        row_limit = params[0] if params else None
        if self.connection_url.startswith("dummy://"):
            frame = sample_company_profiles()
            if row_limit is None:
                return frame
            return frame.head(int(row_limit))

        if self._engine is None:
            from sqlalchemy import create_engine

            self._engine = create_engine(self.connection_url)

        with self._engine.connect() as connection:
            return pd.read_sql(query, connection, params=params)


def make_pg_resource(dsn_env_var: str):
    """Dagster resource. The DSN is read when the run starts, not at import."""

    @resource
    def postgres_resource(_context: Any) -> CompanyTableClient:
        connection_url = os.getenv(dsn_env_var)
        if not connection_url:
            message = f"Environment variable {dsn_env_var} is not set."
            raise ValueError(message)
        return CompanyTableClient(connection_url)

    return postgres_resource
