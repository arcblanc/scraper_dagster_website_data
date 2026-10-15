# src/oc_etl/ops/validate.py

import pandas as pd
from dagster import In, Out, op

from src.oc_etl.validation.schemas import company_profiles_schema
from src.oc_etl.validation.validate_utils import validate_dataframe


@op(
    ins={"raw_df": In()},
    out=Out(),
    description="Validates company_profiles data using Pandera schema.",
)
def validate_company_profiles(raw_df: pd.DataFrame) -> pd.DataFrame:
    raw_df["financial_year_end"] = pd.to_datetime(raw_df["financial_year_end"], errors="coerce")
    raw_df["incorporation_date"] = pd.to_datetime(raw_df["incorporation_date"], errors="coerce")
    # The schema types this column as a timestamp, same as the two dates above.
    raw_df["latest_doc_update_date"] = pd.to_datetime(raw_df["latest_doc_update_date"], errors="coerce")

    return validate_dataframe(raw_df, company_profiles_schema, "company_profiles")
