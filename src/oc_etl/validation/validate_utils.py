# src/oc_etl/validation/validate_utils.py

import pandas as pd
import pandera.pandas as pa


class DataValidationError(Exception):
    """Custom exception for validation errors."""


def validate_dataframe(df: pd.DataFrame, schema: pa.DataFrameSchema, name: str) -> pd.DataFrame:
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        msg = f"Validation failed for {name}:{e}"
        raise DataValidationError(msg) from e
