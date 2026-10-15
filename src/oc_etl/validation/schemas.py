import pandera.pandas as pa
from pandera import Column
from pandera.dtypes import DateTime, Int64

company_profiles_schema = pa.DataFrameSchema(
    {
        "registration_number": Column(str, nullable=True),
        "registration_number_old": Column(str, nullable=True),
        "company_name": Column(str, nullable=True),
        "company_type": Column(str, nullable=True),
        "business_sector": Column(str, nullable=True),
        "msic_code": Column(str, nullable=True),
        "financial_year_end": Column(DateTime, nullable=True),
        "latest_doc_update_date": Column(pa.Timestamp, nullable=True),
        "website": Column(str, nullable=True),
        "naic_codes": Column(str, nullable=True),
        "logo_url": Column(str, nullable=True),
        "incorporation_date": Column(DateTime, nullable=True),
        "company_status_id": Column(Int64, nullable=True),  # Pandera uses float for numeric, or use Int if no nulls
        "state": Column(str, nullable=True),
        "phone_number": Column(str, nullable=True),
        "total_watchers": Column(Int64, nullable=True),
    },
)
