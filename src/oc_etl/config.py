"""Tables the company ETL is allowed to pull.

One row here becomes three assets: extract, sanitize, warehouse load.
The key path is raw|sanitized|upload_to_snowflake / domain / database / schema / table.
"""

TABLE_CONFIG = [
    {
        "domain": "company",
        "db_type": "postgres",
        "database": "pg_recc",
        "schema": "public",
        "table": "company_profiles",
        "description": "Raw company profiles from pg_recc database",
    },
]
