import os

import snowflake.connector
from dagster import InitResourceContext, resource


@resource
def snowflake_resource(context: InitResourceContext) -> snowflake.connector.SnowflakeConnection:
    conn = snowflake.connector.connect(
        user=os.getenv("SNOW_USER"),
        password=os.getenv("SNOW_PWD"),
        account=os.getenv("SNOW_ACCOUNT"),
        warehouse=os.getenv("SNOW_WAREHOUSE"),
        database=os.getenv("SNOW_DATABASE"),
        schema=os.getenv("SNOW_SCHEMA"),
        role=os.getenv("SNOW_ROLE"),
    )

    # Debug print: confirm active role inside Dagster
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_ROLE();")
    current_role = cursor.fetchone()[0]
    msg = "Connected to Snowflake with role: " + current_role
    context.log.info(msg)

    return conn
