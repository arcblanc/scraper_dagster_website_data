from dagster import ScheduleDefinition

from src.oc_etl.jobs.company_etl_job import company_etl_job

# minute hour day-of-month month day-of-week
# 0 0 * * * is midnight in Asia/Kuala_Lumpur.
daily_company_etl_schedule = ScheduleDefinition(
    job=company_etl_job,
    cron_schedule="0 0 * * *",
    execution_timezone="Asia/Kuala_Lumpur",
    name="daily_company_etl_schedule",
    description="Run the company ETL every midnight, Kuala Lumpur time.",
)
