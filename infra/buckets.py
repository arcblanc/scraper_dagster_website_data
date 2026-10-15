import pulumi
from pulumi_aws import s3

bucket_name = "onecredit-dagster-assets-dev"
dagster_data_bucket = s3.Bucket(
    "dagster_assets_bucket",
    bucket=bucket_name,
    versioning=s3.BucketVersioningArgs(enabled=True),
    acl="private",
)

compute_logs_bucket_name = "onecredit-dagster-compute-logs-dev"
dagster_compute_logs_bucket = s3.Bucket(
    "dagster_compute_logs_bucket",
    bucket=compute_logs_bucket_name,
    versioning=s3.BucketVersioningArgs(enabled=True),
    acl="private",
)

pulumi.export("dagster_assets_bucket_name", dagster_data_bucket.id)
pulumi.export("dagster_compute_logs_bucket_name", dagster_compute_logs_bucket.id)
