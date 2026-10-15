# s3.py
import os

import boto3
import s3fs
from dagster import InitResourceContext, resource

DEFAULT_ACCESS_KEY_ENV = "AWS_ACCESS_KEY_ID"
AWS_SECRET_KEY_ENV_VAR_NAME = "AWS_SECRET_ACCESS_KEY"  # noqa: S105


def make_s3_resource(
    access_key_env: str = DEFAULT_ACCESS_KEY_ENV,
    secret_key_env: str = AWS_SECRET_KEY_ENV_VAR_NAME,
) -> resource:
    """Factory to create a Dagster S3 resource using environment variables."""

    @resource
    def _s3_resource(_context: InitResourceContext) -> boto3.client:
        access_key = os.getenv(access_key_env)
        secret_key = os.getenv(secret_key_env)

        if not access_key or not secret_key:
            msg = f"Missing AWS credentials. Ensure {access_key_env} and {secret_key_env} are set in the environment."
            raise OSError(msg)

        return s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
        )

    return _s3_resource
