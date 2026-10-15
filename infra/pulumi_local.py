"""Local stand-in for `pulumi up`.

The resource list uses the same type tokens a Pulumi AWS program would
submit (vpc, subnets, security group, RDS, S3, EFS, load balancer, ECS,
ECR, IAM) plus a Snowflake database and schema. Applying it writes a
file-backend state under the work directory and creates the local
directories those resources stand for.

No Pulumi Cloud login, AWS account, or Snowflake account is required.
A second apply updates the same URNs.
"""

from __future__ import annotations

import json
from pathlib import Path

PROJECT_NAME = "onecredit"
STACK_NAME = "local"

# Type tokens match the Pulumi AWS and Snowflake providers so the plan
# reads as the cloud program. The local engine never calls those APIs.
STACK_RESOURCES: list[dict] = [
    {
        "type": "aws:ec2/vpc:Vpc",
        "name": "onecredit-vpc",
        "props": {"cidr_block": "10.0.0.0/16"},
    },
    {
        "type": "aws:ec2/subnet:Subnet",
        "name": "public-a",
        "props": {"cidr_block": "10.0.1.0/24", "map_public_ip_on_launch": True},
    },
    {
        "type": "aws:ec2/subnet:Subnet",
        "name": "private-a",
        "props": {"cidr_block": "10.0.11.0/24", "map_public_ip_on_launch": False},
    },
    {
        "type": "aws:ec2/securityGroup:SecurityGroup",
        "name": "dagster-sg",
        "props": {"description": "Dagster webserver and daemon", "ingress_port": 3000},
    },
    {
        "type": "aws:rds/instance:Instance",
        "name": "dagster-db",
        "props": {"engine": "postgres", "instance_class": "db.t3.micro", "db_name": "dagsterdb"},
    },
    {
        "type": "aws:s3/bucket:Bucket",
        "name": "dagster-data-bucket",
        "props": {"versioning": True},
    },
    {
        "type": "aws:efs/fileSystem:FileSystem",
        "name": "dagster-efs",
        "props": {"encrypted": True},
    },
    {
        "type": "aws:lb/loadBalancer:LoadBalancer",
        "name": "dagster-alb",
        "props": {"load_balancer_type": "application", "internal": False},
    },
    {
        "type": "aws:ecs/cluster:Cluster",
        "name": "dagster-cluster",
        "props": {"capacity_providers": ["FARGATE"]},
    },
    {
        "type": "aws:ecr/repository:Repository",
        "name": "dagster-repo",
        "props": {"name": "dagster", "image_tag_mutability": "MUTABLE"},
    },
    {
        "type": "aws:iam/role:Role",
        "name": "dagster-execution-role",
        "props": {"assume_service": "ecs-tasks.amazonaws.com"},
    },
    {
        "type": "aws:iam/role:Role",
        "name": "dagster-task-role",
        "props": {"assume_service": "ecs-tasks.amazonaws.com"},
    },
    {
        "type": "snowflake:index/database:Database",
        "name": "DUMMY",
        "props": {"name": "DUMMY"},
    },
    {
        "type": "snowflake:index/schema:Schema",
        "name": "PUBLIC",
        "props": {"database": "DUMMY", "name": "PUBLIC"},
    },
]


def resource_urn(resource: dict) -> str:
    """Pulumi URN for one resource on the local stack."""
    return f"urn:pulumi:{STACK_NAME}::{PROJECT_NAME}::{resource['type']}::{resource['name']}"


def preview_local_stack() -> list[dict]:
    """The plan a `pulumi preview` would print. Nothing is written."""
    planned = []
    for resource in STACK_RESOURCES:
        planned.append(
            {
                "urn": resource_urn(resource),
                "type": resource["type"],
                "name": resource["name"],
                "props": resource["props"],
            }
        )
    return planned


def _materialise(root: Path) -> None:
    """Directories and an empty warehouse file for the resources that hold data.

    VPC, IAM, ECS, and the load balancer are records in the state file.
    The bucket, the database marker, the file system, and the warehouse
    file are the pieces a local ETL can actually open.
    """
    (root / "object_store" / "dagster-data-bucket").mkdir(parents=True, exist_ok=True)
    (root / "rds").mkdir(parents=True, exist_ok=True)
    (root / "rds" / "dagster-db.txt").write_text(
        "local stand-in for aws:rds/instance:Instance dagster-db engine=postgres\n",
        encoding="utf-8",
    )
    (root / "efs" / "dagster-efs").mkdir(parents=True, exist_ok=True)
    warehouse_dir = root / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    # Touch the file the Snowflake stand-in opens. DuckDB creates the
    # catalog on first connect; an empty file is enough for the state.
    warehouse_file = warehouse_dir / "DUMMY.duckdb"
    if not warehouse_file.exists():
        warehouse_file.write_bytes(b"")


def up_local_stack(root: Path) -> dict:
    """Apply the plan into `root/.pulumi/stacks/local.json`.

    This is the file backend. It does not call Pulumi Cloud or AWS.
    """
    root = Path(root)
    _materialise(root)
    state_dir = root / ".pulumi" / "stacks"
    state_dir.mkdir(parents=True, exist_ok=True)
    resources = preview_local_stack()
    state = {
        "backend": "file",
        "project": PROJECT_NAME,
        "stack": STACK_NAME,
        "cloud_login": False,
        "resources": resources,
    }
    state_path = state_dir / "local.json"
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return state
