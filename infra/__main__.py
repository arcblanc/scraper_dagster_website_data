import pulumi
import pulumi_awsx as awsx
import pulumi_aws as aws
from pulumi import ResourceOptions
from iam import get_dagster_roles
from buckets import compute_logs_bucket_name, dagster_data_bucket  # noqa: F401

from services import (
    create_webserver_service,
    create_daemon_service,
    create_code_server_service,
)
from security_groups import dagster_sg
from config_infra import (
    vpc_id,
    public_subnet_ids,
    private_subnet_ids,
    db_password,
)
from rds import rds_instance
from efs import efs, access_point

from service_discovery import (
    webserver_sd_service,
    daemon_sd_service,
    codeserver_sd_service,
)

# ECS Cluster
ecs_cluster = aws.ecs.Cluster("dagster-cluster")

# ECR Repository
ecr_repo = awsx.ecr.Repository(
    "dagster-repo",
    name="dagster",
    lifecycle_policy={
        "rules": [
            {
                "tag_status": awsx.ecr.LifecycleTagStatus.ANY,
                "description": "Keep last 5 images",
                "maximum_number_of_images": 5,
            }
        ]
    },
)

# IAM Roles
roles = get_dagster_roles()
task_execution_role_arn = roles["execution_role_arn"]
task_role_arn = roles["task_role_arn"]

# Docker Image Build
dagster_image = awsx.ecr.Image(
    "dagster-image",
    repository_url=ecr_repo.url,
    context="..",
    dockerfile="../Dockerfile.prod",
    platform="linux/amd64",
    opts=ResourceOptions(depends_on=[ecr_repo]),
)

pg_url = pulumi.Output.concat(
    "postgresql://dagster_user:",
    db_password,
    "@",
    rds_instance.address,
    ":5432/dagsterdb",
)

# ALB
alb = awsx.lb.ApplicationLoadBalancer(
    "dagster-alb",
    subnet_ids=public_subnet_ids,
    security_groups=[dagster_sg.id],
    default_target_group={
        "vpc_id": vpc_id,
        "protocol": "HTTP",
        "port": 3000,
        "health_check": {
            "enabled": True,
            "healthy_threshold": 2,
            "interval": 100,
            "matcher": "200",
            "path": "/",
            "port": "traffic-port",
            "protocol": "HTTP",
            "timeout": 10,
            "unhealthy_threshold": 2,
        },
    },
)

# Log Groups
webserver_log_group = aws.cloudwatch.LogGroup(
    "dagster-webserver-logs", name="/ecs/dagster-webserver"
)
daemon_log_group = aws.cloudwatch.LogGroup(
    "dagster-daemon-logs", name="/ecs/dagster-daemon"
)
code_server_log_group = aws.cloudwatch.LogGroup(
    "dagster-codeserver-logs", name="/ecs/dagster-codeserver"
)

# ECS Services
webserver = create_webserver_service(
    name="dagster",
    image_uri=dagster_image.image_uri,
    cluster=ecs_cluster,
    private_subnet_ids=private_subnet_ids,
    ecs_sg_id=dagster_sg.id,
    alb_target_group_arn=alb.default_target_group.arn,
    log_group_name=webserver_log_group.name,
    pg_url=pg_url,
    execution_role_arn=task_execution_role_arn,
    task_role_arn=task_role_arn,
    efs_id=efs.id,
    access_point_id=access_point.id,
    registry_arn=webserver_sd_service.arn,
)

daemon = create_daemon_service(
    name="dagster",
    image_uri=dagster_image.image_uri,
    cluster=ecs_cluster,
    private_subnet_ids=private_subnet_ids,
    ecs_sg_id=dagster_sg.id,
    log_group_name=daemon_log_group.name,
    pg_url=pg_url,
    execution_role_arn=task_execution_role_arn,
    task_role_arn=task_role_arn,
    efs_id=efs.id,
    access_point_id=access_point.id,
    registry_arn=daemon_sd_service.arn,
)

code_server = create_code_server_service(
    name="dagster",
    image_uri=dagster_image.image_uri,
    cluster=ecs_cluster,
    private_subnet_ids=private_subnet_ids,
    ecs_sg_id=dagster_sg.id,
    log_group_name=code_server_log_group.name,
    pg_url=pg_url,
    execution_role_arn=task_execution_role_arn,
    task_role_arn=task_role_arn,
    efs_id=efs.id,
    access_point_id=access_point.id,
    registry_arn=codeserver_sd_service.arn,
)

# Outputs
pulumi.export("ecsClusterName", ecs_cluster.name)
pulumi.export("dagsterImageUri", dagster_image.image_uri)
pulumi.export("albDnsName", alb.load_balancer.dns_name)
pulumi.export("albUrl", pulumi.Output.concat("http://", alb.load_balancer.dns_name))
pulumi.export("rdsEndpoint", rds_instance.address)
pulumi.export("rdsPort", rds_instance.port)
pulumi.export("rdsDbName", "dagsterdb")
pulumi.export("efsId", efs.id)
pulumi.export("efsAccessPointId", access_point.id)
pulumi.export("taskExecutionRoleArn", task_execution_role_arn)
pulumi.export("taskRoleArn", task_role_arn)
pulumi.export("ecrRepoUrl", ecr_repo.url)
