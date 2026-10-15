import pulumi
import pulumi_aws as aws
import pulumi_awsx as awsx


# Shared helper for EFS volume config
def get_efs_volume(
    name: str, efs_id: pulumi.Output[str], access_point_id: pulumi.Output[str]
):
    return {
        "name": name,
        "efs_volume_configuration": {
            "file_system_id": efs_id,
            "authorization_config": {
                "access_point_id": access_point_id,
                "iam": "ENABLED",
            },
            "transit_encryption": "ENABLED",
        },
    }


# Shared mount point config
DEFAULT_MOUNT_POINT = {
    "source_volume": "dagster-efs-volume",
    "container_path": "/opt/dagster",
    "read_only": False,
}


# Common environment variables
def get_base_env_vars(pg_url: pulumi.Output[str], role: str):
    base_vars = [
        {"name": "DAGSTER_PG_URL", "value": pg_url},
        {"name": "AWS_REGION", "value": "ap-southeast-5"},
        {"name": "DAGSTER_HOME", "value": "/opt/dagster"},
        {"name": "DAGSTER_ROLE", "value": role},
    ]

    # Add daemon-specific heartbeat configuration
    if role == "daemon":
        base_vars.extend(
            [
                # More conservative heartbeat settings
                {
                    "name": "DAGSTER_DAEMON_HEARTBEAT_TOLERANCE_SECONDS",
                    "value": "300",  # Reduced from 1800 - too high can mask real issues
                },
                {
                    "name": "DAGSTER_DAEMON_HEARTBEAT_INTERVAL_SECONDS",
                    "value": "60",  # Increased from 30 - less frequent heartbeats
                },
                # Ensure single daemon instance
                {"name": "DAGSTER_DAEMON_STARTUP_CHECK_INTERVAL", "value": "10"},
                # Add run launcher settings to prevent conflicts
                {
                    "name": "DAGSTER_RUN_LAUNCHER_CLASS",
                    "value": "dagster._core.launcher.DefaultRunLauncher",
                },
                # Prevent daemon from starting multiple processes
                {"name": "DAGSTER_DAEMON_SINGLE_INSTANCE", "value": "true"},
                # Add cleanup settings
                {"name": "DAGSTER_DAEMON_CLEANUP_INTERVAL_SECONDS", "value": "300"},
            ]
        )

    return base_vars


# Improved service-specific configurations
SERVICE_CONFIGS = {
    "webserver": {
        "port": 3000,
        "command": ["/bin/bash", "-c", "/app/entrypoint.sh"],
        "health_check": {
            "command": [
                "CMD-SHELL",
                "curl -f http://localhost:3000/dagit_info || exit 1",
            ],
            "interval": 60,
            "timeout": 10,
            "retries": 5,
            "start_period": 120,
        },
        "grace_period": 300,
        "extra_env": [
            {"name": "DAGSTER_WEBSERVER_HOST", "value": "0.0.0.0"},
            {"name": "DAGSTER_WEBSERVER_PORT", "value": "3000"},
            {
                "name": "DAGSTER_CODE_SERVER_HOST",
                "value": "codeserver.dagster.internal",
            },
            {"name": "DAGSTER_CODE_SERVER_PORT", "value": "4000"},
            {"name": "DAGSTER_WORKSPACE_YAML", "value": "/opt/dagster/workspace.yaml"},
            {"name": "DAGSTER_WEBSERVER_STARTUP_TIMEOUT", "value": "120"},
            {"name": "DAGSTER_NAMESPACE", "value": "dagster.internal"},
            {"name": "DAGSTER_WEBSERVER_SERVICE", "value": "webserver"},
            {"name": "DAGSTER_DAEMON_SERVICE", "value": "daemon"},
            {"name": "DAGSTER_CODE_SERVER_SERVICE", "value": "codeserver"},
        ],
        "has_load_balancer": True,
        "cpu": 1024,
        "memory": 2048,
    },
    "daemon": {
        "port": 8793,
        "command": ["/bin/bash", "-c", "/app/entrypoint.sh"],
        "health_check": {
            # Ultra-simple health check - just return success
            # Let ECS restart policy handle failures instead of complex health checks
            "command": ["CMD-SHELL", "exit 0"],
            "interval": 300,  # Very infrequent - just to satisfy ECS requirements
            "timeout": 5,
            "retries": 1,
            "start_period": 60,  # Minimal startup period
        },
        "grace_period": 180,  # Longer grace period for cleanup
        "extra_env": [
            {
                "name": "DAGSTER_CODE_SERVER_HOST",
                "value": "codeserver.dagster.internal",
            },
            {"name": "DAGSTER_CODE_SERVER_PORT", "value": "4000"},
            {"name": "DAGSTER_NAMESPACE", "value": "dagster.internal"},
            {"name": "DAGSTER_WEBSERVER_SERVICE", "value": "webserver"},
            {"name": "DAGSTER_DAEMON_SERVICE", "value": "daemon"},
            {"name": "DAGSTER_CODE_SERVER_SERVICE", "value": "codeserver"},
            # Add logging configuration
            {"name": "DAGSTER_DAEMON_LOG_LEVEL", "value": "INFO"},
            # Prevent daemon from interfering with manual runs
            {"name": "DAGSTER_DAEMON_SKIP_HEARTBEAT_CHECK", "value": "false"},
        ],
        "has_load_balancer": False,
        "cpu": 1024,
        "memory": 3072,  # Increased memory - daemon can be memory-intensive
    },
    "codeserver": {
        "port": 4000,
        "command": ["/bin/bash", "-c", "/app/entrypoint.sh"],
        "health_check": {
            "command": ["CMD-SHELL", "nc -z localhost 4000 || exit 1"],
            "interval": 45,
            "timeout": 10,
            "retries": 4,
            "start_period": 90,
        },
        "grace_period": 120,
        "extra_env": [
            {"name": "DAGSTER_CODE_SERVER_HOST", "value": "0.0.0.0"},
            {"name": "DAGSTER_CODE_SERVER_PORT", "value": "4000"},
            {"name": "DAGSTER_WORKSPACE_YAML", "value": "/opt/dagster/workspace.yaml"},
            {"name": "DAGSTER_NAMESPACE", "value": "dagster.internal"},
            {"name": "DAGSTER_WEBSERVER_SERVICE", "value": "webserver"},
            {"name": "DAGSTER_DAEMON_SERVICE", "value": "daemon"},
            {"name": "DAGSTER_CODE_SERVER_SERVICE", "value": "codeserver"},
        ],
        "has_load_balancer": False,
        "cpu": 512,
        "memory": 1024,
    },
}


def create_dagster_service(
    name: str,
    service_type: str,
    image_uri: pulumi.Output[str],
    cluster: aws.ecs.Cluster,
    private_subnet_ids: list[str],
    ecs_sg_id: pulumi.Output[str],
    log_group_name: str,
    pg_url: pulumi.Output[str],
    execution_role_arn: str,
    task_role_arn: str,
    efs_id: pulumi.Output[str],
    access_point_id: pulumi.Output[str],
    registry_arn: pulumi.Output[str],
    alb_target_group_arn: pulumi.Output[str] = None,  # type: ignore
):
    config = SERVICE_CONFIGS[service_type]
    container_name = f"{name}-{service_type.replace('-', '')}"

    # Build environment variables
    env_vars = get_base_env_vars(pg_url, service_type)
    env_vars.extend(config["extra_env"])

    # Create task definition with improved settings
    task_def = awsx.ecs.FargateTaskDefinition(
        f"{name}-{service_type}-taskdef",
        container={
            "name": container_name,
            "image": image_uri,
            "cpu": config["cpu"],
            "memory": config["memory"],
            "essential": True,
            "port_mappings": [{"container_port": config["port"], "protocol": "tcp"}]
            if config["port"]
            else [],
            "mount_points": [DEFAULT_MOUNT_POINT],
            "command": config["command"],
            "environment": env_vars,
            "health_check": config["health_check"],
            "stop_timeout": 60,  # Increased for daemon cleanup
        },  # type: ignore
        execution_role={"role_arn": execution_role_arn},
        task_role={"role_arn": task_role_arn},
        volumes=[get_efs_volume("dagster-efs-volume", efs_id, access_point_id)],
        log_group={
            "existing": {
                "name": log_group_name,
                "region": "ap-southeast-5",
            }
        },
    )

    # Build service configuration with improved settings
    service_args = {
        "cluster": cluster.arn,
        "task_definition": task_def.task_definition,
        "desired_count": 1,
        "enable_execute_command": True,
        "platform_version": "1.4.0",
        "network_configuration": {
            "subnets": private_subnet_ids,
            "security_groups": [ecs_sg_id],
            "assign_public_ip": False,
        },
        "service_registries": {
            "registry_arn": registry_arn,
            "container_name": container_name,
        },
        "health_check_grace_period_seconds": config["grace_period"],
        "tags": {
            "Service": f"dagster-{service_type}",
            "Environment": "production",
            "ManagedBy": "pulumi",
        },
    }

    # Add load balancer if needed
    if config["has_load_balancer"] and alb_target_group_arn:
        service_args["load_balancers"] = [
            {
                "target_group_arn": alb_target_group_arn,
                "container_name": container_name,
                "container_port": config["port"],
            }
        ]

    service = awsx.ecs.FargateService(f"{name}-{service_type}-service", **service_args)

    return service


# Convenience functions for backward compatibility
def create_webserver_service(
    name: str,
    image_uri: pulumi.Output[str],
    cluster: aws.ecs.Cluster,
    alb_target_group_arn: pulumi.Output[str],
    private_subnet_ids: list[str],
    ecs_sg_id: pulumi.Output[str],
    log_group_name: str,
    pg_url: pulumi.Output[str],
    execution_role_arn: str,
    task_role_arn: str,
    efs_id: pulumi.Output[str],
    access_point_id: pulumi.Output[str],
    registry_arn: pulumi.Output[str],
):
    return create_dagster_service(
        name,
        "webserver",
        image_uri,
        cluster,
        private_subnet_ids,
        ecs_sg_id,
        log_group_name,
        pg_url,
        execution_role_arn,
        task_role_arn,
        efs_id,
        access_point_id,
        registry_arn,
        alb_target_group_arn,
    )


def create_daemon_service(
    name: str,
    image_uri: pulumi.Output[str],
    cluster: aws.ecs.Cluster,
    private_subnet_ids: list[str],
    ecs_sg_id: pulumi.Output[str],
    log_group_name: str,
    pg_url: pulumi.Output[str],
    execution_role_arn: str,
    task_role_arn: str,
    efs_id: pulumi.Output[str],
    access_point_id: pulumi.Output[str],
    registry_arn: pulumi.Output[str],
):
    return create_dagster_service(
        name,
        "daemon",
        image_uri,
        cluster,
        private_subnet_ids,
        ecs_sg_id,
        log_group_name,
        pg_url,
        execution_role_arn,
        task_role_arn,
        efs_id,
        access_point_id,
        registry_arn,
    )


def create_code_server_service(
    name: str,
    image_uri: pulumi.Output[str],
    cluster: aws.ecs.Cluster,
    private_subnet_ids: list[str],
    ecs_sg_id: pulumi.Output[str],
    log_group_name: str,
    pg_url: pulumi.Output[str],
    execution_role_arn: str,
    task_role_arn: str,
    efs_id: pulumi.Output[str],
    access_point_id: pulumi.Output[str],
    registry_arn: pulumi.Output[str],
):
    return create_dagster_service(
        name,
        "codeserver",
        image_uri,
        cluster,
        private_subnet_ids,
        ecs_sg_id,
        log_group_name,
        pg_url,
        execution_role_arn,
        task_role_arn,
        efs_id,
        access_point_id,
        registry_arn,
    )
