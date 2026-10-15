import pulumi
import pulumi_aws as aws

# Reusable constants
PROJECT_TAG = {"Project": "dagster"}
REGION = aws.config.region

# Trust policy for ECS tasks
assume_role_policy_doc = aws.iam.get_policy_document(
    statements=[
        {
            "effect": "Allow",
            "actions": ["sts:AssumeRole"],
            "principals": [
                {
                    "type": "Service",
                    "identifiers": ["ecs-tasks.amazonaws.com"],
                },
            ],
        }
    ]
)

# --- Task Execution Role (for ECS infra: pull images, write logs, etc.) ---
dagster_execution_role = aws.iam.Role(
    "dagsterExecutionRole",
    assume_role_policy=assume_role_policy_doc.json,
    tags={**PROJECT_TAG, "Name": "dagster-execution-role"},
)

aws.iam.RolePolicyAttachment(
    "dagsterExecutionPolicy",
    role=dagster_execution_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
)

# --- Task Role (for Dagster app permissions: EFS, S3, Logs, etc.) ---
dagster_task_role = aws.iam.Role(
    "dagsterTaskRole",
    assume_role_policy=assume_role_policy_doc.json,
    tags={**PROJECT_TAG, "Name": "dagster-task-role"},
)

# SSM (for ECS Exec)
aws.iam.RolePolicyAttachment(
    "dagsterTaskSSMExecPolicy",
    role=dagster_task_role.name,
    policy_arn="arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
)

# --- EFS Policy (limit to region-wide resources) ---
efs_policy_doc = aws.iam.get_policy_document(
    statements=[
        {
            "effect": "Allow",
            "actions": [
                "elasticfilesystem:ClientMount",
                "elasticfilesystem:ClientWrite",
                "elasticfilesystem:ClientRootAccess",
            ],
            "resources": ["*"],  # Optionally restrict to known EFS ARNs
        },
        {
            "effect": "Allow",
            "actions": [
                "elasticfilesystem:DescribeFileSystems",
                "elasticfilesystem:DescribeAccessPoints",
            ],
            "resources": ["*"],  # Required for client connection setup
        },
    ]
)

efs_policy = aws.iam.Policy(
    "dagsterEFSPolicy",
    description="Policy for Dagster ECS tasks to access EFS",
    policy=efs_policy_doc.json,
    tags={**PROJECT_TAG, "Name": "dagster-efs-policy"},
)

aws.iam.RolePolicyAttachment(
    "dagsterTaskEFSPolicy",
    role=dagster_task_role.name,
    policy_arn=efs_policy.arn,
)

# --- S3 Policy (Access BOTH asset bucket AND compute logs bucket) ---
s3_assets_bucket_name = "onecredit-dagster-assets-dev"
s3_compute_logs_bucket_name = "onecredit-dagster-compute-logs-dev"

s3_policy_doc = aws.iam.get_policy_document(
    statements=[
        {
            "effect": "Allow",
            "actions": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
            ],
            "resources": [
                # Assets bucket
                f"arn:aws:s3:::{s3_assets_bucket_name}",
                f"arn:aws:s3:::{s3_assets_bucket_name}/*",
                # Compute logs bucket
                f"arn:aws:s3:::{s3_compute_logs_bucket_name}",
                f"arn:aws:s3:::{s3_compute_logs_bucket_name}/*",
            ],
        }
    ]
)

s3_policy = aws.iam.Policy(
    "dagsterS3Policy",
    description="Dagster ECS tasks can access S3 asset bucket and compute logs bucket",
    policy=s3_policy_doc.json,
    tags={**PROJECT_TAG, "Name": "dagster-s3-policy"},
)

aws.iam.RolePolicyAttachment(
    "dagsterTaskS3Policy",
    role=dagster_task_role.name,
    policy_arn=s3_policy.arn,
)

# --- Export for use in ECS task definitions ---
pulumi.export("dagster_execution_role_arn", dagster_execution_role.arn)
pulumi.export("dagster_task_role_arn", dagster_task_role.arn)
pulumi.export("dagster_efs_policy_arn", efs_policy.arn)


# --- Accessors for other modules ---
def get_dagster_roles():
    return {
        "execution_role": dagster_execution_role,
        "task_role": dagster_task_role,
        "execution_role_arn": dagster_execution_role.arn,
        "task_role_arn": dagster_task_role.arn,
    }
