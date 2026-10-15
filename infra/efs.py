import pulumi
import pulumi_aws as aws
from config_infra import private_subnet_ids
from security_groups import dagster_sg

efs = aws.efs.FileSystem(
    "dagster-efs",
    creation_token="dagster-efs-token",
    encrypted=True,
    lifecycle_policies=[
        {
            "transition_to_ia": "AFTER_30_DAYS",
        }
    ],
    tags={"Name": "dagster-efs", "Project": "dagster"},
)

# Enable automatic backups
aws.efs.BackupPolicy(
    "dagster-efs-backup",
    file_system_id=efs.id,
    backup_policy={
        "status": "ENABLED",
    },
)

# Create access point for namespacing / security
access_point = aws.efs.AccessPoint(
    "dagster-efs-access-point",
    file_system_id=efs.id,
    posix_user={
        "gid": 0,
        "uid": 0,
    },
    root_directory={
        "creation_info": {
            "owner_gid": 0,
            "owner_uid": 0,
            "permissions": "755",
        },
        "path": "/dagster",
    },
    tags={"Name": "dagster-efs-access-point"},
)

# One mount target per AZ (deduplicated)
subnet_az_map = {}
for subnet_id in private_subnet_ids:
    subnet = aws.ec2.get_subnet(id=subnet_id)
    if subnet.availability_zone not in subnet_az_map:
        subnet_az_map[subnet.availability_zone] = subnet_id

mount_targets = []
for az, subnet_id in subnet_az_map.items():
    mt = aws.efs.MountTarget(
        f"dagster-efs-mount-target-{az.replace('-', '')}",
        file_system_id=efs.id,
        subnet_id=subnet_id,
        security_groups=[dagster_sg.id],
    )
    mount_targets.append(mt)

# Export for reuse
pulumi.export("efs_id", efs.id)
pulumi.export("efs_access_point_id", access_point.id)
pulumi.export("efs_dns_name", efs.dns_name)
