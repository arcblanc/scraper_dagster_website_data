import pulumi
import pulumi_aws as aws

# Tags and config
config = pulumi.Config()
vpc_id = config.require("vpcId")
PROJECT_TAG = {"Project": "dagster"}

# --- ONE Security Group for EVERYTHING ---
dagster_sg = aws.ec2.SecurityGroup(
    "dagster-sg",
    description="Security Group for all Dagster infrastructure (ECS, ALB, RDS, EFS)",
    vpc_id=vpc_id,
    tags={**PROJECT_TAG, "Name": "dagster-everything"},
)

# --- Allow ALL egress ---
aws.ec2.SecurityGroupRule(
    "dagster-egress-all",
    type="egress",
    from_port=0,
    to_port=65535,
    protocol="-1",
    security_group_id=dagster_sg.id,
    cidr_blocks=["0.0.0.0/0"],
)

# --- Allow ALL internal communication ---
# This covers: ECS services, ALB, RDS, EFS - everything talks to everything
aws.ec2.SecurityGroupRule(
    "dagster-internal-all",
    type="ingress",
    from_port=0,
    to_port=65535,
    protocol="-1",
    security_group_id=dagster_sg.id,
    source_security_group_id=dagster_sg.id,
)

# --- Allow external access to ALB (port 80/443) ---
aws.ec2.SecurityGroupRule(
    "dagster-alb-http",
    type="ingress",
    from_port=80,
    to_port=80,
    protocol="tcp",
    security_group_id=dagster_sg.id,
    cidr_blocks=["0.0.0.0/0"],
)

aws.ec2.SecurityGroupRule(
    "dagster-alb-https",
    type="ingress",
    from_port=443,
    to_port=443,
    protocol="tcp",
    security_group_id=dagster_sg.id,
    cidr_blocks=["0.0.0.0/0"],
)

# --- Export Security Group ID ---
pulumi.export("dagster_sg_id", dagster_sg.id)
