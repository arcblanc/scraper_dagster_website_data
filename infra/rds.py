import pulumi
import pulumi_aws as aws
from config_infra import private_subnet_ids, db_password
from security_groups import dagster_sg

# RDS Subnet Group
db_subnet_group = aws.rds.SubnetGroup(
    "rds-subnet-group",
    subnet_ids=private_subnet_ids,
)

# RDS Instance
rds_instance = aws.rds.Instance(
    "dagster-rds",
    engine="postgres",
    instance_class="db.t3.micro",
    allocated_storage=20,
    db_name="dagsterdb",
    username="dagster_user",
    password=db_password,
    db_subnet_group_name=db_subnet_group.name,
    vpc_security_group_ids=[dagster_sg.id],
    skip_final_snapshot=True,
    publicly_accessible=False,
    storage_encrypted=True,
)

pulumi.export("rds_address", rds_instance.address)
pulumi.export("rds_endpoint", rds_instance.endpoint)
pulumi.export("rds_port", rds_instance.port)
pulumi.export("rds_security_group_id", dagster_sg.id)

print("RDS successfully created!")
