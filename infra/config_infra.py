import pulumi

# Config
config = pulumi.Config("oc-data-eng-pulumi")

config = pulumi.Config()
vpc_id = config.require("vpcId")
public_subnet_ids = config.require_object("publicSubnetIds")
private_subnet_ids = config.require_object("privateSubnetIds")
db_password = config.require_secret("dbPassword")

print("VPC:", vpc_id)
print("Public Subnets:", public_subnet_ids)
print("Private Subnets:", private_subnet_ids)
