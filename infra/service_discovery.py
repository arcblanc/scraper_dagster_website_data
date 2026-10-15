import pulumi
import pulumi_aws as aws

# Config
config = pulumi.Config()
vpc_id = config.require("vpcId")

# create a namespace for Dagster services in AWS Cloud Map
cloudmap_namespace = aws.servicediscovery.PrivateDnsNamespace(
    "dagster-namespace",
    name="dagster.internal",
    vpc=vpc_id,
    description="Private namespace for Dagster services",
)

# Create service discovery service for each ECS service
webserver_sd_service = aws.servicediscovery.Service(
    "webserver-sd-service",
    name="webserver",
    dns_config={
        "namespace_id": cloudmap_namespace.id,
        "dns_records": [
            {
                "type": "A",
                "ttl": 10,
            }
        ],
        "routing_policy": "MULTIVALUE",
    },
    health_check_custom_config={
        "failure_threshold": 1,
    },
)

daemon_sd_service = aws.servicediscovery.Service(
    "daemon-sd-service",
    name="daemon",
    dns_config={
        "namespace_id": cloudmap_namespace.id,
        "dns_records": [
            {
                "type": "A",
                "ttl": 10,
            }
        ],
        "routing_policy": "MULTIVALUE",
    },
    health_check_custom_config={
        "failure_threshold": 1,
    },
)

codeserver_sd_service = aws.servicediscovery.Service(
    "codeserver-sd-service",
    name="codeserver",
    dns_config={
        "namespace_id": cloudmap_namespace.id,
        "dns_records": [
            {
                "type": "A",
                "ttl": 10,
            }
        ],
        "routing_policy": "MULTIVALUE",
    },
    health_check_custom_config={
        "failure_threshold": 1,
    },
)
