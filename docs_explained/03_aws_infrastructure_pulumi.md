# AWS Infrastructure & Pulumi Deployment: The Cloud Kitchen

## What is AWS Infrastructure?

Imagine you're opening a restaurant chain. Instead of building your own kitchen from scratch, you rent a fully-equipped professional kitchen space. The landlord provides everything: stoves, refrigerators, dishwashers, security systems, and even staff to maintain the equipment. You just focus on cooking great food.

**AWS is that professional kitchen space for your data pipeline.**

## 🏗️ **Infrastructure as Code with Pulumi**

### **What is Pulumi?**

Think of **Pulumi** as an **architect and contractor** who builds your entire restaurant based on blueprints you provide. Instead of manually setting up each piece of equipment, you write code that describes your ideal kitchen layout, and Pulumi builds it exactly as specified.

**Traditional Approach (Manual Setup):**
```
1. Log into AWS Console
2. Create VPC manually
3. Create subnets manually
4. Create security groups manually
5. Create databases manually
6. Create containers manually
7. Configure networking manually
8. Set up monitoring manually
```

**Pulumi Approach (Infrastructure as Code):**
```python
# Define your entire infrastructure in code
vpc = awsx.ec2.Vpc("my-vpc")
database = aws.rds.Instance("my-db")
container = aws.ecs.Service("my-service")
```

**One command builds everything:**
```bash
pulumi up
```

## 🎯 **The AWS Infrastructure Architecture**

### **The Complete Kitchen Layout**

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS Cloud                               │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Public    │  │   Private   │  │   Private   │          │
│  │   Subnet    │  │   Subnet    │  │   Subnet    │          │
│  │  (ALB)      │  │ (Webserver) │  │ (Daemon)    │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   RDS       │  │     S3      │  │   EFS       │          │
│  │ PostgreSQL  │  │   Storage   │  │   Shared    │          │
│  │  Database   │  │             │  │   Storage   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

## 🏢 **Core AWS Services Explained**

### **1. VPC (Virtual Private Cloud): The Restaurant Building**

Think of **VPC** as the building that houses your restaurant. It's a private, isolated space where you control everything.

```python
# From infra/config_infra.py
vpc_id = "vpc-12345678"  # Your restaurant building
public_subnet_ids = ["subnet-1", "subnet-2"]  # Public dining areas
private_subnet_ids = ["subnet-3", "subnet-4"]  # Private kitchen areas
```

**Why VPC matters:**
- **Security**: Only authorized traffic can enter your "building"
- **Isolation**: Your restaurant is separate from other restaurants
- **Control**: You decide what connects to what

### **2. ECS (Elastic Container Service): The Kitchen Staff**

**ECS** is like having a team of professional chefs who can work on multiple dishes simultaneously.

```python
# From infra/__main__.py
ecs_cluster = aws.ecs.Cluster("dagster-cluster")
```

**What ECS provides:**
- **Container orchestration**: Manages multiple containers (like managing multiple chefs)
- **Auto-scaling**: Adds more chefs when busy, removes them when slow
- **Load balancing**: Distributes work evenly among chefs
- **Health monitoring**: Ensures all chefs are working properly

### **3. Fargate: The Chef Management System**

**Fargate** is like having a professional chef management company. You don't worry about hiring individual chefs or buying kitchen equipment - the company provides everything.

```python
# From infra/services.py
webserver = create_webserver_service(
    name="dagster",
    image_uri=dagster_image.image_uri,
    cluster=ecs_cluster,
    # ... other configuration
)
```

**Benefits of Fargate:**
- **Serverless**: No need to manage servers (like not worrying about kitchen equipment)
- **Pay-per-use**: Only pay for what you use (like paying chefs only when they work)
- **Auto-scaling**: Automatically adjusts based on demand
- **Managed**: AWS handles the underlying infrastructure

### **4. RDS (Relational Database Service): The Recipe Book**

**RDS** is like having a professional librarian who manages your recipe book (database).

```python
# From infra/rds.py
rds_instance = aws.rds.Instance(
    "dagster-db",
    engine="postgres",
    instance_class="db.t3.micro",
    allocated_storage=20,
    db_name="dagsterdb",
    username="dagster_user",
    password=db_password,
    skip_final_snapshot=True,
)
```

**What RDS provides:**
- **Managed database**: AWS handles backups, updates, security
- **High availability**: Multiple copies of your data
- **Automated maintenance**: Updates and patches applied automatically
- **Monitoring**: Built-in performance monitoring

### **5. S3 (Simple Storage Service): The Massive Walk-in Freezer**

**S3** is like having an infinite walk-in freezer where you can store anything.

```python
# From infra/buckets.py
dagster_data_bucket = aws.s3.Bucket(
    "dagster-data-bucket",
    versioning={"enabled": True},
    lifecycle_rules=[{
        "id": "cleanup_old_versions",
        "enabled": True,
        "noncurrent_version_expiration": {"days": 30}
    }]
)
```

**S3 features:**
- **Unlimited storage**: Store as much data as you need
- **Durability**: 99.999999999% reliability (like having backup freezers)
- **Cost-effective**: Pay only for what you store
- **Versioning**: Keep multiple versions of files

### **6. EFS (Elastic File System): The Shared Kitchen Storage**

**EFS** is like having a shared storage area that all your chefs can access simultaneously.

```python
# From infra/efs.py
efs = aws.efs.FileSystem(
    "dagster-efs",
    encrypted=True,
    tags={"Name": "dagster-efs"}
)
```

**Why EFS matters:**
- **Shared access**: Multiple containers can read/write simultaneously
- **Persistent**: Data survives container restarts
- **Scalable**: Automatically grows as you need more space
- **High performance**: Fast access for data processing

### **7. Application Load Balancer (ALB): The Hostess**

**ALB** is like having a professional hostess who directs customers to available tables.

```python
# From infra/__main__.py
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
```

**ALB features:**
- **Traffic distribution**: Routes requests to healthy containers
- **Health checks**: Ensures only healthy services receive traffic
- **SSL termination**: Handles HTTPS certificates
- **Auto-scaling**: Works with ECS to scale services

## 🔐 **Security & Access Control**

### **Security Groups: The Restaurant Security System**

**Security Groups** are like having security guards who control who can enter different areas of your restaurant.

```python
# From infra/security_groups.py
dagster_sg = aws.ec2.SecurityGroup(
    "dagster-sg",
    description="Security group for Dagster services",
    vpc_id=vpc_id,
    ingress=[
        # Allow HTTP traffic to webserver
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=3000,
            to_port=3000,
            cidr_blocks=["0.0.0.0/0"]
        ),
        # Allow internal communication
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=4000,
            to_port=4000,
            security_groups=[dagster_sg.id]
        ),
    ],
    egress=[
        # Allow all outbound traffic
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1",
            from_port=0,
            to_port=0,
            cidr_blocks=["0.0.0.0/0"]
        ),
    ],
)
```

### **IAM (Identity and Access Management): Employee Badges**

**IAM** is like having a badge system that controls what each employee can access.

```python
# From infra/iam.py
def get_dagster_roles():
    """Create IAM roles for Dagster services"""
    
    # Task execution role (like a chef's badge)
    execution_role = aws.iam.Role(
        "dagster-execution-role",
        assume_role_policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "ecs-tasks.amazonaws.com"}
            }]
        })
    )
    
    # Task role (like a manager's badge)
    task_role = aws.iam.Role(
        "dagster-task-role",
        assume_role_policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "ecs-tasks.amazonaws.com"}
            }]
        })
    )
    
    return {
        "execution_role_arn": execution_role.arn,
        "task_role_arn": task_role.arn
    }
```

## 🐳 **Container Management**

### **ECR (Elastic Container Registry): The Recipe Repository**

**ECR** is like having a central recipe repository where all your chefs can access the latest recipes.

```python
# From infra/__main__.py
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
```

**ECR features:**
- **Private registry**: Only your team can access your recipes
- **Version control**: Keep multiple versions of your containers
- **Lifecycle policies**: Automatically clean up old versions
- **Security scanning**: Check for vulnerabilities in your containers

### **Docker Image Building**

```python
# From infra/__main__.py
dagster_image = awsx.ecr.Image(
    "dagster-image",
    repository_url=ecr_repo.url,
    context="..",
    dockerfile="../Dockerfile.prod",
    platform="linux/amd64",
    opts=ResourceOptions(depends_on=[ecr_repo]),
)
```

**What this does:**
- **Builds** your application into a container image
- **Pushes** the image to ECR
- **Makes** it available for deployment
- **Ensures** consistency across environments

## 📊 **Monitoring & Logging**

### **CloudWatch Log Groups: The Kitchen Logbook**

**CloudWatch** is like having a detailed logbook that records everything that happens in your kitchen.

```python
# From infra/__main__.py
webserver_log_group = aws.cloudwatch.LogGroup(
    "dagster-webserver-logs", 
    name="/ecs/dagster-webserver"
)
daemon_log_group = aws.cloudwatch.LogGroup(
    "dagster-daemon-logs", 
    name="/ecs/dagster-daemon"
)
code_server_log_group = aws.cloudwatch.LogGroup(
    "dagster-codeserver-logs", 
    name="/ecs/dagster-codeserver"
)
```

**CloudWatch provides:**
- **Centralized logging**: All logs in one place
- **Search and filter**: Find specific events quickly
- **Alerts**: Notify you when something goes wrong
- **Metrics**: Track performance over time

## 🚀 **Deployment Process**

### **The Deployment Pipeline**

```bash
# 1. Set up environment variables
export $(cat .env | xargs)

# 2. Login to Pulumi
pulumi login s3://onecredit-data-eng-pulumi-state-malaysia

# 3. Navigate to infrastructure directory
cd infra

# 4. Set up Python environment
python3 -m venv .venv
source .venv/bin/activate
uv pip install .

# 5. Deploy everything
pulumi up
```

**What happens during deployment:**

1. **Infrastructure Creation**: Pulumi creates all AWS resources
2. **Docker Build**: Your application is built into a container
3. **Image Push**: Container is uploaded to ECR
4. **Service Deployment**: ECS services are created and started
5. **Health Checks**: System verifies everything is working
6. **DNS Setup**: Load balancer becomes accessible

### **Service Discovery: Internal Communication**

```python
# From infra/service_discovery.py
webserver_sd_service = aws.servicediscovery.HttpService(
    "dagster-webserver-sd",
    namespace_id=namespace.id,
    name="webserver",
    health_check_custom_config={
        "failure_threshold": 1,
        "healthy_threshold": 1,
    }
)
```

**Service Discovery provides:**
- **Internal DNS**: Services can find each other by name
- **Load balancing**: Distributes requests among multiple instances
- **Health monitoring**: Only healthy services receive traffic
- **Auto-scaling**: Works with ECS to scale services

## 💰 **Cost Management**

### **Estimated Monthly Costs**

- **ECS Fargate**: ~$30-50/month (compute resources)
- **RDS PostgreSQL**: ~$15-25/month (database)
- **S3 Storage**: ~$5-10/month (data storage)
- **ALB**: ~$20/month (load balancer)
- **EFS**: ~$5-10/month (shared storage)
- **Data Transfer**: ~$5-15/month (network traffic)

**Total: ~$80-130/month** for a complete production setup

### **Cost Optimization Strategies**

1. **Right-sizing**: Use appropriate instance sizes
2. **Auto-scaling**: Scale down during low usage
3. **Lifecycle policies**: Clean up old data automatically
4. **Reserved instances**: Commit to longer terms for discounts
5. **Monitoring**: Track usage to identify optimization opportunities

## 🔧 **Troubleshooting & Maintenance**

### **Common Issues and Solutions**

**Service Won't Start:**
```bash
# Check ECS service logs
aws logs describe-log-groups --log-group-name-prefix "/ecs/dagster"

# Check service events
aws ecs describe-services --cluster dagster-cluster --services dagster-webserver-service
```

**Database Connection Issues:**
```bash
# Test database connectivity
aws rds describe-db-instances --db-instance-identifier dagster-db

# Check security group rules
aws ec2 describe-security-groups --group-ids sg-12345678
```

**Container Build Failures:**
```bash
# Check ECR repository
aws ecr describe-repositories --repository-names dagster

# List container images
aws ecr list-images --repository-name dagster
```

### **Monitoring and Alerts**

```python
# Example CloudWatch alarm
alarm = aws.cloudwatch.MetricAlarm(
    "dagster-cpu-alarm",
    comparison_operator="GreaterThanThreshold",
    evaluation_periods=2,
    metric_name="CPUUtilization",
    namespace="AWS/ECS",
    period=300,
    statistic="Average",
    threshold=80,
    alarm_description="High CPU usage in Dagster services"
)
```

## 🎯 **Best Practices**

### **Security Best Practices**

1. **Principle of Least Privilege**: Only grant necessary permissions
2. **Network Isolation**: Use private subnets for sensitive services
3. **Encryption**: Encrypt data at rest and in transit
4. **Regular Updates**: Keep containers and dependencies updated
5. **Access Logging**: Monitor who accesses your resources

### **Performance Best Practices**

1. **Right-sizing**: Use appropriate instance types
2. **Caching**: Cache frequently accessed data
3. **CDN**: Use CloudFront for static content
4. **Database Optimization**: Use appropriate indexes and queries
5. **Monitoring**: Set up comprehensive monitoring

### **Operational Best Practices**

1. **Infrastructure as Code**: Version control your infrastructure
2. **Automated Testing**: Test infrastructure changes
3. **Backup Strategy**: Regular backups of critical data
4. **Disaster Recovery**: Plan for failure scenarios
5. **Documentation**: Keep infrastructure documentation updated

This AWS infrastructure provides a **scalable, secure, and cost-effective** foundation for your data pipeline - just like a professional kitchen provides everything needed to serve customers reliably and efficiently. 