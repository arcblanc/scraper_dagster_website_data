# Malaysia Financial Data Pipeline

This repository contains the unified data pipeline infrastructure for Malaysian financial data, including company ETL processing, CTOS business registration scraping, and planned Bursa Malaysia market data integration.

Think of this as a **comprehensive financial data kitchen** that processes multiple types of Malaysian financial data - from company profiles to business registrations to market data.

---

## **Unified Financial Data Pipeline**

The pipeline now includes:

### **Original Data Processing (oc_etl)**
- Company profile data extraction from PostgreSQL
- Data sanitization and validation
- Snowflake data warehouse uploads
- Daily ETL schedules

### **CTOS Scrapers Integration**
- **ROB (Registry of Businesses)** scraping for Malaysian business data
- **ROC (Registry of Companies)** scraping for Malaysian company data
- PostgreSQL-based data storage and tracking
- Automated retry mechanisms for failed scraping jobs
- Weekly scheduled scraping (ROB on Mondays, ROC on Thursdays)

**Example CTOS Data Structure:**
```
| business_name | registration_number | registration_number_old | business_type | business_commenced | state |
|---------------|-------------------|------------------------|---------------|-------------------|-------|
| BERJAYA SPORTS TOTO BERHAD | 0009109K | 196901000688 | 1. INVESTMENT HOLDING 2. PROVISION OF MANAGEMENT SERVICES TO ITS SUBSIDIARY COMPANIES | 1969-11-24 | WILAYAH PERSEKUTUAN |
```

### **Bursa Malaysia Integration (Planned)**
- **Market Data Collection**: Real-time and historical stock market data
- **Financial Statements**: Company financial data and reports
- **Trading Data**: Volume, price, and trading activity
- **Regulatory Filings**: Bursa announcements and disclosures

**Example Bursa Financial Data Structure:**
```
| company_name_bursa | registration_number | company_id | fiscal_date | total_assets | net_income_after_taxes | investor_name | ownership_% |
|-------------------|-------------------|------------|-------------|--------------|----------------------|---------------|-------------|
| 7-ELEVEN MALAYSIA HOLDINGS BERHAD | 201301028701 | 5250 | 2024-12-31 | 2500000000 | 150000000 | EPF | 15.2 |
```

### **Unified Dagster Assets & Jobs**
- **14 Assets** combining both systems
- **7 Jobs** for different workflows
- **3 Schedules** for automated execution
- Single workspace configuration for unified management

---

## Local Development

The recommended way to develop locally is using the **VS Code Dev Container** setup. This ensures all dependencies and tools are installed in a consistent environment, without needing to set up infrastructure or cloud resources.

### Quick Start (Dev Container)

```bash
# First time only
git clone <repo-url>
cd malaysia-financial-data-pipeline

# Build the development container
docker build -f .devcontainer/Dockerfile.dev -t oc-data-eng-dev .

# Run the container with volume mount
docker run --rm -it -v $(pwd):/workspaces/oc-data-eng oc-data-eng-dev bash
```


1. **Open the project in VS Code.**
2. When prompted, or via the Command Palette (`Cmd+Shift+P`), select:
   - **"Dev Containers: Rebuild and Reopen in Container without Cache"**
   - This will build and start the dev container using the `.devcontainer/Dockerfile`.
3. Once the container is ready, it will automatically run `dagster dev` in the foreground. Wait until you see:
   > INFO - Serving dagster-webserver on http://127.0.0.1:3000 in process 1238.
4. Open your browser and go to [http://localhost:3000](http://localhost:3000) to access the Dagster UI (Dagit).

**Note:**
- You do **not** need to set up or run any infrastructure (Pulumi, AWS, etc.) for local development and testing.
- All code, dependencies, and tools are managed inside the dev container.
- The unified Dagster workspace will show both `oc_etl` and CTOS assets, jobs, and schedules.

To stop Dagster, simply press `Ctrl+C` in the terminal.

---

## Production Deployment

Production deployment is now fully automated using **Pulumi**. You no longer need to use Docker Compose for production. Instead, Pulumi will handle all infrastructure provisioning, Docker image building, and deployment to AWS.

### How to Deploy to Production (Step-by-Step)

1. **Export your environment variables**
   Make sure your `.env` file is loaded or export the necessary environment variables for AWS and Pulumi. This has to be done at the project root level.
   ```bash
   export $(cat .env | xargs)
   ```

2. **Login to Pulumi using the state bucket URL**
   The state bucket URL is specified in `infra/Pulumi.yaml` under the `backend.url` field.
   ```bash
   pulumi login s3://onecredit-data-eng-pulumi-state-malaysia
   ```

3. **Set up the Python environment for infrastructure code**
   ```bash
   cd infra
   python3 -m venv .venv
   source .venv/bin/activate
   uv pip install .   # install from the infra level's pyproject.toml
   ```

4. **Deploy the infrastructure**
   This step has to be done under the infra/ level. This will automatically build the Docker image, push it to ECR, and deploy all Dagster services (webserver, daemon, codeserver) on AWS ECS Fargate.
   ```bash
   pulumi up
   ```

5. **To destroy the infrastructure (use with caution)**
   ```bash
   pulumi destroy
   ```

The Dagster UI will be available at the Application Load Balancer URL output under the webserver service in dagster-cluster by Pulumi after deployment.

**Note:**
- All service startup, networking, and configuration are managed by Pulumi and AWS ECS.
- The production deployment includes the unified Dagster setup with both `oc_etl` and CTOS scrapers.

---

## AWS Infrastructure

The project includes comprehensive AWS infrastructure setup in the `infra/` directory using Pulumi. The infrastructure is built using Pulumi with a preference for `awsx` (AWS Crosswalk) resources where possible, falling back to standard `aws` resources when needed. This approach provides higher-level abstractions and better defaults for common AWS patterns.

### Infrastructure Components

**Compute & Orchestration:**
- **ECS Fargate Cluster**: Hosts three Dagster services (webserver, daemon, codeserver)
- **Application Load Balancer**: Provides HTTP access to the Dagster UI
- **Service Discovery**: Private DNS namespace for service-to-service communication using cloudmap

**Storage & Persistence:**
- **EFS File System**: Persistent storage for Dagster metadata and code
- **S3 Buckets**: Asset storage and compute logs
- **RDS PostgreSQL**: Dagster metadata database and CTOS scraper data storage

**Security & Access:**
- **IAM Roles & Policies**: Task execution and ECS service roles with appropriate permissions
- **Security Groups**: Network isolation and access control
- **VPC & Networking**: Private subnets with NAT gateway for internet access, under staging for now

**Container Registry:**
- **ECR Repository**: Stores the Dagster application Docker image
- **Lifecycle Policies**: Automatic cleanup of old images

**Monitoring:**
- **CloudWatch Log Groups**: Centralized logging for all Dagster services

---

## Accessing the Dagster UI

After successful deployment, you can access the Dagster UI as follows:

1. Choose **Malaysia region** in AWS Console
2. Go to **Elastic Container Service** → **dagster-cluster** → **dagster-webserver-service**
3. Scroll down to see **load balancer**
4. Click **dagster-alb**
5. Find the **DNS name** (copy and paste in your browser)

**Note:** The ALB is configured for HTTP access only (not HTTPS) for the staging environment.

**Important:** If you run `pulumi destroy` followed by `pulumi up`, the ALB URL may change. You can find the current URL in the AWS Console under EC2 > Load Balancers, or check the Pulumi output after deployment.

---

## Prerequisites

- AWS CLI configured with appropriate credentials
- Pulumi CLI installed
- Docker installed (for building the application image)

**Important Notes:**
- The deployment will create resources in the `ap-southeast-5` region
- Estimated cost: ~$50-100/month for the complete setup
- The infrastructure includes high availability features (multiple AZs, load balancer)

---

## Project Structure

- `src/oc_etl`: Original data pipeline source code
- `src/ctos_scrapers`: CTOS scraper integration for Malaysian business data
- `src/unified_definitions.py`: Unified Dagster definitions combining both systems
- `infra/`: AWS infrastructure setup using Pulumi
- `sql/`: SQL queries and transformations
- `data/`: Local data storage (if needed)
- `docs_explained/`: Comprehensive documentation for all components

---

## Documentation

For detailed documentation on each component, see the `docs_explained/` directory:

- **01_overview_and_purpose.md**: Overall system architecture and purpose
- **02_dagster_data_pipeline.md**: Dagster orchestration details
- **03_aws_infrastructure_pulumi.md**: AWS infrastructure setup
- **04_development_workflow_and_structure.md**: Development workflow and project structure
- **05_ctos_integration.md**: CTOS scraper integration details
- **06_postgresql_integration.md**: PostgreSQL data storage and management

---

## Roadmap

### **Phase 1: Current Implementation** ✅
- ✅ Company ETL processing
- ✅ CTOS business registration scraping
- ✅ Unified Dagster orchestration
- ✅ AWS infrastructure deployment

### **Phase 2: Bursa Malaysia Integration** 🚧
- 🚧 Market data collection from Bursa Malaysia
- 🚧 Financial statement processing
- 🚧 Trading data analysis
- 🚧 Regulatory filing monitoring

### **Phase 3: Advanced Analytics** 📋
- 📋 Machine learning models for financial analysis
- 📋 Real-time data processing
- 📋 Advanced reporting and dashboards
- 📋 API endpoints for data access https://open.dosm.gov.my/

---

*This project represents a comprehensive approach to Malaysian financial data engineering, combining traditional ETL processes with modern web scraping and cloud infrastructure.*
