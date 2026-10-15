# Development Workflow & Project Structure: Building Your Data Kitchen

Welcome to the development guide for the OneCredit data pipeline! This document explains how to work with our **unified codebase** that combines the original `oc_etl` data processing with **CTOS scrapers** for Malaysian business registration data.

Think of this as your **kitchen manual** - it tells you how to set up your workspace, organize your tools, and maintain high standards in your data kitchen.

---

## 🏗️ **Project Structure: Your Kitchen Layout**

### **Root Directory Structure**

```
oc-data-eng/
├── src/                          # Main source code (your kitchen)
│   ├── oc_etl/                   # Original data pipeline
│   ├── ctos_scrapers/            # CTOS scraper integration
│   ├── unified_definitions.py    # Unified Dagster workspace
│   └── unified_definitions/      # Module structure
├── infra/                        # AWS infrastructure (kitchen building)
├── docs_explained/               # Documentation (recipe books)
├── sql/                          # SQL queries (cooking instructions)
├── helpers_sh/                   # Helper scripts (kitchen tools)
├── .devcontainer/                # Development environment
├── workspace.yaml                # Dagster workspace config
├── pyproject.toml               # Python dependencies
└── README.md                    # Main project overview
```

### **Source Code Organization**

#### **`src/oc_etl/` - Original Data Pipeline**
```
src/oc_etl/
├── assets/                       # Data processing assets
│   ├── company/                  # Company data assets
│   └── litigation/               # Litigation data assets
├── jobs/                         # Job definitions
├── resources/                    # Resource configurations
├── schedules/                    # Schedule definitions
└── config.py                    # Configuration settings
```

#### **`src/ctos_scrapers/` - CTOS Scraper Integration**
```
src/ctos_scrapers/
└── ctos_scraper/
    ├── assets/                   # Scraper assets
    │   ├── scrape_rob.py         # ROB scraper
    │   ├── scrape_roc.py         # ROC scraper
    │   ├── scrape_rob_url.py     # ROB URL scraper
    │   ├── scrape_roc_url.py     # ROC URL scraper
    │   ├── postgres_utils.py     # Database utilities
    │   └── tracking.py           # Progress tracking
    ├── dagster_assets/           # Dagster asset definitions
    │   ├── rob_assets.py         # ROB assets
    │   ├── roc_assets.py         # ROC assets
    │   └── orchestrator_assets.py # Orchestration assets
    └── config/                   # Configuration
        └── logging_setup.py      # Logging configuration
```

#### **`src/unified_definitions.py` - Unified Workspace**
This is the **main kitchen** where everything comes together:
- Imports assets from both `oc_etl` and `ctos_scrapers`
- Defines unified jobs and schedules
- Configures shared resources
- Creates single workspace for all operations

---

## 🛠️ **Development Environment Setup**

### **VS Code Dev Container (Recommended)**

The Dev Container is like having a **pre-configured professional kitchen** with all the tools you need:

1. **Open the project in VS Code**
2. **Select "Dev Containers: Rebuild and Reopen in Container"**
3. **Wait for container to build** (includes all dependencies)
4. **Dagster automatically starts** with `dagster dev`

### **What's Included in the Dev Container**

- **Python 3.13** with all dependencies
- **Dagster** with unified workspace
- **PostgreSQL** client libraries
- **AWS CLI** and SDK
- **Development tools** (ruff, pre-commit, etc.)
- **All CTOS scraper dependencies**

### **Local Development (Alternative)**

If you prefer to set up locally:

```bash
# Install dependencies
uv sync

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Start Dagster
dagster dev
```

---

## 🔧 **Working with the Unified Codebase**

### **Understanding the Unified Workspace**

The `src/unified_definitions.py` file is the **central kitchen** where everything comes together:

```python
# Import both systems
from src.oc_etl import company_raw_assets, company_sanitize_assets
from ctos_scrapers.ctos_scraper.dagster_assets.rob_assets import rob_scrape_url

# Combine all assets
def get_all_assets():
    return oc_etl_assets + ctos_assets

# Create unified definitions
defs = Definitions(
    assets=get_all_assets(),
    jobs=[company_etl_job, ctos_rob_job, ctos_roc_job],
    schedules=[daily_company_etl_schedule, ctos_rob_schedule, ctos_roc_schedule],
    resources={...}
)
```

### **Adding New Assets**

#### **For oc_etl Assets**
1. Create asset in `src/oc_etl/assets/`
2. Import in `src/unified_definitions.py`
3. Add to `get_all_assets()` function

#### **For CTOS Assets**
1. Create asset in `src/ctos_scrapers/ctos_scraper/dagster_assets/`
2. Import in `src/unified_definitions.py`
3. Add to `get_all_assets()` function

### **Adding New Jobs**

```python
# Define new job
new_job = define_asset_job(
    name="new_job_name",
    selection=["asset1", "asset2"]
)

# Add to unified definitions
defs = Definitions(
    jobs=[..., new_job],
    # ... other configurations
)
```

### **Adding New Schedules**

```python
# Define new schedule
new_schedule = ScheduleDefinition(
    job=new_job,
    cron_schedule="0 7 * * 1",  # Every Monday 7 AM
    description="New schedule description"
)

# Add to unified definitions
defs = Definitions(
    schedules=[..., new_schedule],
    # ... other configurations
)
```

---

## 📝 **Code Quality Standards**

### **Python Code Style**

We use **ruff** for code formatting and linting:

```bash
# Format code
ruff format src/

# Check for issues
ruff check src/

# Auto-fix issues
ruff check --fix src/
```

### **Pre-commit Hooks**

Automated quality checks run before commits:

```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

### **Documentation Standards**

- **Use restaurant metaphors** for complex concepts
- **Include code examples** for all features
- **Update relevant docs** when adding features
- **Follow markdown formatting** standards

---

## 🧪 **Testing Your Changes**

### **Local Testing**

1. **Start Dagster UI**
   ```bash
   dagster dev
   ```

2. **Test Assets**
   - Go to Assets tab in Dagster UI
   - Click on asset to see details
   - Test materialization if needed

3. **Test Jobs**
   - Go to Jobs tab in Dagster UI
   - Launch job manually
   - Monitor execution

4. **Test Schedules**
   - Go to Schedules tab in Dagster UI
   - Verify schedule configuration
   - Test manual execution

### **Database Testing**

For CTOS scrapers that use PostgreSQL:

```python
# Test database connection
from ctos_scrapers.ctos_scraper.assets.postgres_utils import get_conn

with get_conn() as conn:
    # Test your database operations
    pass
```

### **Scraper Testing**

For CTOS scrapers:

```python
# Test individual scrapers
from ctos_scrapers.ctos_scraper.assets.scrape_rob import LocalROBScraper

# Test with small dataset
scraper = LocalROBScraper()
# Add test configuration
```

---

## 🚀 **Deployment Workflow**

### **Development → Staging → Production**

1. **Local Development**
   - Test changes in Dev Container
   - Verify all assets and jobs work
   - Update documentation

2. **Staging Deployment**
   ```bash
   cd infra
   pulumi up --stack staging
   ```

3. **Production Deployment**
   ```bash
   cd infra
   pulumi up --stack production
   ```

### **Deployment Checklist**

- [ ] All tests pass locally
- [ ] Documentation updated
- [ ] Environment variables configured
- [ ] Database migrations applied
- [ ] Dagster workspace loads without errors
- [ ] Assets and jobs visible in UI
- [ ] Schedules configured correctly

---

## 🔍 **Debugging and Troubleshooting**

### **Common Issues**

#### **Asset Loading Errors**
```bash
# Check if unified definitions load
python -c "from src.unified_definitions import defs; print('✅ Loaded successfully')"
```

#### **Database Connection Issues**
```bash
# Test database connection
python -c "from ctos_scrapers.ctos_scraper.assets.postgres_utils import get_conn; get_conn()"
```

#### **Import Errors**
```bash
# Check Python path
echo $PYTHONPATH

# Test imports
python -c "import src.oc_etl; import ctos_scrapers"
```

### **Dagster UI Debugging**

1. **Check Asset Status**
   - Green: Success
   - Red: Failed
   - Yellow: In Progress

2. **Review Execution Logs**
   - Click on failed asset
   - Check error messages
   - Review execution steps

3. **Monitor Resources**
   - Check database connections
   - Verify S3 access
   - Test Snowflake connectivity

---

## 📚 **Best Practices**

### **Code Organization**

1. **Keep related code together**
   - CTOS scrapers in `ctos_scrapers/`
   - ETL code in `oc_etl/`
   - Unified definitions in `unified_definitions.py`

2. **Use descriptive names**
   - Asset names should be clear
   - Job names should indicate purpose
   - Schedule names should show timing

3. **Follow existing patterns**
   - Use similar structure for new assets
   - Follow naming conventions
   - Maintain consistent configuration

### **Documentation**

1. **Update docs when adding features**
2. **Use restaurant metaphors** for clarity
3. **Include code examples**
4. **Explain business value**

### **Testing**

1. **Test locally first**
2. **Verify all assets load**
3. **Check job execution**
4. **Validate schedules**

---

## 🔄 **Recent Updates**

### **Unified Dagster Integration**
- Combined `oc_etl` and CTOS scrapers in single workspace
- Fixed asset key naming conventions
- Resolved database connection issues during import
- Updated schedules (ROB on Monday, ROC on Thursday)

### **CTOS Scraper Integration**
- Added PostgreSQL-based data storage
- Implemented retry mechanisms for failed jobs
- Created orchestration assets for complex workflows
- Integrated with existing Dagster resources

### **Development Workflow Improvements**
- Enhanced Dev Container setup
- Improved code quality standards
- Added comprehensive testing procedures
- Updated documentation structure

---

## 🎯 **Next Steps**

1. **Explore the unified workspace** in Dagster UI
2. **Test asset materialization** for both systems
3. **Monitor job executions** to understand data flow
4. **Review schedules** to see automated operations
5. **Contribute improvements** following these guidelines

Remember: Just like a professional kitchen, this development environment takes practice to master, but once you understand how all the tools work together, you can create amazing data-driven solutions! 