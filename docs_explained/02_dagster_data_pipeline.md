# Dagster Data Pipeline: Orchestrating Your Data Kitchen

Welcome to the heart of the OneCredit data pipeline! This document explains how **Dagster** orchestrates all our data processing - from the original `oc_etl` company data processing to the new **CTOS scrapers** for Malaysian business registration data.

Think of Dagster as your **Head Chef** who coordinates all the cooking processes in your professional kitchen. It ensures that data processing steps happen in the right order, with proper error handling and monitoring.

---

## 🏗️ **The Unified Data Kitchen**

### **What We're Cooking**

Our data kitchen now handles two main types of data processing:

1. **Company Profile Processing** (Original `oc_etl`)
   - Extract company data from PostgreSQL
   - Clean and validate the data
   - Upload to Snowflake data warehouse

2. **CTOS Business Registration Scraping** (New Integration)
   - Scrape Malaysian business registration data
   - Store in PostgreSQL with tracking
   - Handle retries and error recovery

### **The Kitchen Layout**

```
┌─────────────────────────────────────────────────────────────┐
│                    UNIFIED DAGSTER WORKSPACE               │
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐               │
│  │   oc_etl        │    │  CTOS Scrapers  │               │
│  │   Assets        │    │   Assets        │               │
│  │                 │    │                 │               │
│  │ • Raw Data      │    │ • ROB URLs      │               │
│  │ • Sanitized     │    │ • ROB Details   │               │
│  │ • Snowflake     │    │ • ROC URLs      │               │
│  │ • Parsed HTML   │    │ • ROC Details   │               │
│  │ • Cleaned Data  │    │ • Orchestration │               │
│  └─────────────────┘    └─────────────────┘               │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              UNIFIED JOBS & SCHEDULES              │   │
│  │                                                     │   │
│  │ • Company ETL Job (Daily)                          │   │
│  │ • ROB Scraping Job (Monday)                        │   │
│  │ • ROC Scraping Job (Thursday)                      │   │
│  │ • Retry Failed Jobs                                │   │
│  │ • Unified Company Job                              │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🍳 **The Assets: Our Recipes**

### **Original ETL Assets (5 assets)**

These are like the **core recipes** in your kitchen:

1. **`raw/company/pg_recc/public/raw_company_profiles`**
   - **What it does**: Extracts raw company data from PostgreSQL
   - **Like**: Gathering fresh ingredients from the market
   - **Output**: Raw company profiles DataFrame

2. **`sanitized/company/pg_recc/public/sanitized_company_profiles`**
   - **What it does**: Cleans and validates company data
   - **Like**: Washing and preparing ingredients
   - **Dependencies**: Raw company profiles
   - **Output**: Clean, validated company data

3. **`upload_to_snowflake/company/pg_recc/public/upload_to_snow_company_profiles`**
   - **What it does**: Uploads sanitized data to Snowflake
   - **Like**: Storing prepared ingredients in the walk-in freezer
   - **Dependencies**: Sanitized company profiles
   - **Output**: Data in Snowflake data warehouse

4. **`parsed_from_html`**
   - **What it does**: Processes HTML data from web scraping
   - **Like**: Extracting juice from fruits
   - **Output**: Structured data from HTML

5. **`cleaned_parquet_df`**
   - **What it does**: Creates cleaned Parquet files
   - **Like**: Creating pre-portioned meal components
   - **Output**: Clean Parquet files for storage

### **CTOS Scraper Assets (9 assets)**

These are like **specialized cooking processes** for Malaysian business data:

#### **ROB (Registry of Businesses) Assets**

6. **`rob/scrape_url`**
   - **What it does**: Scrapes ROB company listing URLs
   - **Like**: Collecting all the restaurant addresses in a city
   - **Output**: URLs stored in `rob_url_master` table

7. **`rob/scrape_registration`**
   - **What it does**: Scrapes detailed ROB company information
   - **Like**: Visiting each restaurant to get their menu and details
   - **Dependencies**: ROB URLs
   - **Output**: Company details in `rob_registration` table

#### **ROC (Registry of Companies) Assets**

8. **`roc/scrape_url`**
   - **What it does**: Scrapes ROC company listing URLs
   - **Like**: Collecting all the corporate office addresses
   - **Output**: URLs stored in `roc_url_master` table

9. **`roc/scrape_registration`**
   - **What it does**: Scrapes detailed ROC company information
   - **Like**: Visiting each corporate office for business details
   - **Dependencies**: ROC URLs
   - **Output**: Company details in `roc_registration` table

#### **Orchestration Assets**

10. **`retry_all_failed_urls`**
    - **What it does**: Retries failed scraping attempts
    - **Like**: Re-cooking dishes that didn't turn out right
    - **Output**: Retried failed URLs

11. **`orchestrate_url_scraping`**
    - **What it does**: Coordinates URL scraping across both ROB and ROC
    - **Like**: Managing the prep station in a busy kitchen
    - **Output**: Coordinated URL scraping

12. **`orchestrate_details_scraping`**
    - **What it does**: Coordinates detailed scraping across both ROB and ROC
    - **Like**: Managing the cooking station in a busy kitchen
    - **Output**: Coordinated detail scraping

---

## 👨‍🍳 **The Jobs: Complete Meal Preparations**

### **Original Jobs**

1. **`company_etl_job`**
   - **What it does**: Complete company data ETL pipeline
   - **Like**: Preparing a full course meal
   - **Schedule**: Daily
   - **Assets**: Raw → Sanitized → Snowflake upload

### **CTOS Scraper Jobs**

2. **`ctos_rob_scraping_job`**
   - **What it does**: Complete ROB scraping workflow
   - **Like**: Preparing a Malaysian street food menu
   - **Schedule**: Every Monday at 7 AM
   - **Assets**: ROB URLs → ROB Details

3. **`ctos_roc_scraping_job`**
   - **What it does**: Complete ROC scraping workflow
   - **Like**: Preparing a corporate catering menu
   - **Schedule**: Every Thursday at 7 AM
   - **Assets**: ROC URLs → ROC Details

4. **`ctos_retry_failed_job`**
   - **What it does**: Retry failed scraping attempts
   - **Like**: Re-cooking failed dishes
   - **Schedule**: On-demand
   - **Assets**: Retry failed URLs

### **Unified Jobs**

5. **`unified_company_etl_job`**
   - **What it does**: Combines both ETL and scraping workflows
   - **Like**: Preparing a complete banquet with multiple cuisines
   - **Schedule**: On-demand
   - **Assets**: Company ETL + ROB scraping + ROC scraping

---

## ⏰ **The Schedules: Kitchen Timetables**

### **Daily Operations**

1. **`daily_company_etl_schedule`**
   - **When**: Every day
   - **What**: Company data processing
   - **Like**: Daily kitchen prep routine

### **Weekly Scraping Operations**

2. **`ctos_rob_scraping_job_schedule`**
   - **When**: Every Monday at 7 AM
   - **What**: ROB business registration scraping
   - **Like**: Weekly market shopping for fresh ingredients

3. **`ctos_roc_scraping_job_schedule`**
   - **When**: Every Thursday at 7 AM
   - **What**: ROC company registration scraping
   - **Like**: Weekly supplier delivery for corporate ingredients

---

## 🔧 **The Resources: Kitchen Equipment**

### **Database Resources**

- **`pg_recc`**: PostgreSQL connection for company data
- **`pg_portal`**: PostgreSQL connection for portal data
- **`snowflake_resource`**: Snowflake data warehouse connection

### **Storage Resources**

- **`s3_io_manager`**: S3 storage for processed data
- **`s3_litigation_io_manager`**: S3 storage for litigation data
- **`s3`**: General S3 access

### **CTOS-Specific Resources**

The CTOS scrapers use the same PostgreSQL resources as the original ETL, creating a unified data storage approach.

---

## 📊 **Monitoring Your Kitchen**

### **Dagster UI Dashboard**

The Dagster UI is like your **kitchen monitoring system**:

- **Assets Tab**: See all your recipes (data assets)
- **Jobs Tab**: See all your meal preparations (job executions)
- **Schedules Tab**: See your kitchen timetables
- **Resources Tab**: Monitor your kitchen equipment

### **Real-Time Monitoring**

- **Asset Status**: Green (success), Red (failed), Yellow (in progress)
- **Job Execution**: See which jobs are running and their progress
- **Schedule Status**: Monitor scheduled job executions
- **Resource Health**: Check database and storage connections

### **Error Handling**

- **Automatic Retries**: Failed scraping attempts are automatically retried
- **Error Logging**: All errors are logged with detailed information
- **Alerting**: Failed jobs trigger notifications
- **Recovery**: Manual retry options for failed jobs

---

## 🚀 **Running Your Kitchen**

### **Local Development**

```bash
# Start the Dagster UI
dagster dev

# Access the UI at http://localhost:3000
```

### **Production Deployment**

The kitchen runs automatically in AWS with:
- **Scheduled Jobs**: Run automatically based on timetables
- **Monitoring**: Continuous health checks
- **Scaling**: Automatic scaling based on workload
- **Backup**: Automatic data backups

---

## 🔍 **Troubleshooting Your Kitchen**

### **Common Issues**

1. **Asset Failures**
   - Check database connections
   - Verify data source availability
   - Review error logs in Dagster UI

2. **Scraping Failures**
   - Check network connectivity
   - Verify target website availability
   - Review retry mechanisms

3. **Schedule Issues**
   - Check timezone settings
   - Verify schedule configurations
   - Review job dependencies

### **Debugging Tools**

- **Dagster UI**: Visual debugging and monitoring
- **Logs**: Detailed execution logs
- **Asset Materialization**: Track data lineage
- **Job Execution**: Step-by-step execution tracking

---

## 🎯 **Key Benefits of Unified Orchestration**

### **Single Source of Truth**
- All data processing in one place
- Unified monitoring and alerting
- Consistent error handling

### **Efficient Resource Usage**
- Shared database connections
- Unified storage strategy
- Optimized scheduling

### **Simplified Management**
- One workspace for all assets
- Unified job definitions
- Consistent configuration

### **Enhanced Monitoring**
- Complete data lineage tracking
- Unified performance metrics
- Integrated error reporting

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

---

## 📚 **Next Steps**

1. **Explore the Dagster UI** to see all assets and jobs
2. **Monitor job executions** to understand data flow
3. **Review schedules** to see automated operations
4. **Check resources** to understand infrastructure connections

Remember: Just like a professional kitchen, this data pipeline takes time to master, but once you understand how all the components work together, you can create amazing data-driven insights! 