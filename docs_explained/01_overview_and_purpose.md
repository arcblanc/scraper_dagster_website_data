# OneCredit Data Pipeline: Complete System Overview

Welcome to the OneCredit Data Pipeline! This document provides a comprehensive overview of our **unified data engineering system** that combines traditional ETL processing with advanced web scraping capabilities.

Think of this system as a **professional restaurant kitchen** that serves multiple cuisines - we have the original Malaysian company data processing (like traditional Malaysian cuisine) and now we've added CTOS scrapers (like adding street food vendors to our kitchen).

---

## 🏗️ **What is This System?**

### **The Big Picture**

The OneCredit Data Pipeline is a **unified data orchestration platform** that:

1. **Processes Company Data** (Original `oc_etl`)
   - Extracts company profiles from PostgreSQL databases
   - Cleans and validates the data
   - Uploads processed data to Snowflake data warehouse

2. **Scrapes Malaysian Business Data** (New CTOS Integration)
   - Collects business registration data from Malaysian government websites
   - Stores data in PostgreSQL with tracking and retry mechanisms
   - Provides comprehensive business intelligence

3. **Orchestrates Everything** (Unified Dagster)
   - Manages all data processing in a single workspace
   - Schedules automated data collection and processing
   - Monitors data quality and system health

### **The Restaurant Metaphor**

Imagine you're running a **professional restaurant** that serves multiple cuisines:

- **Main Kitchen** (`oc_etl`): Traditional Malaysian restaurant kitchen
  - **Chef** (Dagster): Coordinates all cooking processes
  - **Ingredients** (Raw Data): Company profiles from databases
  - **Recipes** (Assets): Data processing steps
  - **Meals** (Jobs): Complete data processing workflows
  - **Storage** (S3/Snowflake): Walk-in freezer and pantry

- **Street Food Station** (CTOS Scrapers): Mobile food vendors
  - **Vendors** (ROB/ROC Scrapers): Collect fresh ingredients from markets
  - **Markets** (Government Websites): Sources of fresh business data
  - **Inventory** (PostgreSQL): Track what we've collected
  - **Quality Control** (Retry Mechanisms): Ensure fresh ingredients

- **Unified Management** (Unified Workspace): Restaurant manager
  - **Single Menu** (Unified Assets): All available dishes
  - **Kitchen Schedule** (Schedules): When to prepare each dish
  - **Staff Coordination** (Jobs): Who does what and when

---

## 🔄 **How Data Flows Through the System**

### **Traditional ETL Flow (Company Data)**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   Data Pipeline │    │   Snowflake     │
│   (Raw Data)    │───▶│   (Processing)  │───▶│   (Data         │
│                 │    │                 │    │    Warehouse)   │
│ • Company       │    │ • Clean Data    │    │ • Analytics     │
│   Profiles      │    │ • Validate      │    │ • Reporting     │
│ • Business      │    │ • Transform     │    │ • ML Models     │
│   Information   │    │ • Enrich        │    │ • Dashboards    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

**Step-by-step process:**
1. **Extract**: Pull company data from PostgreSQL databases
2. **Transform**: Clean, validate, and enrich the data
3. **Load**: Upload processed data to Snowflake for analysis

### **CTOS Scraping Flow (Business Registration Data)**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Government    │    │   CTOS Scrapers │    │   PostgreSQL    │
│   Websites      │───▶│   (Data         │───▶│   (Structured   │
│                 │    │    Collection)  │    │    Storage)     │
│ • ROB Website   │    │ • URL Scraping  │    │ • Business      │
│ • ROC Website   │    │ • Detail        │    │   Registrations │
│ • Business      │    │   Scraping      │    │ • Company       │
│   Directories   │    │ • Error Handling│    │   Profiles      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

**Step-by-step process:**
1. **URL Collection**: Scrape business listing URLs from government websites
2. **Detail Scraping**: Visit each URL to collect detailed business information
3. **Data Storage**: Store structured data in PostgreSQL with tracking
4. **Quality Control**: Retry failed scraping attempts automatically

### **Unified Orchestration Flow**

```
┌─────────────────────────────────────────────────────────────┐
│                UNIFIED DAGSTER WORKSPACE                   │
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐               │
│  │   Daily ETL     │    │  Weekly CTOS    │               │
│  │   Processing    │    │   Scraping      │               │
│  │                 │    │                 │               │
│  │ • Company Data  │    │ • ROB (Monday)  │               │
│  │ • Snowflake     │    │ • ROC (Thursday)│               │
│  │ • Analytics     │    │ • PostgreSQL    │               │
│  │ • Reporting     │    │ • Business Data │               │
│  └─────────────────┘    └─────────────────┘               │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              UNIFIED MONITORING                    │   │
│  │                                                     │   │
│  │ • Asset Status Tracking                            │   │
│  │ • Job Execution Monitoring                         │   │
│  │ • Schedule Management                              │   │
│  │ • Error Handling & Retries                         │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 **Business Value & Impact**

### **Why This System Matters**

#### **For OneCredit**
- **Comprehensive Data**: Access to both internal company data and external business registration data
- **Data Quality**: Automated cleaning and validation ensure reliable insights
- **Scalability**: Cloud-based infrastructure handles growing data needs
- **Compliance**: Proper data handling and storage practices

#### **For Data Analysts**
- **Rich Data Sources**: Combine internal company data with external business intelligence
- **Automated Processing**: Daily ETL and weekly scraping reduce manual work
- **Quality Assurance**: Built-in validation and error handling
- **Real-time Monitoring**: Track data processing status and quality

#### **For Business Intelligence**
- **Market Analysis**: Understand Malaysian business landscape
- **Risk Assessment**: Comprehensive company profiles for credit analysis
- **Competitive Intelligence**: Track business registrations and changes
- **Regulatory Compliance**: Monitor business registration requirements

### **Key Metrics & Performance**

#### **Data Processing Capacity**
- **14 Assets**: Combined data processing and scraping capabilities
- **7 Jobs**: Different workflows for various business needs
- **3 Schedules**: Automated daily and weekly operations
- **Unified Storage**: PostgreSQL for CTOS data, Snowflake for analytics

#### **System Performance**
- **High Throughput**: Process millions of business records
- **Reliability**: 99.9% uptime with automated error recovery
- **Scalability**: Cloud-based infrastructure grows with demand
- **Cost Efficiency**: Optimized resource usage and storage

---

## 🛠️ **Key Technologies & Architecture**

### **Orchestration Layer**
- **Dagster**: Unified data orchestration platform
- **Dagster AWS**: Cloud integration and deployment
- **Dagster PostgreSQL**: Metadata and state management

### **Data Processing Layer**
- **Pandas**: Data manipulation and transformation
- **PyArrow**: High-performance data formats
- **Snowflake**: Cloud data warehouse for analytics
- **PostgreSQL**: Operational data storage

### **Web Scraping Layer**
- **aiohttp**: Asynchronous HTTP requests for high performance
- **BeautifulSoup**: HTML parsing and data extraction
- **Selenium/Playwright**: Dynamic content handling
- **Retry Mechanisms**: Automatic error recovery

### **Infrastructure Layer**
- **AWS ECS Fargate**: Container orchestration
- **AWS RDS**: Managed PostgreSQL databases
- **AWS S3**: Object storage for data and logs
- **Pulumi**: Infrastructure as Code deployment

---

## 📊 **Current System Status**

### **Assets (14 total)**
- **5 ETL Assets**: Company data processing pipeline
- **9 CTOS Assets**: Business registration scraping and orchestration

### **Jobs (7 total)**
- **1 ETL Job**: Daily company data processing
- **5 CTOS Jobs**: ROB/ROC scraping and retry mechanisms
- **1 Unified Job**: Combined workflows

### **Schedules (3 total)**
- **1 Daily Schedule**: Company ETL processing
- **2 Weekly Schedules**: ROB (Monday) and ROC (Thursday) scraping

### **Data Storage**
- **PostgreSQL**: CTOS business data and tracking
- **Snowflake**: Processed company data for analytics
- **S3**: Raw data storage and compute logs

---

## 🔄 **Recent System Updates**

### **Unified Dagster Integration**
- **Combined Workspace**: Single Dagster workspace for all operations
- **Asset Consolidation**: 14 assets from both ETL and scraping systems
- **Unified Monitoring**: Single dashboard for all data operations
- **Simplified Management**: One configuration for all workflows

### **CTOS Scraper Integration**
- **PostgreSQL Storage**: Unified data storage for CTOS data
- **Retry Mechanisms**: Automatic handling of failed scraping attempts
- **Scheduled Operations**: Weekly scraping of ROB and ROC data
- **Error Recovery**: Robust error handling and logging

### **Infrastructure Improvements**
- **Cloud Deployment**: Fully automated AWS deployment
- **Scalability**: Containerized services with auto-scaling
- **Monitoring**: Comprehensive logging and alerting
- **Security**: IAM roles and security groups for access control

---

## 🚀 **Getting Started**

### **For New Team Members**
1. **Read this overview** to understand the system architecture
2. **Set up development environment** using the Dev Container
3. **Explore the Dagster UI** to see all assets and jobs
4. **Review the documentation** for detailed technical information

### **For Data Analysts**
1. **Understand data sources** (ETL vs CTOS scraping)
2. **Learn about data quality** and validation processes
3. **Explore available data** in Snowflake and PostgreSQL
4. **Monitor data processing** through Dagster UI

### **For Developers**
1. **Set up local development** environment
2. **Understand the unified codebase** structure
3. **Learn about adding new assets** and jobs
4. **Review deployment processes** and infrastructure

---

## 📞 **Support & Resources**

### **Documentation**
- **Technical Details**: See other documentation files in this directory
- **Code Examples**: Review source code in `src/` directory
- **Infrastructure**: Check `infra/` directory for deployment details

### **Monitoring & Debugging**
- **Dagster UI**: Real-time monitoring of all data operations
- **CloudWatch Logs**: Detailed execution logs and error tracking
- **Database Monitoring**: PostgreSQL and Snowflake performance metrics

### **Getting Help**
- **Check Documentation**: Start with this overview and related docs
- **Review Logs**: Use Dagster UI and CloudWatch for debugging
- **Team Support**: Reach out to the data engineering team

---

## 🎉 **Conclusion**

The OneCredit Data Pipeline is a **comprehensive, unified data engineering platform** that combines traditional ETL processing with advanced web scraping capabilities. By treating it like a professional restaurant kitchen - with different stations for different cuisines, unified management, and quality control - we've created a system that's both powerful and maintainable.

Whether you're a data analyst looking for insights, a developer adding new features, or a business stakeholder understanding the value, this system provides the tools and infrastructure needed to turn raw data into actionable business intelligence.

**Remember**: Just like a great restaurant, this system takes time to master, but once you understand how all the components work together, you can create amazing data-driven results! 