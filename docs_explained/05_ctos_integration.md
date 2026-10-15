# CTOS Scraper Integration: Adding External Data Sources

## What is CTOS?

**CTOS** (Credit Tip-Off Service) is Malaysia's leading credit reporting agency. Think of it as a **comprehensive business directory** that contains detailed information about companies and businesses in Malaysia. It's like having access to a massive phone book, but instead of just phone numbers, you get detailed business information like registration numbers, business types, and financial data.

## 🎯 **Why Integrate CTOS Data?**

### **Business Value**

Imagine you're running a restaurant and you want to know everything about your suppliers, competitors, and potential customers. CTOS data is like having a **business intelligence database** that tells you:

- **Company registration details** (like business licenses)
- **Business types and categories** (restaurant, manufacturing, etc.)
- **Registration dates** (how long they've been in business)
- **Financial information** (credit ratings, payment history)
- **Contact information** (addresses, phone numbers)

### **Data Pipeline Enhancement**

By integrating CTOS data into your existing pipeline, you're adding a **rich external data source** that complements your internal company data:

```
Existing Data Sources     +     CTOS Data     =     Enhanced Analytics
├── Internal databases         ├── ROB (Business)         ├── Complete company profiles
├── User data                 ├── ROC (Company)          ├── Credit risk assessment
├── Company profiles          └── Registration details    ├── Market analysis
└── Transaction data                                      └── Business intelligence
```

## 🏗️ **CTOS Scraper Architecture**

### **The Scraping Pipeline**

The CTOS scraper works like a **data harvesting machine** that systematically collects information from CTOS's public directories:

```
Step 1: URL Discovery          Step 2: Data Extraction          Step 3: Storage
     ↓                              ↓                              ↓
Browse CTOS directories    →    Extract company details    →    Store in database
(a-z, 0-9 listings)              (registration, type, etc.)      (PostgreSQL/DynamoDB)
```

### **Two Types of Data**

The scraper collects two main types of data:

1. **ROB (Business)** - Individual business registrations
   - Small businesses, sole proprietorships
   - Personal business registrations
   - Individual trader information

2. **ROC (Company)** - Company registrations
   - Limited companies, corporations
   - Public and private companies
   - Corporate business entities

## 🔧 **How the Scraper Works**

### **URL Scraping Phase**

```python
# From src/ctos_scrapers/ctos_scraper/assets/scrape_rob_url.py
async def run_parallel_scraper(resume_from: str | None = None, retry_failed: bool = False):
    """Scrape ROB company listing URLs with parallel processing."""
    
    # Like systematically going through a phone book
    for prefix in ['a', 'b', 'c', ..., 'z', '0', '1', ..., '9']:
        # Skip if we're resuming from a specific point
        if resume_from and prefix < resume_from:
            continue
            
        # Scrape all pages for this prefix
        await scrape_prefix_pages(prefix)
```

**What this does:**
- **Systematically visits** each letter/number in the CTOS directory
- **Extracts company URLs** from listing pages
- **Handles pagination** (multiple pages per letter)
- **Tracks progress** for resume capability
- **Parallel processing** for speed

### **Details Scraping Phase**

```python
# From src/ctos_scrapers/ctos_scraper/assets/scrape_rob.py
class LocalROBScraper:
    """Scrape detailed ROB company information."""
    
    async def run(self):
        # Read URLs from the master list
        urls = self.load_urls_from_csv()
        
        for url in urls:
            # Visit each company's detailed page
            company_data = await self.scrape_company_details(url)
            
            # Extract registration information
            registration_number = company_data.get('registration_number')
            business_type = company_data.get('business_type')
            registration_date = company_data.get('registration_date')
            
            # Store in database
            self.save_to_database(company_data)
```

**What this does:**
- **Visits each company's detailed page**
- **Extracts specific fields** (registration number, business type, etc.)
- **Handles different data formats**
- **Stores results** in PostgreSQL or DynamoDB
- **Tracks progress** and handles errors

## 🎭 **Dagster Integration**

### **Unified Asset Structure**

The CTOS scraper is integrated into your existing Dagster pipeline through **unified assets**:

```python
# From src/unified_definitions.py
def get_all_assets() -> List:
    """Collect all assets from both oc_etl and ctos_scrapers."""
    
    # Existing oc_etl assets
    oc_etl_assets = [
        *company_raw_assets,
        *company_sanitize_assets,
        *company_snow_assets,
        parsed_from_html,
        cleaned_parquet_df,
    ]
    
    # CTOS scraper assets
    ctos_assets = [
        rob_scrape_url,           # ROB URL scraping
        rob_scrape_registration,  # ROB details scraping
        roc_scrape_url,           # ROC URL scraping
        roc_scrape_registration,  # ROC details scraping
        full_scraping_pipeline,   # Complete pipeline
        retry_all_failed_urls,    # Error recovery
        resume_all_urls_from_last, # Resume capability
    ]
    
    return oc_etl_assets + ctos_assets
```

### **Asset Dependencies**

The CTOS assets follow a logical dependency chain:

```
rob_scrape_url → rob_scrape_registration
     ↓                    ↓
URL Discovery    →    Details Extraction    →    Database Storage
```

**Why this matters:**
- **URLs must be discovered** before details can be scraped
- **Dagster ensures** the right order of execution
- **Parallel processing** possible for different data types
- **Error handling** at each step

## 🚀 **Running the CTOS Scraper**

### **Individual Asset Execution**

You can run specific CTOS assets through the Dagster UI:

1. **ROB URL Scraping**: `rob_scrape_url`
   - Discovers all ROB business URLs
   - Takes several hours to complete
   - Can be resumed if interrupted

2. **ROB Details Scraping**: `rob_scrape_registration`
   - Extracts detailed information from each URL
   - Depends on URL scraping being complete
   - Stores results in database

3. **ROC URL Scraping**: `roc_scrape_url`
   - Discovers all ROC company URLs
   - Similar to ROB but for companies

4. **ROC Details Scraping**: `roc_scrape_registration`
   - Extracts detailed company information
   - Stores in database

### **Complete Pipeline Execution**

```python
# From src/ctos_scrapers/ctos_scraper/dagster_assets/orchestrator_assets.py
@asset
def full_scraping_pipeline(config: ScraperConfig) -> dict:
    """Complete scraping pipeline with configurable options."""
    
    # Step 1: URL Discovery
    run_url_scraping(config, resume_from)
    
    # Step 2: Details Extraction (if enabled)
    if config.include_details:
        run_details_scraping(config)
    
    return {
        "rob_urls": "completed" if config.scraper_type in ["rob", "both"] else "skipped",
        "roc_urls": "completed" if config.scraper_type in ["roc", "both"] else "skipped",
        "rob_details": "completed" if config.include_details and config.scraper_type in ["rob", "both"] else "skipped",
        "roc_details": "completed" if config.include_details and config.scraper_type in ["roc", "both"] else "skipped",
    }
```

### **Configuration Options**

The scraper supports various configuration options:

```python
class ScraperConfig(Config):
    """Unified configuration for both ROB and ROC scrapers."""
    
    scraper_type: Literal["rob", "roc", "both"] = "both"
    retry_failed: bool = False
    resume_from: str | None = None
    resume_from_last: bool = False
    include_details: bool = True
```

**Configuration options:**
- **`scraper_type`**: Run ROB, ROC, or both
- **`retry_failed`**: Only retry previously failed pages
- **`resume_from`**: Resume from specific letter/number
- **`resume_from_last`**: Resume from last successful point
- **`include_details`**: Whether to scrape detailed information

## 📊 **Data Output and Storage**

### **PostgreSQL Tracking Tables**

The scraper maintains tracking information in PostgreSQL tables:

```
PostgreSQL Tables:
├── rob_url_master              # All discovered ROB URLs
├── rob_registration            # ROB company details
├── rob_tracking               # Progress tracking for ROB
├── roc_url_master             # All discovered ROC URLs
├── roc_registration           # ROC company details
└── roc_tracking               # Progress tracking for ROC
```

### **Database Storage**

Data is stored in PostgreSQL for all tracking and data storage:

1. **PostgreSQL**: For structured querying, analysis, and tracking
   - URL master tables (rob_url_master, roc_url_master)
   - Registration details tables (rob_registration, roc_registration)
   - Tracking tables (rob_tracking, roc_tracking)
2. **S3**: For backup and archival purposes

### **Data Schema**

The scraped data includes:

```python
company_data = {
    "registration_number": "12345678-X",
    "business_name": "ABC Restaurant Sdn Bhd",
    "business_type": "Restaurant",
    "registration_date": "2020-01-15",
    "address": "123 Main Street, Kuala Lumpur",
    "status": "Active",
    "source": "CTOS ROB",
    "scraped_date": "2024-01-15"
}
```

## 🔄 **Integration with Existing Pipeline**

### **Unified Jobs**

The integration creates unified jobs that combine both systems:

```python
unified_company_job = define_asset_job(
    name="unified_company_etl_job",
    selection=[
        # Existing company ETL assets
        "raw_company_profiles",
        "sanitized_company_profiles", 
        "uploaded_to_snowflake",
        # CTOS scraping assets
        "rob_scrape_url",
        "rob_scrape_registration",
        "roc_scrape_url", 
        "roc_scrape_registration",
    ]
)
```

### **Scheduled Execution**

The scraper runs on a schedule:

```python
ctos_rob_schedule = ScheduleDefinition(
    job=ctos_rob_job, 
    cron_schedule="0 7 * * 5",  # Every Friday 7 AM
    description="Weekly ROB scraping schedule"
)
```

**Why weekly?**
- **CTOS data changes slowly** (new registrations, updates)
- **Respectful scraping** (don't overwhelm their servers)
- **Resource management** (scraping is resource-intensive)
- **Error recovery** (time to fix issues between runs)

## 🛠️ **Error Handling and Recovery**

### **Resume Capability**

The scraper can resume from where it left off:

```python
@asset
def resume_all_urls_from_last() -> None:
    """Resume scraping from the last successful point."""
    
    # Get last successful point for ROB
    rob_tracker = URLScrapeTracker("rob_tracking")
    rob_resume_point = rob_tracker.get_resume_point()
    
    # Get last successful point for ROC
    roc_tracker = URLScrapeTracker("roc_tracking")
    roc_resume_point = roc_tracker.get_resume_point()
    
    # Resume scraping from these points
    run_url_scraping_with_resume(rob_resume_point, roc_resume_point)
```

### **Retry Failed Pages**

```python
@asset
def retry_all_failed_urls() -> None:
    """Retry all failed URL pages for both ROB and ROC."""
    
    # Retry ROB failed pages
    asyncio.run(rob_url_scraper(retry_failed=True))
    
    # Retry ROC failed pages
    asyncio.run(roc_url_scraper(retry_failed=True))
```

## 📈 **Monitoring and Observability**

### **Progress Tracking**

The scraper provides detailed progress information:

- **URL discovery progress** (which letters/numbers completed)
- **Details scraping progress** (how many companies processed)
- **Error rates** (failed pages, retry attempts)
- **Performance metrics** (scraping speed, success rate)

### **Logging and Debugging**

```python
logger.info("🚀 Starting ROB URL scraper with config: %s", config)
logger.info("📍 Resuming from last point: %s", resume_from)
logger.info("✅ Parallel retry of all failed URLs completed")
```

## 🎯 **Business Impact**

### **Enhanced Data Coverage**

By integrating CTOS data, you now have:

- **Complete company profiles** from multiple sources
- **External validation** of internal data
- **Market intelligence** for business decisions
- **Risk assessment** capabilities

### **Data Quality Improvements**

- **Cross-reference** internal data with external sources
- **Identify data gaps** in your existing systems
- **Validate company information** accuracy
- **Enrich customer profiles** with external data

### **Analytics Opportunities**

- **Market analysis** (company density by location)
- **Industry trends** (business type distribution)
- **Credit risk assessment** (using CTOS credit data)
- **Competitive intelligence** (understanding the market)

## 🔧 **Setup and Configuration**

### **Environment Variables**

Add these to your `.env` file:

```bash
# PostgreSQL Configuration (for CTOS scraper)
PG_USER=your_postgres_user
PG_PASSWORD=your_postgres_password
PG_HOST=your_postgres_host
PG_PORT=5432
PG_DATABASE=your_database

# CTOS Table Names (optional - defaults are used if not set)
ROB_COMPANY_DETAILS=rob_registration
ROB_URLS=rob_url_master
ROC_COMPANY_DETAILS=roc_registration
ROC_URLS=roc_url_master

# Dagster Configuration
DAGSTER_HOME=/workspace/.dagster
```

### **Database Setup**

The scraper automatically creates these PostgreSQL tables when needed:

```sql
-- URL Master Tables (automatically created)
CREATE TABLE rob_url_master (
    name TEXT,
    url TEXT PRIMARY KEY
);

CREATE TABLE roc_url_master (
    name TEXT,
    url TEXT PRIMARY KEY
);

-- Registration Details Tables (automatically created)
CREATE TABLE rob_registration (
    id TEXT,
    url TEXT,
    business_name TEXT,
    registration_number_old TEXT,
    registration_number TEXT PRIMARY KEY,
    business_type TEXT,
    business_commenced TEXT,
    state TEXT
);

CREATE TABLE roc_registration (
    id TEXT,
    url TEXT,
    business_name TEXT,
    registration_number_old TEXT,
    registration_number TEXT PRIMARY KEY,
    business_type TEXT,
    business_commenced TEXT,
    state TEXT
);

-- Tracking Tables (automatically created)
CREATE TABLE rob_tracking (
    prefix TEXT,
    page INTEGER,
    status TEXT,
    url TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prefix, page, status)
);

CREATE TABLE roc_tracking (
    prefix TEXT,
    page INTEGER,
    status TEXT,
    url TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prefix, page, status)
);
```

### **Running the Integration**

1. **Start Dagster with unified definitions**:
   ```bash
   dagster dev -m src.unified_definitions
   ```

2. **Access the UI**: Open http://localhost:3000

3. **Run CTOS assets**: Materialize the scraping assets

4. **Monitor progress**: Check logs and asset status

## 🎪 **The Complete Picture**

With CTOS integration, your data pipeline now looks like this:

```
External Data Sources    →    Data Processing    →    Business Intelligence
├── CTOS ROB/ROC        →    Dagster Assets    →    Analytics & Reports
├── Internal Databases   →    Data Cleaning     →    Risk Assessment
├── User Data           →    Data Validation   →    Market Analysis
└── Transaction Data    →    Data Storage      →    Decision Making
```

This integration transforms your data pipeline from **internal-only** to **comprehensive business intelligence**, providing a complete view of the Malaysian business landscape for OneCredit's operations.

The CTOS scraper is now fully integrated with your existing Dagster pipeline, creating a unified data ecosystem that combines internal and external data sources for comprehensive business intelligence. 