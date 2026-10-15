# PostgreSQL Integration: Unified Data Storage

## 🎯 **Why PostgreSQL for Everything?**

You're absolutely right to use PostgreSQL for all CTOS data instead of DynamoDB. Here's why this approach is better:

### **Unified Data Architecture**

Instead of having data scattered across different storage systems, PostgreSQL provides a **single source of truth** for all your data:

```
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                          │
├─────────────────────────────────────────────────────────────────┤
│  📊 Internal ETL Data    │    🌐 CTOS Scraper Data           │
│  ├── company_profiles    │    ├── rob_url_master             │
│  ├── user_data          │    ├── rob_registration            │
│  ├── transaction_data   │    ├── roc_url_master              │
│  └── business_data      │    └── roc_registration            │
├─────────────────────────────────────────────────────────────────┤
│                    Shared Resources                            │
│  ├── Tracking tables (rob_tracking, roc_tracking)            │
│  ├── URL master tables (rob_url_master, roc_url_master)      │
│  └── Registration tables (rob_registration, roc_registration) │
└─────────────────────────────────────────────────────────────────┘
```

## 🏗️ **PostgreSQL Schema Design**

### **URL Master Tables**

These tables store all discovered company URLs:

```sql
-- ROB URL Master Table
CREATE TABLE rob_url_master (
    name TEXT,
    url TEXT PRIMARY KEY
);

-- ROC URL Master Table  
CREATE TABLE roc_url_master (
    name TEXT,
    url TEXT PRIMARY KEY
);
```

**What this stores:**
- **Company names** from CTOS listings
- **URLs** to detailed company pages
- **Primary key** on URL for deduplication

### **Registration Details Tables**

These tables store the detailed company information:

```sql
-- ROB Registration Details
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

-- ROC Registration Details
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
```

**What this stores:**
- **Complete company profiles** with registration details
- **Business types** and categories
- **Registration dates** and status
- **Geographic information** (state)

### **Tracking Tables**

These tables manage scraping progress and resume capability:

```sql
-- ROB Tracking Table
CREATE TABLE rob_tracking (
    prefix TEXT,
    page INTEGER,
    status TEXT,
    url TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prefix, page, status)
);

-- ROC Tracking Table
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

**What this stores:**
- **Scraping progress** (which pages completed)
- **Error tracking** (failed pages for retry)
- **Resume points** (where to continue if interrupted)
- **Retry statistics** (how many times failed)

## 🔧 **How PostgreSQL Integration Works**

### **Automatic Table Creation**

The scraper automatically creates tables when needed:

```python
# From src/ctos_scrapers/ctos_scraper/assets/postgres_utils.py
def ensure_url_master_table(table_name: str) -> None:
    """Automatically create URL master table if it doesn't exist."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                name TEXT,
                url TEXT PRIMARY KEY
            )
        """)
        conn.commit()
```

**Benefits:**
- **No manual setup** required
- **Consistent schema** across environments
- **Automatic migrations** when schema changes
- **Safe operations** (IF NOT EXISTS)

### **Efficient Data Operations**

The scraper uses optimized PostgreSQL operations:

```python
# Batch inserts with conflict handling
def insert_urls(table_name: str, rows: list[dict]) -> None:
    """Efficiently insert URLs with deduplication."""
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name} (name, url)
            VALUES %s
            ON CONFLICT (url) DO NOTHING
            """,
            [(row["name"], row["url"]) for row in rows],
        )
        conn.commit()
```

**Features:**
- **Batch operations** for performance
- **Conflict resolution** (no duplicates)
- **Transaction safety** (all or nothing)
- **Connection pooling** for efficiency

### **Progress Tracking and Resume**

```python
# Track scraping progress
def insert_tracking(table_name: str, prefix: str, page: int, status: str, url: str = "") -> None:
    """Track which pages have been scraped."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {table_name} (prefix, page, status, url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (prefix, page, status) DO NOTHING
        """, (prefix, page, status, url))
        conn.commit()

# Resume from last successful point
def fetch_tracking_scraped(table_name: str) -> set:
    """Get all successfully scraped pages."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT prefix, page FROM {table_name} WHERE status='scraped'")
        return {(row[0], row[1]) for row in cur.fetchall()}
```

## 📊 **Data Flow and Integration**

### **Complete Data Pipeline**

```
1. URL Discovery         2. Progress Tracking        3. Details Scraping
     ↓                        ↓                           ↓
Browse CTOS pages    →   Store in tracking    →    Extract company details
(a-z, 0-9 listings)      (rob_tracking)              (rob_registration)
     ↓                        ↓                           ↓
Store URLs in master    →   Resume capability    →    Store in PostgreSQL
(rob_url_master)            (roc_tracking)              (roc_registration)
```

### **Integration with Existing Pipeline**

The CTOS data integrates seamlessly with your existing ETL pipeline:

```python
# From src/unified_definitions.py
def get_all_assets() -> List:
    """Collect all assets from both oc_etl and ctos_scrapers."""
    
    # Existing oc_etl assets (PostgreSQL-based)
    oc_etl_assets = [
        *company_raw_assets,      # From pg_recc
        *company_sanitize_assets, # From pg_portal
        *company_snow_assets,     # To Snowflake
        parsed_from_html,
        cleaned_parquet_df,
    ]
    
    # CTOS scraper assets (PostgreSQL-based)
    ctos_assets = [
        rob_scrape_url,           # To rob_url_master
        rob_scrape_registration,  # To rob_registration
        roc_scrape_url,           # To roc_url_master
        roc_scrape_registration,  # To roc_registration
    ]
    
    return oc_etl_assets + ctos_assets
```

## 🚀 **Benefits of PostgreSQL-Only Approach**

### **1. Unified Data Management**

- **Single database** for all data
- **Consistent backup** strategy
- **Unified monitoring** and logging
- **Simplified maintenance**

### **2. Better Performance**

- **Optimized queries** with indexes
- **Efficient joins** between internal and external data
- **Transaction safety** for data integrity
- **Connection pooling** for scalability

### **3. Enhanced Analytics**

```sql
-- Cross-reference internal and CTOS data
SELECT 
    c.company_name,
    c.registration_number,
    ctos.business_type,
    ctos.state
FROM company_profiles c
LEFT JOIN rob_registration ctos 
    ON c.registration_number = ctos.registration_number
WHERE ctos.business_type = 'Restaurant';
```

### **4. Simplified Operations**

- **One database** to monitor
- **Unified backup** strategy
- **Consistent security** policies
- **Easier troubleshooting**

## 🔧 **Configuration and Setup**

### **Environment Variables**

```bash
# PostgreSQL Configuration
PG_USER=your_postgres_user
PG_PASSWORD=your_postgres_password
PG_HOST=your_postgres_host
PG_PORT=5432
PG_DATABASE=your_database

# CTOS Table Names (optional - defaults used if not set)
ROB_COMPANY_DETAILS=rob_registration
ROB_URLS=rob_url_master
ROC_COMPANY_DETAILS=roc_registration
ROC_URLS=roc_url_master
```

### **Database Permissions**

Ensure your PostgreSQL user has these permissions:

```sql
-- Create tables
GRANT CREATE ON DATABASE your_database TO your_user;

-- Insert/update data
GRANT INSERT, UPDATE, SELECT ON ALL TABLES IN SCHEMA public TO your_user;

-- Future tables (for auto-created tables)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, SELECT ON TABLES TO your_user;
```

### **Performance Optimization**

```sql
-- Add indexes for better performance
CREATE INDEX idx_rob_registration_business_name ON rob_registration (business_name);
CREATE INDEX idx_roc_registration_business_name ON roc_registration (business_name);
CREATE INDEX idx_rob_tracking_status ON rob_tracking (status);
CREATE INDEX idx_roc_tracking_status ON roc_tracking (status);
```

## 📈 **Monitoring and Maintenance**

### **Database Statistics**

```sql
-- Check table sizes
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats 
WHERE tablename LIKE '%rob%' OR tablename LIKE '%roc%';

-- Check scraping progress
SELECT 
    'rob_url_master' as table_name,
    COUNT(*) as total_urls
FROM rob_url_master
UNION ALL
SELECT 
    'rob_registration' as table_name,
    COUNT(*) as total_companies
FROM rob_registration;
```

### **Performance Monitoring**

```sql
-- Check slow queries
SELECT 
    query,
    calls,
    total_time,
    mean_time
FROM pg_stat_statements 
WHERE query LIKE '%rob%' OR query LIKE '%roc%'
ORDER BY mean_time DESC;
```

## 🎯 **Migration from DynamoDB**

If you were previously using DynamoDB, the migration is straightforward:

1. **Data Export**: Export existing DynamoDB data
2. **Schema Mapping**: Map DynamoDB attributes to PostgreSQL columns
3. **Data Import**: Use the PostgreSQL writer to import data
4. **Validation**: Verify data integrity after migration

### **Migration Script Example**

```python
# Example migration from DynamoDB to PostgreSQL
def migrate_dynamodb_to_postgres():
    """Migrate existing DynamoDB data to PostgreSQL."""
    
    # Create PostgreSQL writer
    writer = CTOSPostgresWriter(ROB_COMPANY_DETAILS)
    
    # Export from DynamoDB
    dynamo_items = dynamo_writer.fetch_all_items()
    
    # Transform and import to PostgreSQL
    for item in dynamo_items:
        transformed_item = transform_dynamo_item(item)
        writer.write_item(transformed_item)
    
    print(f"Migrated {len(dynamo_items)} items to PostgreSQL")
```

## 🎪 **The Complete Picture**

With PostgreSQL-only storage, your data architecture is now:

```
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                          │
├─────────────────────────────────────────────────────────────────┤
│  📊 Internal Data         │    🌐 External CTOS Data         │
│  ├── company_profiles     │    ├── rob_url_master            │
│  ├── user_data           │    ├── rob_registration           │
│  ├── transaction_data    │    ├── roc_url_master             │
│  └── business_data       │    └── roc_registration           │
├─────────────────────────────────────────────────────────────────┤
│                    Unified Analytics                            │
│  ├── Cross-reference queries                                  │
│  ├── Market analysis                                         │
│  ├── Risk assessment                                         │
│  └── Business intelligence                                   │
└─────────────────────────────────────────────────────────────────┘
```

This PostgreSQL-only approach provides a **unified, performant, and maintainable** data architecture that scales with your business needs while keeping all your data in one place for comprehensive analytics and insights. 