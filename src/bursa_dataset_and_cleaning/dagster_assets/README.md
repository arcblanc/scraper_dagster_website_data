# Dagster Assets for Bursa Dataset Pipeline

This directory contains Dagster assets that orchestrate the Bursa Malaysia data pipeline using the restructured layers.

## Structure

```
dagster_assets/
├── __init__.py
├── definitions.py          # Main Dagster definitions
├── README.md              # This file
└── assets/
    ├── __init__.py
    ├── list_of_ids_assets.py      # Company ID scraping assets
    ├── scraping_layer_assets.py   # Data scraping assets
    └── sql_inject_assets.py       # Database injection assets
```

## Asset Groups

### 1. list_of_ids
- **bursa_company_list**: Scrapes company IDs from all Bursa Malaysia markets
- **market_specific_lists**: Individual company lists for each market (main, ACE, LEAP)

### 2. scraping_layer
- **company_urls**: Finds Bursa URLs for companies using their IDs
- **balance_sheet_data**: Scrapes balance sheet data from Bursa companies
- **income_statement_data**: Scrapes income statement data from Bursa companies
- **cash_flow_data**: Scrapes cash flow data from Bursa companies
- **profile_data**: Scrapes profile and management data from Bursa companies
- **market_data**: Scrapes market cap and volume data from Bursa companies
- **ssm_matched_companies**: Matches companies with SSM registration data

### 3. sql_inject
- **balance_sheet_sql_injection**: Injects balance sheet data into SQL database
- **income_statement_sql_injection**: Injects income statement data into SQL database
- **cash_flow_sql_injection**: Injects cash flow data into SQL database
- **profile_sql_injection**: Injects profile data into SQL database
- **market_data_sql_injection**: Injects market data into SQL database
- **master_sql_injection**: Master SQL injection job that runs all injection scripts

## Usage

### Running Individual Jobs

```bash
# Run only company ID scraping
dagster job execute -f definitions.py -j list_of_ids_job

# Run only data scraping
dagster job execute -f definitions.py -j scraping_layer_job

# Run only SQL injection
dagster job execute -f definitions.py -j sql_inject_job

# Run full pipeline
dagster job execute -f definitions.py -j full_bursa_pipeline
```

### Running Individual Assets

```bash
# Run specific asset
dagster asset materialize -f definitions.py -a bursa_company_list

# Run asset with dependencies
dagster asset materialize -f definitions.py -a balance_sheet_data
```

### Starting Dagster UI

```bash
# Start Dagster UI
dagster dev -f definitions.py
```

## Dependencies

The assets use the existing Python scripts in the restructured layers:
- `list_of_ids/`: Company ID scraping scripts
- `scraping_layer/`: Data scraping scripts
- `sql_inject/`: Database injection scripts

## Data Flow

1. **list_of_ids** → Scrapes company IDs from Bursa Malaysia
2. **scraping_layer** → Uses company IDs to find URLs and scrape financial data
3. **sql_inject** → Processes scraped data and injects into SQL database

## Configuration

Make sure your environment variables are set for database connections:
- `PG_USER`
- `PG_PASSWORD`
- `PG_HOST`
- `PG_PORT`
- `PG_DATABASE`

## Notes

- The assets execute the existing Python scripts as subprocesses
- Each asset includes metadata about the data processed
- Error handling is included for each asset
- The structure allows for granular execution of pipeline components

SELECT
    b.registration_number,
    b.fiscal_date,
    b.total_assets * 1000000 AS total_assets,

    CASE
        WHEN b.retained_earnings_accumulated_deficit ~ ',' THEN
            REPLACE(b.retained_earnings_accumulated_deficit, ',', '')::NUMERIC * 1000000
        ELSE
            b.retained_earnings_accumulated_deficit::NUMERIC * 1000000
    END AS retained_earning,

    CASE
        WHEN b.total_liabilities ~ ',' THEN
            REPLACE(b.total_liabilities, ',', '')::NUMERIC * 1000000
        ELSE
            b.total_liabilities::NUMERIC * 1000000
    END AS total_liabilities,

    CASE
        WHEN b.total_equity ~ ',' THEN
            REPLACE(b.total_equity, ',', '')::NUMERIC * 1000000
        ELSE
            b.total_equity::NUMERIC * 1000000
    END AS networth,

    CASE
        WHEN i.net_income_before_taxes ~ ',' THEN
            REPLACE(i.net_income_before_taxes, ',', '')::NUMERIC * 1000000
        ELSE
            i.net_income_before_taxes::NUMERIC * 1000000
    END AS pbt,

    CASE
        WHEN i.net_income_after_taxes ~ ',' THEN
            REPLACE(i.net_income_after_taxes, ',', '')::NUMERIC * 1000000
        ELSE
            i.net_income_after_taxes::NUMERIC * 1000000
    END AS pat

FROM
    public_complete_balance_sheet b
LEFT JOIN public_complete_income i
    ON i.registration_number = b.registration_number
    AND i.fiscal_date = b.fiscal_date
WHERE
    b.registration_number::text IN (
        SELECT registration_number::text
        FROM public_complete_company_profile
    )
    AND b.registration_number IN (
        '198401014370',  -- AEON CO. (M) BHD
        '201001034084',  -- MR D.I.Y. GROUP (M) BERHAD
        '196501000672',  -- PUBLIC BANK BHD
        '199101019838',  -- MAH SING GROUP BERHAD
        '197301002148',  -- PETRONAS DAGANGAN BERHAD
        '197901003918',  -- KOSSAN RUBBER INDUSTRIES BERHAD
        '199801018294',  -- TOP GLOVE CORPORATION BHD.
        '200901024473',  -- MAXIS BERHAD
        '193401000023',  -- HONG LEONG BANK BHD
        '198401016183',  -- TELEKOM MALAYSIA BHD
        '199701009694',  -- CELCOMDIGI BERHAD
        '195601000197',  -- CIMB GROUP HOLDINGS BERHAD
        '197401002663',  -- SP SETIA BHD
        '200601022130',  -- HARTALEGA HOLDINGS BHD
        '202301017784',  -- 99 SPEED MART RETAIL HOLDINGS BERHAD
        '196001000142'   -- MALAYAN BANKING BERHAD
    );

    This is to see data for the onecredit portal.
