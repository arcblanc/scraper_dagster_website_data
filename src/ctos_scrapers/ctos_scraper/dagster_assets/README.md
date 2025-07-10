# CTOS Scraper Dagster Assets

This directory contains simplified Dagster assets for orchestrating the CTOS scraping pipeline with essential functionality for retry and resume operations.

## Overview

The assets are organized into two main categories:

1. **Main Scraping Assets** - Core scraping tasks for URLs and details
2. **Retry/Resume Assets** - Essential retry and resume functionality

## Asset Structure

### Main Scraping Assets

#### ROB Assets (`rob_assets.py`)
- `scrape_url` - Scrape ROB company listing URLs (rob_url_master)
- `scrape_registration` - Scrape ROB company registration details (rob_registration)

#### ROC Assets (`roc_assets.py`)
- `scrape_url` - Scrape ROC company listing URLs (roc_url_master)
- `scrape_registration` - Scrape ROC company registration details (roc_registration)

### Retry/Resume Assets (`orchestrator_assets.py`)

- `retry_all_failed_urls` - Retry all failed URL pages for both scrapers with parallel processing
- `resume_all_urls_from_last` - Resume URL scraping from last point for both scrapers

## Configuration Options

### URL Scraper Configuration

```python
class URLScraperConfig(Config):
    retry_failed: bool = False
    resume_from: Optional[str] = None
    resume_from_last: bool = False
```

## Usage Examples

### 1. Basic Scraping

```bash
# Scrape ROB URLs and details
dagster job execute -f definitions.py -j rob_scraping_job

# Scrape ROC URLs and details
dagster job execute -f definitions.py -j roc_scraping_job
```

### 2. Retry Failed Pages

```bash
# Retry failed pages for both scrapers
dagster job execute -f definitions.py -j retry_failed_job
```

### 3. Resume from Last Point

```bash
# Resume both scrapers from last point
dagster job execute -f definitions.py -j resume_from_last_job
```

### 4. Scraping with Configuration

```bash
# Scrape ROB URLs with custom config
dagster job execute -f definitions.py -j rob_scraping_job \
  --config '{"ops": {"scrape_url": {"config": {"retry_failed": true, "resume_from_last": true}}}}'
```

## Available Jobs

### Basic Scraping Jobs
- `rob_scraping_job` - Complete ROB scraping pipeline (URLs + details)
- `
