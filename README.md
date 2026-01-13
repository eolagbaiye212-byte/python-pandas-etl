# Tech Layoffs ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for analyzing tech company layoffs data from 2022-2023. This project follows a medallion architecture (Bronze/Silver/Gold) to process raw data through multiple transformation layers and load it into a PostgreSQL data warehouse with a star schema design.

## Project Overview

This ETL pipeline processes layoff data through three medallion layers:

- **Bronze Layer**: Raw data ingestion (minimal transformation)
- **Silver Layer**: Data cleaning and quality improvements
- **Gold Layer**: Star schema modeling for analytics

The processed data is loaded into a PostgreSQL database for analytical queries and reporting.

## Architecture

### Medallion Architecture

```
Raw Data (CSV) → Bronze Layer → Silver Layer → Gold Layer → PostgreSQL Database
```

### Star Schema Design

The Gold layer creates a dimensional model with:

- **Fact Table**: `fact_layoffs` - Contains layoff metrics and foreign keys
- **Dimension Tables**:
  - `dim_company` - Company information and status
  - `dim_date` - Date attributes (year, month, quarter)
  - `dim_industry` - Industry classifications
  - `dim_location` - Company headquarters locations

## Project Structure

```
Tech_Layoffs_Project/
├── main.py                      # Orchestrates the entire ETL pipeline
├── etl_log.txt                 # Logs all ETL process steps with timestamps
├── tech_layoffs_bronze.csv     # Bronze layer output
├── tech_layoffs_silver.csv     # Silver layer output
├── README.md                   # This file
├── database/
│   ├── __init__.py
│   ├── get_engine.py           # Database connection setup
│   └── load_into_db.py         # Loads dataframes into PostgreSQL
├── etl/
│   ├── __init__.py
│   ├── ingest_csv.py           # Bronze layer - CSV ingestion
│   ├── clean_layoffs.py        # Silver layer - Data cleaning & transformation
│   ├── star_schema.py          # Gold layer - Star schema creation
│   ├── log_progress.py         # Logs ETL step timestamps
│   └── reset_log.py            # Resets ETL log file
└── sql/
    ├── __init__.py
    ├── validation_queries.py   # Data quality validation SQL queries
    └── execute_validation.py   # Executes and displays validation results
```

## Data Processing Pipeline

### 1. Ingestion (Bronze Layer - `etl/ingest_csv.py`)
- Reads raw CSV file from source
- Minimal transformation - primarily copies data to Bronze layer
- Output: `tech_layoffs_bronze.csv`

### 2. Cleaning (Silver Layer - `etl/clean_layoffs.py`)
- Converts 'Unclear' values to NaN for numeric columns
- Creates quality tracking columns:
  - `known_total_layoffs` - Boolean flag for data completeness
  - `known_impacted_workforce_percentage` - Boolean flag for data completeness
- Normalizes industry column (lowercase, strips whitespace)
- Converts date strings to datetime format
- Handles missing values in notes
- Enforces schema types
- Output: `tech_layoffs_silver.csv`

### 3. Modeling (Gold Layer - `etl/star_schema.py`)
- Creates dimension tables from Silver data
- Generates fact table linking to dimensions via foreign keys
- Returns dictionary of all tables ready for database loading

### 4. Loading (`database/load_into_db.py`)
- Iterates through dimension and fact tables
- Writes each table to PostgreSQL database
- Uses `if_exists='replace'` mode for table creation/refresh

### 5. Validation (`sql/validation_queries.py` & `sql/execute_validation.py`)
- Validates dimension table primary keys for nulls
- Verifies total record counts match source data
- Validates all foreign keys have matching dimension entries
- Flags outlier layoff counts (>50,000 or negative)
- Checks data consistency for companies appearing multiple times
- Displays validation results with source data comparison

### 6. Logging (`etl/log_progress.py` & `etl/reset_log.py`)
- Records each ETL step with timestamps in `etl_log.txt`
- Enables audit trail and troubleshooting
- Resets log file at pipeline start

## Setup Instructions

### Prerequisites

- Python 3.7+
- PostgreSQL database
- Required Python packages:
  - `pandas`
  - `sqlalchemy`
  - `psycopg2-binary`

### Installation

1. **Install dependencies**:
   ```bash
   pip install pandas sqlalchemy psycopg2-binary
   ```

2. **Create PostgreSQL database**:
   ```sql
   CREATE DATABASE techlayoffs;
   ```

3. **Set environment variables**:
   ```
   PGUSER=postgres
   PGPASSWORD=your_password
   PGHOST=localhost
   PGPORT=5432
   PGDATABASE=techlayoffs
   ```

   On Windows (PowerShell):
   ```powershell
   $env:PGUSER='postgres'
   $env:PGPASSWORD='your_password'
   $env:PGHOST='localhost'
   $env:PGPORT='5432'
   $env:PGDATABASE='techlayoffs'
   ```

4. **Update data paths in `main.py`**:
   - Modify `RAW`, `BRONZE`, and `SILVER` paths as needed

### Running the Pipeline

```bash
python main.py
```

This will execute all stages in sequence:
1. Reset ETL log and log process start
2. Ingest raw CSV to Bronze layer
3. Clean and transform to Silver layer
4. Create star schema (Gold layer)
5. Establish database connection
6. Load all tables to PostgreSQL
7. Execute data quality validation queries
8. Log validation completion

The pipeline maintains an audit trail in `etl_log.txt` with timestamps for each step.

## Data Dictionary

### Fact Table: `fact_layoffs`
| Column | Type | Description |
|--------|------|-------------|
| company_ID | INTEGER | Foreign key to dim_company |
| industry_ID | INTEGER | Foreign key to dim_industry |
| location_ID | INTEGER | Foreign key to dim_location |
| date_ID | INTEGER | Foreign key to dim_date |
| total_layoffs | FLOAT | Number of employees laid off |
| known_total_layoffs | INTEGER | Flag indicating if total_layoffs is known |
| impacted_workforce_percentage | FLOAT | Percentage of workforce impacted |
| known_impacted_workforce_percentage | INTEGER | Flag indicating if percentage is known |

### Dimension Table: `dim_company`
| Column | Type | Description |
|--------|------|-------------|
| company_ID | INTEGER | Primary key |
| company | VARCHAR | Company name |
| status | VARCHAR | Company status |
| sources | VARCHAR | Data sources |

### Dimension Table: `dim_date`
| Column | Type | Description |
|--------|------|-------------|
| date_ID | INTEGER | Primary key |
| date | DATE | Reported date |
| year | INTEGER | Year of report |
| month | INTEGER | Month of report |
| day | DATE | Day of report |
| quarter | INTEGER | Quarter of report |

### Dimension Table: `dim_industry`
| Column | Type | Description |
|--------|------|-------------|
| industry_ID | INTEGER | Primary key |
| industry | VARCHAR | Industry classification |

### Dimension Table: `dim_location`
| Column | Type | Description |
|--------|------|-------------|
| location_ID | INTEGER | Primary key |
| headquarter_location | VARCHAR | Company headquarters location |

## Key Features

✓ **Data Quality Tracking** - Flags for unknown values allow downstream analysis to handle missing data appropriately

✓ **Normalized Data** - Industry names standardized for consistent aggregations

✓ **Temporal Analysis** - Date dimension enables time-series analysis at multiple granularities

✓ **Dimensional Modeling** - Star schema optimizes analytical queries

✓ **Environment Configuration** - Database credentials managed via environment variables for security

✓ **Data Validation** - Comprehensive SQL validation queries verify data integrity post-load

✓ **ETL Logging** - Timestamped audit trail of all pipeline steps for monitoring and troubleshooting

✓ **Modular Architecture** - Organized into logical modules (etl/, database/, sql/) for maintainability

## Notes

- The pipeline assumes the raw source CSV exists at the path specified in `main.py`
- Database connection details are read from environment variables (with sensible defaults for `PGUSER`, `PGHOST`, `PGPORT`, and `PGDATABASE`)
- `PGPASSWORD` is required and must be set
- The pipeline uses `if_exists='replace'` mode, which drops and recreates tables on each run
- Numeric columns with 'Unclear' values are converted to NaN for proper statistical operations
- ETL progress is logged with timestamps in `etl_log.txt` for audit trail purposes
- Validation queries are automatically executed after data loading to verify data integrity

## Validation Queries

The pipeline includes comprehensive validation checks:

1. **Primary Key Nulls** - Ensures all dimension table primary keys are non-null
2. **Record Count** - Verifies fact table contains correct number of records
3. **Foreign Key Integrity** - Confirms all fact table foreign keys reference existing dimension entries
4. **Outlier Detection** - Flags layoff counts that seem impossibly high (>50,000) or negative
5. **Consistency Check** - Validates that companies appearing multiple times have consistent attributes
6. **Source Comparison** - Compares total records against raw source data

## Future Enhancements

- Implement incremental loading (upsert) instead of full replacement
- Create SQL views for common analytical queries
- Implement error handling and retry logic

## License

This project is for educational purposes in data engineering practice.
