# This file orchestrates the ETL pipeline. It runs all the modules made for this ETL.

# Import the other python modules into the main
from etl.ingest_csv import ingest_csv
from etl.clean_layoffs import clean_layoffs
from etl.star_schema import star_schema
from etl.log_progress import log_progress
from etl.reset_log import reset_log
from database.get_engine import get_engine
from database.load_into_db import load_into_db
from sql.execute_validation import execute_validation

# Define the RAW, BRONZE, and SILVER output paths
RAW = "C:/Users/eolag/OneDrive/Documents/Data Engineering/tech_layoffs.csv"
BRONZE = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Practice ETL_ELT/Tech_Layoffs_Project/tech_layoffs_bronze.csv"
SILVER = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Practice ETL_ELT/Tech_Layoffs_Project/tech_layoffs_silver.csv"

# Define file path for logging of etl processes
log_file = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Practice ETL_ELT/Tech_Layoffs_Project/etl_log.txt"

def main():
    reset_log(log_file) # Reset ETL logging
    log_progress(log_file, ' ETL process started')

    # Execute Bronze layer
    ingest_csv(RAW, BRONZE)
    log_progress(log_file, ' Bronze layer complete')

    # Execute Silver layer
    clean_layoffs(BRONZE, SILVER)
    log_progress(log_file, ' Silver layer complete')
    # Execute Gold layer
    tables = star_schema(SILVER)
    log_progress(log_file, ' Gold layer complete')

# Ensure database is created in Postgre and environmental variables are created prior to running this module

    engine = get_engine() # Establish connection
    log_progress(log_file, ' Connection to PostgreSQL database established')

    load_into_db(engine, tables) # Load (star schema) dataframes into the database
    log_progress(log_file, ' Data succesfully loaded into the database. ETL process complete.')

    # Execute validation queries
    execute_validation(engine)
    log_progress(log_file, ' Data in tech_layoffs successfully validated.')

if __name__ == "__main__":
    main()