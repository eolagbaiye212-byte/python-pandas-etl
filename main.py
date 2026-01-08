# This file orchestrates the ETL pipeline. It runs all the modules made for this ETL.

# Import the other python modules into the main
import ingest_csv
import clean_layoffs
import star_schema
import get_engine
import load_into_db

# Define the RAW, BRONZE, and SILVER output paths
RAW = "C:/Users/eolag/OneDrive/Documents/Data Engineering/tech_layoffs.csv"
BRONZE = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Practice ETL_ELT/Tech_Layoffs_Project/tech_layoffs_bronze.csv"
SILVER = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Practice ETL_ELT/Tech_Layoffs_Project/tech_layoffs_silver.csv"

def main():
    ingest_csv.ingest_csv(RAW, BRONZE)
    clean_layoffs.clean_layoffs(BRONZE, SILVER)
    tables = star_schema.star_schema(SILVER)

# Ensure database is created in Postgre and environmental variables are created prior to running this module

    engine = get_engine.get_engine() # Establish connection
    load_into_db.load_into_db(engine, tables) # Load (star schema) dataframes into the database

if __name__ == "__main__":
    main()