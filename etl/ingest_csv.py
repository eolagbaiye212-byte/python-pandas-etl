# This is the ingestion stage of the ETL pipeline for the analysis of tech company layoffs from 2022-2023.
# It takes a csv path as an input, converts it to a dataframe, and outputs the dataframe as a csv file to a location of choice

# Import the pandas library
import pandas as pd

def ingest_csv(input_path, output_path):
    df_raw = pd.read_csv(input_path)
    df_raw.to_csv(output_path, index = False)
