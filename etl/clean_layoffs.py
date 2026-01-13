# This code takes the raw ingested data and transforms it into clean, usable data. 
# It's the Silver layer in the Bronze/Silver/Gold architecture. The cleaning of the data is specific to the data provided.

# Import required libraries
import pandas as pd
from datetime import datetime

def clean_layoffs(input_path, output_path):
    df = pd.read_csv(input_path) # This take the raw data and makes saves it as a dataframe

    # Handle the 'Unclear' in the data. Convert instances of 'Unclear' are converted to NaN.
    # Important for maintaining the integrity of the data: not returning errors during data aggregation and metrics, trend analysis over time, etc
    df['total_layoffs'] = pd.to_numeric(df['total_layoffs'], errors = 'coerce')
    df['impacted_workforce_percentage'] = pd.to_numeric(df['impacted_workforce_percentage'], errors = 'coerce')

    # Now that every instance of 'Unclear' in the total_layoffs and impacted_workforce_percentage have been converted to NaN
    # Add a new column for total layoffs and impacted workforce percentage that tracks the unknowns. Good for data quality and analytics. Returns boolean objects.
    df['known_total_layoffs'] = df['total_layoffs'].notna()
    df['known_impacted_workforce_percentage'] = df['impacted_workforce_percentage'].notna()

    # Convert reported date to date_time format
    df['reported_date'] = pd.to_datetime(df['reported_date'], errors = 'coerce')

    # We now have to normalize the Industry column. We'll remove all the commas, and make all the letters lowercase
    df['industry'] = df['industry'].str.lower().str.strip()

    # Handle layoffs where additional information is not available
    df['additional_notes'] = df['additional_notes'].fillna('None')

    # Enforce the schema
    df = df.astype({'total_layoffs':'float64', 'impacted_workforce_percentage':'float64', 'known_total_layoffs':'int8', 'known_impacted_workforce_percentage':'int8'})

    # Save dataframe as csv file
    df.to_csv(output_path, index = False)