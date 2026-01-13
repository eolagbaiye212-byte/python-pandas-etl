# This module contains a function that executes the validation queries in "validation_queries"

# Import the list containing the validation queries from the 'validation_queries' module
from .validation_queries import validation_queries
import pandas as pd

RAW = "C:/Users/eolag/OneDrive/Documents/Data Engineering/tech_layoffs.csv"

def execute_validation(engine):
    #query_response = {}

    for i, query in enumerate(validation_queries(), start = 1):
        print('')
        print(f" Executing validation query {i}...")
        print(pd.read_sql(query, engine, index_col=None))
        if i == 2:
            print(f"The total number of records (layoff instances) in the raw data is {len(pd.read_csv(RAW))}")




