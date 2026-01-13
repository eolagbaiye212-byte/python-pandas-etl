# This code takes the transformed data, and makes it into a star schema. The tables are then loaded into a database.
# This is the 'Gold' layer in the Bronze/Silver/Gold architerture

# Import required libraries
import pandas as pd

def star_schema(input_path):
    df = pd.read_csv(input_path, parse_dates=['reported_date'])

    # Create dimension tables
    # Company dimension
    dim_company = (df[['company','status','sources']].drop_duplicates().reset_index(drop=True))    
    dim_company['company_ID'] = dim_company.index + 1
   
    # Date dimension
    dim_date = (df[['reported_date']].drop_duplicates().rename(columns={'reported_date' : 'date'}).reset_index(drop=True))
    dim_date['date_ID'] = dim_date.index + 1
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['day'] = dim_date['date'].dt.date
    dim_date['quarter'] = dim_date['date'].dt.quarter

    # Industry dimension
    dim_industry = (df[['industry']].drop_duplicates().reset_index(drop=True))
    dim_industry['industry_ID'] = dim_industry.index + 1

    # Location dimension
    dim_location = (df[['headquarter_location']].drop_duplicates().reset_index(drop=True))
    dim_location['location_ID'] = dim_location.index + 1

    #Create fact table
    fact = df.merge(dim_company, how='left', on=['company','status','sources'])
    fact = fact.merge(dim_date, how= 'left', left_on='reported_date', right_on='date')
    fact = fact.merge(dim_industry, how='left', on='industry')
    fact = fact.merge(dim_location, how='left', on='headquarter_location')
    
    fact_layoffs = fact[['company_ID','industry_ID','location_ID','date_ID','total_layoffs','known_total_layoffs','impacted_workforce_percentage','known_impacted_workforce_percentage']]

    # Create a dictionary where the keys are the table names, and the values are the dictionaries corresponding to the name.
    # The function will be made to return this dictionary, and then it'll be iterated through in the load function to creates the appropiate tables in the PostgreSQL database
    tables = {'dim_company':dim_company, 'dim_date':dim_date, 'dim_industry':dim_industry, 'dim_location':dim_location, 'fact_layoffs':fact_layoffs}
    return tables


