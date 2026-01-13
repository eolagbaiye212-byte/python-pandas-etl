# This file contains SQL validation queries to check the integrity of the data after the ETL process has been completed.

import pandas as pd

def validation_queries():
# Verify dimension tables do not contain nulls in their primary keys
    cte1 = 'CTE1 AS (SELECT COUNT(*) AS Num_Null_Keys_company FROM dim_company WHERE "company_ID" IS NULL)'
    cte2 = 'CTE2 AS (SELECT COUNT(*) AS Num_Null_Keys_industry FROM dim_industry WHERE "industry_ID" IS NULL)'
    cte3 = 'CTE3 AS (SELECT COUNT(*) AS Num_Null_Keys_location FROM dim_location WHERE "location_ID" IS NULL)'
    cte4 = 'CTE4 AS (SELECT COUNT(*) AS Num_Null_Keys_date FROM dim_date WHERE "date_ID" IS NULL)'
    select_cte = "SELECT c1.*, c2.*, c3.*, c4.* FROM CTE1 c1 CROSS JOIN CTE2 c2 CROSS JOIN CTE3 c3 CROSS JOIN CTE4 c4"
    query1 = f"WITH {cte1}, {cte2}, {cte3}, {cte4} {select_cte};"

# Compare total records against the source data count (no data lost)
    query2 = "SELECT COUNT(*) AS total_layoff_events FROM fact_layoffs"

# Validate that every foreign key in the fact table has a matching dimension entry
    query3_1 = '(SELECT COUNT(DISTINCT "company_ID") AS companyID_fact FROM fact_layoffs)'
    query3_2 = '(SELECT COUNT(DISTINCT "company_ID") AS companyID_dim FROM dim_company)' 
    query3_3 = '(SELECT COUNT(DISTINCT "industry_ID") AS industryID_fact FROM fact_layoffs)'
    query3_4 = '(SELECT COUNT(DISTINCT "industry_ID") AS industryID_dim FROM dim_industry)'
    query3_5 = '(SELECT COUNT(DISTINCT "location_ID") AS locationID_fact FROM fact_layoffs)'
    query3_6 = '(SELECT COUNT(DISTINCT "location_ID") AS locationID_dim FROM dim_location)'  
    query3_7 = '(SELECT COUNT(DISTINCT "date_ID") AS dateID_fact FROM fact_layoffs)'
    query3_8 = '(SELECT COUNT(DISTINCT "date_ID") AS dateID_dim FROM dim_date)' 
    query3 = f"SELECT {query3_1}, {query3_2}, {query3_3}, {query3_4}, {query3_5}, {query3_6}, {query3_7}, {query3_8};"

# Flag layoff counts that seem impossibly high or negative (this validation also serves analytics purposes)
    query4 = 'SELECT "company_ID" AS outlier_data FROM fact_layoffs WHERE total_layoffs > 50000 OR total_layoffs < 0'

# If a company appears multiple times, verify the companies attributes (location, industry) remain consistent
    join5_1 = 'SELECT f."company_ID", c.company, i.industry, l.headquarter_location, d.date FROM fact_layoffs f '
    join5_2 = 'JOIN dim_company c ON (c."company_ID"= f."company_ID") JOIN dim_industry i ON (i."industry_ID" = f."industry_ID") JOIN dim_location l ON (l."location_ID" = f."location_ID") JOIN dim_date d ON (d."date_ID" = f."date_ID")'
    join5_3 = 'WHERE f."company_ID" IN (SELECT "company_ID" FROM fact_layoffs f GROUP BY f."company_ID" HAVING COUNT(*) > 1) ORDER BY c.company;'
    join5 = join5_1+ join5_2 + join5_3
    query5 = f"{join5}"

# The two validations below can also be seen through query 6
# Query to show companies with multiple layoff events (validates presence of duplicates)
# Flag true duplicates (same company, same date, same layoff count)

    queries = [query1, query2, query3, query4, query5]
    return queries