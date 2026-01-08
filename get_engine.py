# This will create a function that estblishes a connection with PostgreSQL for the loading of the dataframes into the database as tables

# Import required libraries
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus


# Define function. This functions says "Build me a databse login URL using whatever environment I'm running in".
def get_engine():
    # Getting the user, password, host, port, and db for the given environment
    user = os.getenv('PGUSER','postgres')
    password = os.getenv('PGPASSWORD')
    host = os.getenv('PGHOST', 'localhost')
    port = os.getenv('PGPORT', '5432')
    db = os.getenv('PGDATABASE','techlayoffs')

    if not password:
        raise ValueError('PGPASSWORD environment variable not set')
    
    password = quote_plus(password)
    
    # Takes what the OS gives it, and uses those names to establish a connection to PostgreSQL
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    return engine
