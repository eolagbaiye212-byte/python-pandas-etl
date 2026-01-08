# This function will accept the engine, as well as the tables variable created in the star schema, which contains a dictionary of the
# dimension tables as well as the fact table (as values, and the keys as their names), and load them into the database as tables

def load_into_db(engine, tables: dict):
    for table_name, df in tables.items():
        df.to_sql(table_name, engine, if_exists = 'replace', index = False)

# table.items returns the the key-value pairs in tables as a list of tuples. They're the iterated through, being loaded into the database
# with the key (table_name) as the name of the table, and the dataframes (values) as the tables.