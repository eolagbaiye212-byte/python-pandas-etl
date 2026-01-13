# This file resets the etl_log.txt file by writing over it's contents at the start of each ETL process.

def reset_log(file_path):
    with open(file_path, 'w') as f:
        f.write("") # Truncate contents of the file