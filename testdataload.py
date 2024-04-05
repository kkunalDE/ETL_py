import os
import logging
import time
import subprocess
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path
env_path = Path('.', '.env')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv(dotenv_path=env_path)


def get_env_variable(name):
    """Variable handling
    """
    try:
        return os.getenv(name)
    except KeyError:
        logging.error(f"Environment variable {name} not found.")
        raise

def create_connection_string():
    """Generation of connection string
    """
    server = get_env_variable('SQL_SERVER_SERVER')
    database = get_env_variable('SQL_SERVER_DATABASE_STG')
    driver = get_env_variable('SQL_SERVER_DRIVER').replace(' ', '+')
    return f"mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver={driver}"

def create_engine_connection():
    """
    SQL ALCEHMY NEEDS ENGINE for transaction management
    """
    try:
        connection_string = create_connection_string()
        return create_engine(connection_string)
    except Exception as e:
        logging.error("Error creating engine connection")
        logging.exception(e)
        raise

def create_dummy_data(n):
    """Creating dummy data :5,00,000 to be exact for two tables with same strucutre 
    but different data for showcasing merge update
    """
    start_time = time.time()
    data = {
        'ID': range(1, n + 1),
        'Name': [f'Name{i}' for i in range(1, n + 1)],
        'Age': [i % 50 + 20 for i in range(1, n + 1)],
        'Department': [f'Department{i % 5}' for i in range(1, n + 1)],
        'Salary': [50000 + i * 10 for i in range(1, n + 1)]
    }
    df = pd.DataFrame(data)
    end_time = time.time()
    logging.info(f"Dummy data creation completed in {end_time - start_time:.2f} seconds.")
    return df

def merge_data(engine):
    "merge operation"
    start_time = time.time()
    merge_sql = """
    MERGE INTO [POC_IOA_ODSSTAGE].[dbo].[test_data_ods] AS target
    USING [POC_IOA_ODSSTAGE].[dbo].[test_data_odsstg] AS source
    ON target.ID = source.ID
    WHEN MATCHED THEN
        UPDATE SET
            target.Name = source.Name,
            target.Age = source.Age,
            target.Department = source.Department,
            target.Salary = source.Salary;
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(text(merge_sql))
            conn.commit()
        end_time = time.time()
        logging.info(f"Merge operation performed successfully in {end_time - start_time:.2f} seconds. {result.rowcount} records processed.")
    except Exception as e:
        logging.error("An error occurred during the merge operation.")
        logging.exception(e)

def get_merge_config(engine):
    merge_config_query = "SELECT target_table, source_table, primary_key, columns_to_merge FROM merge_config;"
    merge_configs = []
    
    with engine.connect() as conn:
        result = conn.execute(text(merge_config_query))
        for row in result:
            # Construct a dictionary for each row and append it to merge_configs
            config = {
                'target_table': row[0],  # Assuming row[0] is the target_table
                'source_table': row[1],  # Assuming row[1] is the source_table
                'primary_key': row[2],   # Assuming row[2] is the primary_key
                'columns_to_merge': row[3].split(',')  # Assuming row[3] is the columns_to_merge
            }
            merge_configs.append(config)
        print(merge_configs)

    return merge_configs





def main():
    engine = create_engine_connection()

    start_time = time.time()
    dummy_data = create_dummy_data(500000)
    end_time = time.time()

    try:
        start_time = time.time()
        logging.info("inserting dummy data to stage and target")
        dummy_data.to_sql('test_data_odsstg', con=engine, if_exists='replace', index=False)
        end_time = time.time()
        logging.info(f"Dummy data for ODS STG inserted into the database in {end_time - start_time:.2f} seconds.")

        start_time = time.time()
        dummy_data['Age'] += 1
        dummy_data['Salary'] += 5000
        dummy_data.to_sql('test_data_ods', con=engine, if_exists='replace', index=False)
        end_time = time.time()
        logging.info(f"Dummy data for ODS inserted into the database in {end_time - start_time:.2f} seconds.")

        #merge_data(engine)
        #get_merge_config(engine)
        logging.info("starting merge process")
        
        subprocess.run(['python','customizedmerge.py'])
    except Exception as e:
        logging.error("An error occurred during the data processing.")
        logging.exception(e)

if __name__ == "__main__":
    main()
