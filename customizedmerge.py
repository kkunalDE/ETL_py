import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv(override=True)





def get_env_variable(name):
    """
    Get the value of an environment variable.

    Parameters:
    name (str): The name of the environment variable.

    Returns:
    str: The value of the environment variable.

    Raises:
    KeyError: If the environment variable is not found.
    """
    try:
        return os.getenv(name)
    except KeyError:
        logging.error("Environment variable %s not found.",name)
        raise

def create_connection_string():
    """
    Create a connection string for SQLAlchemy.

    Returns:
    str: The connection string.
    """
    server = get_env_variable('SQL_SERVER_SERVER')
    database = get_env_variable('SQL_SERVER_DATABASE_STG')
    driver = get_env_variable('SQL_SERVER_DRIVER').replace(' ', '+')
    return f"mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver={driver}"

def create_engine_connection():
    """
    Create an engine connection using SQLAlchemy.

    Returns:
    sqlalchemy.engine.base.Engine: The engine connection.
    """
    try:
        connection_string = create_connection_string()
        return create_engine(connection_string)
    except Exception as e:
        logging.error("Error creating engine connection")
        logging.exception(e)
        raise

def get_merge_config(engine):
    """
    Get merge configuration data from the database.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): The engine connection.

    Returns:
    list: A list of dictionaries containing merge configuration data.
    """
    merge_config_query = "SELECT target_table, source_table, primary_key, columns_to_merge FROM merge_config;"
    merge_configs = []
    
    with engine.connect() as conn:
        result = conn.execute(text(merge_config_query))
        for row in result:
            config = {
                'target_table': row[0],
                'source_table': row[1],
                'primary_key': row[2],
                'columns_to_merge': row[3].split(',')
            }
            merge_configs.append(config)

    return merge_configs

def merge_tables(engine, merge_configs):
    """
    Merge data from source tables into target tables based on merge configurations.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): The engine connection.
    merge_configs (list): A list of dictionaries containing merge configuration data.
    """
    for config in merge_configs:
        target_table = config['target_table']
        source_table = config['source_table']
        primary_keys = config['primary_key'].split(',')
        columns = config['columns_to_merge']

        on_clause = ' AND '.join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        set_clause = ', '.join([f"target.{col} = source.{col}" for col in columns])
        insert_columns = ', '.join([f"source.{col}" for col in columns])
        insert_values = ', '.join(columns)

        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {source_table} AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_values}) VALUES ({insert_columns})
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE;
        """

        with engine.connect() as conn:
            result = conn.execute(text(merge_sql))
            conn.commit()
            logging.info("Merged data from {%s} into {%s}. Rows processed: {%s}", source_table, target_table, result.rowcount)


def main():
    """
    Main function to execute the merge process.
    """
    
    engine = create_engine_connection()
    merge_configs = get_merge_config(engine)
    start_time = time.time()
    merge_tables(engine, merge_configs)
    end_time = time.time()
    logging.info(f"All merge operations completed successfully, time taken {end_time-start_time:.2f}")

if __name__ == "__main__":
    main()
