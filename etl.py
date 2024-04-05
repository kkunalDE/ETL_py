"""
ETL (Extract, Transform, Load) script for transferring data from Redshift to SQL Server.

This script reads the database configurations from environment variables, 
connects to a Redshift database,
extracts data from specified tables, and loads the data into a SQL Server database. 

It handles logging,error management, and can truncate and reload tables as needed.
"""
# Standard library imports
import logging
import os
# import subprocess
import warnings
from datetime import datetime

# Third-party library imports
import pandas as pd
from dotenv import load_dotenv
from pandas.io.sql import DatabaseError
import redshift_connector
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


warnings.filterwarnings('ignore', category=UserWarning)
load_dotenv(override=True)

# Initialize logging
log_filename = f"etl_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(message)s')

# Load credentials and table information
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
redshift_database = os.getenv('REDSHIFT_DATABASE')
redshift_port = os.getenv('REDSHIFT_PORT')

sql_server_server = os.getenv('SQL_SERVER_SERVER')
sql_server_database = os.getenv('SQL_SERVER_DATABASE')
sql_server_driver = os.getenv('SQL_SERVER_DRIVER').replace(' ','+')

csv_file_path = r'C:\Users\kkuna\Downloads\redshift-to-sqlserver\TABLE_ORDER_BOOK.csv'

# Assume tables_list is a DataFrame containing the schema and table names
tables_list = pd.read_csv(csv_file_path)

# Create Redshift connection string
redshift_conn_string = f"host={redshift_host} dbname={redshift_database} port={redshift_port} user={redshift_user} password={redshift_password}"

# SQL Server connection string
connection_string = f"mssql+pyodbc://{sql_server_server}/{sql_server_database}?trusted_connection=yes&driver={sql_server_driver}"
engine = create_engine(connection_string, fast_executemany=True)
print("Connection string:", connection_string)
# subprocess.Popen(['cmd', '/c', 'timeout /t 15 && streamlit run app.py'])

# ETL process for each table
with redshift_connector.connect(host=redshift_host,
                                database=redshift_database,
                                port=redshift_port,
                                user=redshift_user,
                                password=redshift_password) as conn:
    for row in tables_list.itertuples():
        table_name = f"{row.Schema}.{row.Table_Name}"

        try:
            
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)

            if not df.empty:
                #truncate the table in target
                with engine.begin() as connection: # transaction management lifecycle between truncate to insert
                    # try:
                        dbo_table_name=f"{table_name}"
                        print(dbo_table_name)
                        df.to_sql(dbo_table_name, engine, index=False, if_exists='replace', method=None,chunksize=1000)
                        
                        logging.info("Successfully loaded %s records into [%s].[dbo].[%s] at %s", 
             len(df), sql_server_database, table_name, datetime.now())
            else:
                logging.warning("No data found for %s at %s",dbo_table_name,datetime.now())

        except SQLAlchemyError as e:
            logging.error("SQLAlchemy error processing %s: %s", table_name, e)
        except DatabaseError as e:
            logging.error("Pandas to_sql database error processing %s: %s", table_name, e)
        except Exception as e:
            logging.error("Unexpected error processing %s: %s", table_name, e)
