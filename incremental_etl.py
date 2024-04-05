import pandas as pd
from dotenv import load_dotenv
import os

from sqlalchemy import create_engine


load_dotenv(override=True)

sql_server_server = os.getenv('SQL_SERVER_SERVER')
sql_server_database = os.getenv('SQL_SERVER_DATABASE_STG')
sql_server_driver = os.getenv('SQL_SERVER_DRIVER').replace(' ','+')

connection_string =f"mssql+pyodbc://{sql_server_server}/{sql_server_database}?trusted_connection=yes&driver={sql_server_driver}"

engine = create_engine(connection_string,fast_executemany=True)

print("connection String",connection_string)


source = pd.read_sql_query("""SELECT * FROM [POC_IOA_ODSSTAGE].[dbo].[Agencies.schedules_scheduleexception_stg];""",engine)


target = pd.read_sql_query("""SELECT * FROM [POC_IOA_ODSSTAGE].[dbo].[Agencies.schedules_scheduleexception];""",engine)

changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]

print(changes)







