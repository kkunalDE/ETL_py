import pyodbc

conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=169.254.175.61,1433;"  # Replace with the enabled IP address and port
    r"Database=NEW_DB;"    # Replace with your actual database name
    r"Trusted_Connection=yes;"
)

# Test the connection
try:
    with pyodbc.connect(conn_str) as conn:
        print("Successfully connected to SQL Server")
except pyodbc.Error as e:
    print(f"Error connecting to SQL Server: {e}")
