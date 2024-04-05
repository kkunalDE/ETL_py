from dlt import DLT

# Initialize DLT
dlt = DLT()

import os

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
# Define your source (Redshift)
redshift_source = dlt.source('redshift', {
    'database': 'ioa',
    'host': 'insights-dw.clearcareonline.com',
    'port': '9114',
    'user': 'ioa-cjavvaji',
    'password': 'BY84w9H4Fx2z3unerZwlmoeDmPy0OiY3Gi6CZCva'
})

# Define your destination (SQL Server)
sql_server_destination = dlt.destination('mssql', {
    'database': 'POC_IOA_ODS',
    'host': 'kkishore-praval\PRAVALSQLSERVER'
})

# Define your pipeline
pipeline = dlt.pipeline('your_pipeline_name')  # replace with your actual pipeline name

# Add your source and destination to the pipeline
pipeline.add_source(redshift_source)
pipeline.add_destination(sql_server_destination)

# Run the pipeline
pipeline.run()