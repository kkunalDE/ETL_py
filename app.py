import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime
from streamlit_autorefresh import st_autorefresh



refresh_interval = 60000  # Refresh every 60 seconds
# component to trigger a rerun
st_autorefresh(interval=refresh_interval, key="data_refresh")

# Log file path
log_filename = r'C:\Users\kkuna\Downloads\redshift-to-sqlserver\etl_log_2024-04-02_23-34-30.txt'
log_dir = r'C:\Users\kkuna\Downloads\redshift-to-sqlserver'
log_files = [f for f in os.listdir(log_dir) if f.startswith('etl_log') and f.endswith('.txt')]


# Helper function to parse timestamps from filenames
def extract_timestamp_from_filename(filename):
    timestamp_str = filename.split('_')[-1].split('.')[0]
    date_str = filename.split('_')[-2]
    return datetime.strptime(date_str + timestamp_str, '%Y-%m-%d%H-%M-%S')
# Extract initial timestamp from the log filename ( format "etl_log_YYYY-MM-DD_HH-MM-SS.txt")
initial_timestamp_str = log_filename.split('_')[-1].split('.')[0]
initial_date_str = log_filename.split('_')[-2]
initial_timestamp = datetime.strptime(initial_date_str + initial_timestamp_str, '%Y-%m-%d%H-%M-%S')

def parse_log_file(filename, initial_timestamp):
    log_entries = []
    previous_end_time = initial_timestamp
    with open(filename, 'r') as file:
        for line in file:
            timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}', line)
            if timestamp_match:
                current_end_time = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
                time_taken = (current_end_time - previous_end_time).total_seconds()  # Duration in seconds
                previous_end_time = current_end_time  # This end time is the start time for the next operation
                
                records_loaded_match = re.search(r'Successfully loaded (\d+) records', line)
                table_name_match = re.search(r'into (?P<table_name>.*?) at', line)
                records_loaded = int(records_loaded_match.group(1)) if records_loaded_match else None
                table_name = table_name_match.group('table_name') if table_name_match else None
                
                log_entries.append({
                    'timestamp': current_end_time,
                    'table_name': table_name,  # Extracted table name
                    'records_loaded': records_loaded,  # Extracted record count
                    'time_taken': time_taken  # Calculated time taken for the operation
                })

    return pd.DataFrame(log_entries)

st.title('IOA ETL Process Monitor')

# User selects a log file
selected_log_filename = st.selectbox('Select a log file', log_files)

# Extract initial timestamp from the selected log file name
initial_timestamp = extract_timestamp_from_filename(selected_log_filename)

if st.button('Show Log Data'):
    # Parse the selected log file
    log_df = parse_log_file(os.path.join(log_dir, selected_log_filename), initial_timestamp)
    sorted_log_df = log_df.sort_values(by='timestamp', ascending=False)
    sorted_log_df=sorted_log_df.rename(columns={'time_taken':'time_taken(sec)'})
    
    # Display the DataFrame
    st.dataframe(sorted_log_df[['timestamp', 'table_name', 'records_loaded', 'time_taken(sec)']], width=1000, height=500)
