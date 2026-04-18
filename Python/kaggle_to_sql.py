# %%
import pandas as pd
import os
import sys

from datetime import datetime
from kaggle_loader import KaggleToSQLPipeline
from dotenv import load_dotenv

# %%
# Call from .env file

dotenv_path = os.path.join('..', '.env')
load_dotenv(dotenv_path)

# %%
# Connect to database

# Initialize the pipeline
pipe = KaggleToSQLPipeline(db_name="football", user=os.getenv('DB_USER'), password=os.getenv('DB_PASS'), port=os.getenv('DB_PORT'))

# Download the data

data_folder = pipe.download_data("kamrangayibov/football-data-european-top-5-leagues")

file_path = os.path.join(data_folder, r'kaggle_dataset/')

# %%
# Create a list containing only files that end with '.csv'
csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]

print(csv_files)

# %%
# Preview the dataset using pandas dataframe

csv_to_load = os.path.join(file_path, "coaches.csv") # Replace with the target CSV filename

df = pd.read_csv(csv_to_load)

dataset_name = csv_to_load.replace(file_path, '')

print(f'======== Preview of {dataset_name} table ========')

# %%
# Load the csv file to database (single-file)

pipe.export_csv_to_sql(csv_to_load, table_name="coaches_test") # Set the destination table name

# %%
# Load the csv file to database (all-files)

for csv in csv_files:
    table_name = csv.replace('.csv', '')
    csv_to_load = os.path.join(file_path, csv)
    df = pd.read_csv(csv_to_load)
    df['created_datetime'] = pd.to_datetime(datetime.now())
    pipe.export_df_to_sql(df, table_name=table_name)
    


