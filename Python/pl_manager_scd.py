# %%
import pandas as pd
import numpy as np
import os
import sys

# Add the root project directory (football-analytics) to path
from kaggle_loader import KaggleToSQLPipeline
from scrape_pl_manager import WikiTableParser
from datetime import datetime
from dotenv import load_dotenv

# %%
# Call from .env file

dotenv_path = os.path.join('..', '.env')
load_dotenv(dotenv_path)

# %%
# Connect to database

DB_USER = "root"

# Initialize the pipeline
pipe = KaggleToSQLPipeline(db_name="football", user=os.getenv('DB_USER'), password=os.getenv('DB_PASS'), port=os.getenv('DB_PORT'))

# Download the data and store in our local directory

data_folder = pipe.download_data("kamrangayibov/football-data-european-top-5-leagues")

file_path = os.path.join(data_folder, r'kaggle_dataset/')

# %%
# Create a list containing only files that end with '.csv'
csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]

print(csv_files)

# %%
# Preview the dataset using pandas dataframe

csv_to_load = os.path.join(file_path, "coaches.csv")

coach_df = pd.read_csv(csv_to_load)
# %%
# Preview the dataset using pandas dataframe

csv_to_load = os.path.join(file_path, "teams.csv")

team_df = pd.read_csv(csv_to_load)

# %%
# Scrape manager information from Wikipedia

parser = WikiTableParser('https://en.wikipedia.org/wiki/List_of_Premier_League_managers')
pl_coach = parser.parse_managers_table()
if pl_coach is not None:
    print(pl_coach.head(3))

# %%
# Check any values from "From" column that has a reference string
pl_coach[pl_coach['From'].astype(str).str.contains('\[.*\]', regex=True)]

# %%
# Add new 3 columns based on Key section in Wiki

pl_coach = pl_coach.assign(
    is_current = np.where(pl_coach['Name'].str.contains('†'), 1, 0),
    is_caretaker = np.where(pl_coach['Name'].str.contains('‡'), 1, 0),
    is_non_pl_current = np.where(pl_coach['Name'].str.contains('§'), 1, 0)
)

pl_coach

# %%
# add 2 new columns to check if any invalid date format to set as NaT

pl_coach['from_check'] = pd.to_datetime(pl_coach['From'], errors='coerce').astype(str)
pl_coach['until_check'] = pd.to_datetime(pl_coach['Until'], errors='coerce').astype(str)

# %%
# Check if any NaT from those 2 columns (From and Until)

pl_coach[pl_coach['from_check'].str.contains('NaT', case=False) | pl_coach['until_check'].str.contains('NaT', case=False)]

# %%
# Check current manager with "Present" as the value in "Until" column

pl_coach[pl_coach['Until'].astype(str).str.contains('present', case=False)]

# %%
# add new column to assign until_at as a valid date column (Until)

pl_coach = pl_coach.assign(
    until_at = np.where(
        pl_coach['Until'].astype(str).str.contains('present', case=False), 
        '31 December 2199', # Max limitation from datetime only until 2262-04-11, so i make it 2199-12-31 instead of 9999-12-31
        pl_coach['Until']
    )
)
pl_coach['until_at'] = pd.to_datetime(pl_coach['until_at'], format='%d %B %Y', errors='coerce')

pl_coach[pl_coach['Until'].astype(str).str.contains('present', case=False)]

# %%
# Check to see all of NaT values in "until_check" already being a valid date in "until_at" column

pl_coach[pl_coach['until_check'].str.contains('NaT', case=False)]

# %%
# Remove additional char from "From" column

pl_coach['from_check'] = pl_coach['From'].astype(str).str.replace('\[.*\]', '', regex=True)

# Check
pl_coach[pl_coach['From'].str.contains('\[.*\]', '', regex=True)]

# %%
# add new column to assign from_at as a valid date column (From)

pl_coach = pl_coach.assign(
from_at = np.where(pl_coach['From'].astype(str).str.contains('\[.*\]', regex=True)
         , pd.to_datetime(pl_coach['from_check'], format='%d %B %Y', errors='coerce')
         , pd.to_datetime(pl_coach['from_check'], errors='coerce'))
)

pl_coach

# %%
# Convert columns as datetime format

pl_coach['from_check'] = pd.to_datetime(pl_coach['from_at'], errors='coerce').astype(str)
pl_coach['until_check'] = pd.to_datetime(pl_coach['until_at'], errors='coerce').astype(str)

# %%
# Check if any NaT value

pl_coach[pl_coach['from_check'].str.contains('NaT', case=False) | pl_coach['until_check'].str.contains('NaT', case=False)]

# %%
# Join table coach and team by team_id key in Premier League (league_id = 1)

coach_team = coach_df.merge(team_df[['team_id', 'name', 'league_id']].rename({'name':'club_name'}, axis=1), how='left', on='team_id')
coach_team = coach_team[coach_team['league_id']==1]
coach_team

# %%
# Merge historical manager table to coach table

coach_pl = coach_team.merge(pl_coach, how='left', left_on='club_name', right_on='Club')
coach_pl

# %%
# Check if any null values

coach_pl.isnull().any()

# %%
# Check null values based on Name column

coach_pl[coach_pl['Name'].isnull()]

# %%
# Check club with null values (Bournemouth)

pl_coach[pl_coach['Club'].str.contains('bournemouth', case=False)]

# %%
# Replace club's name align with table historical manager

pl_coach['Club'] = pl_coach['Club'].str.replace('Bournemouth', 'AFC Bournemouth')

# %%
# Retry to merge again

coach_pl = coach_team.merge(pl_coach, how='left', left_on='club_name', right_on='Club')
coach_pl

# %%
# Check if any null value

coach_pl.isnull().any()

# %%
coach_pl[coach_pl['name'].isnull()].head(5)

# %%
coach_pl.columns

# %%
# Keep the "Name" column instead of "name" as the manager's name

coach_pl_scd = coach_pl[['coach_id', 'team_id', 'club_name', 'league_id','Name', 'Nat.','is_current', 'is_caretaker', 'is_non_pl_current','from_at','until_at']]
coach_pl_scd

# %%
# Check if any null values

coach_pl_scd.isnull().any()

# %%
# rename columns

coach_pl_scd = coach_pl_scd.rename({
    'Name':'coach_name',
    'Nat.':'nationality',
    'club_name': 'team_name',
    'from_at': 'valid_from',
    'until_at': 'valid_to'
}, axis=1)

coach_pl_scd

# %%
# remove special char (†|‡|§) in coach_name

coach_pl_scd['coach_name'] = coach_pl_scd['coach_name'].str.replace('†|‡|§', '', regex=True)
coach_pl_scd

# %%
# Add column created_datetime

coach_pl_scd['created_datetime'] = pd.to_datetime(datetime.now())
coach_pl_scd

# %%
coach_pl_scd.isnull().any()

# %%
# check details columns

coach_pl_scd.info()

# %%
# Load the csv file to database

pipe.export_df_to_sql(coach_pl_scd, "coaches_scd")


