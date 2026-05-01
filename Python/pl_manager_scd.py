import os
import pandas as pd
import numpy as np
from datetime import datetime

from kaggle_loader import KaggleToSQLPipeline
from scrape_pl_manager import WikiTableParser
from config import DB_CONFIG


def load_base_data(pipe):
    print('Start load base Data')
    data_folder = pipe.download_data(
        "kamrangayibov/football-data-european-top-5-leagues"
    )
    file_path = os.path.join(data_folder, "kaggle_dataset")

    coach_df = pd.read_csv(os.path.join(file_path, "coaches.csv"))
    team_df = pd.read_csv(os.path.join(file_path, "teams.csv"))

    return coach_df, team_df


def scrape_manager_data():
    parser = WikiTableParser(
        "https://en.wikipedia.org/wiki/List_of_Premier_League_managers"
    )
    df = parser.parse_managers_table()

    df["is_current"] = np.where(df['Name'].str.contains('†'), 1, 0)
    df["is_caretaker"] = np.where(df['Name'].str.contains('‡'), 1, 0)
    df["is_non_pl_current"] = np.where(df['Name'].str.contains('§'), 1, 0)

    return df


def transform_dates(df):
    df["from_clean"] = df["From"].str.replace(r"\[.*\]", "", regex=True)

    df["valid_from"] = pd.to_datetime(df["from_clean"], errors="coerce")

    df["valid_to"] = np.where(
        df["Until"].str.contains("present", case=False, na=False),
        "31 December 2199",
        df["Until"]
    )

    df["valid_to"] = pd.to_datetime(df["valid_to"], format='%d %B %Y', errors="coerce")

    return df


def build_scd(coach_df, team_df, manager_df):
    coach_team = coach_df.merge(
        team_df[["team_id", "name", "league_id"]].rename({'name':'club_name'}, axis=1),
        on="team_id",
        how="left"
    )

    coach_team = coach_team[coach_team["league_id"] == 1]

    manager_df["Club"] = manager_df["Club"].replace(
        {"Bournemouth": "AFC Bournemouth"}
    )

    merged = coach_team.merge(
        manager_df,
        left_on="club_name",
        right_on="Club",
        how="left"
    )

    result = merged[[
        "coach_id", "team_id", "club_name", "league_id",
        "Name", "Nat.", "is_current",
        "is_caretaker", "is_non_pl_current",
        "valid_from", "valid_to"
    ]]

    result = result.rename(columns={
        "club_name": "team_name",
        "Name": "coach_name",
        "Nat.": "nationality"
    })

    result["coach_name"] = result["coach_name"].str.replace(
        r"[†‡§]", "", regex=True
    )

    result["created_datetime"] = datetime.now()

    return result


def run_scd_pipeline():
    pipe = KaggleToSQLPipeline(**DB_CONFIG)

    coach_df, team_df = load_base_data(pipe)
    print('succes loading base data')
    manager_df = scrape_manager_data()
    print('success scrape manager')
    manager_df = transform_dates(manager_df)

    scd_df = build_scd(coach_df, team_df, manager_df)

    pipe.export_df_to_sql(scd_df, "coaches_scd")

    print("✅ SCD table loaded successfully")

    # return print(manager_df.head(10))
    # return print(manager_df[manager_df['valid_from'].astype(str).str.contains('NaT', case=False) | manager_df['valid_to'].astype(str).str.contains('NaT', case=False)])
    # return print(manager_df.count())

if __name__ == "__main__":
    run_scd_pipeline()