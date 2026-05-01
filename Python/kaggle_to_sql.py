import os
import pandas as pd
from datetime import datetime
from kaggle_loader import KaggleToSQLPipeline
from config import DB_CONFIG


def run_kaggle_pipeline():
    pipe = KaggleToSQLPipeline(**DB_CONFIG)

    data_folder = pipe.download_data(
        "kamrangayibov/football-data-european-top-5-leagues"
    )

    file_path = os.path.join(data_folder, "kaggle_dataset")

    csv_files = [f for f in os.listdir(file_path) if f.endswith(".csv")]

    for csv in csv_files:
        csv_path = os.path.join(file_path, csv)
        df = pd.read_csv(csv_path)

        df["created_datetime"] = datetime.now()

        table_name = csv.replace(".csv", "")
        pipe.export_df_to_sql(df, table_name)

    print("✅ Kaggle data loaded successfully")