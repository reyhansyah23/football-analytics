"""
Kaggle to MySQL ETL Pipeline
---------------------------
This script automates the process of downloading a dataset from Kaggle
and exporting specific CSV files or Dataframe into a MySQL database.
"""

import os
import pandas as pd
import kagglehub
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

class KaggleToSQLPipeline:
    def __init__(self, db_name, user, password, host, port):
        # Store database credentials and connection strings
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        # server_url is used for general server actions (like creating the DB)
        self.server_url = f"mysql+pymysql://{user}:{password}@{host}:{port}"
        self.wait_for_mysql()

        # --- NEW: Connection Validation ---
        print("Validating database connection...")
        temp_engine = create_engine(self.server_url)
        try:
            with temp_engine.connect() as conn:
                print("Connection successful!")
        except Exception as e:
            # If the password is wrong, it raises an error here
            print(f"\n[CRITICAL ERROR] Could not connect to MySQL: {e} can't connect xto {host}:{port}")
            print("Please check your password and port settings.")
            import sys
            sys.exit(1) # Stop the script immediately
        finally:
            temp_engine.dispose()
    
    def wait_for_mysql(self, retries=10, delay=5):
        print("Validating database connection...")

        for attempt in range(1, retries + 1):
            try:
                engine = create_engine(self.server_url)
                with engine.connect() as conn:
                    print("✅ Connection successful!")
                    engine.dispose()
                    return
            except Exception as e:
                print(f"⏳ Attempt {attempt}/{retries} failed: {e}")
                time.sleep(delay)

        print("\n❌ [CRITICAL ERROR] Could not connect to MySQL after retries.")
        import sys
        sys.exit(1)
        
    def download_data(self, dataset_handle):
        """Downloads dataset from Kaggle and returns the local path."""
        print(f"--- Downloading {dataset_handle} ---")
        # Directing the download to the current working directory
        os.environ["KAGGLEHUB_CACHE"] = os.getcwd()
        path = kagglehub.dataset_download(dataset_handle)
        print(f"Data saved to: {path}")
        return path

    def ensure_database_exists(self):
        """Initializes the database on the server if it does not exist."""
        print(f"--- Checking/Creating Database: '{self.db_name}' ---")
        engine = create_engine(self.server_url)
        try:
            # Using a context manager (with) ensures the connection closes automatically
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.db_name}"))
                conn.execute(text("COMMIT"))
            
            print(f"Database '{self.db_name}' is ready.")
            
        except Exception as e:
            print(f"Error ensuring database exists: {e}")
            
        finally:
            engine.dispose()

    def export_csv_to_sql(self, csv_path, table_name):
        """Reads a specific CSV and exports it to the SQL database using chunking."""
        if not os.path.exists(csv_path):
            print(f"Error: File {csv_path} not found.")
            return

        self.ensure_database_exists()
        
        print(f"--- Exporting {os.path.basename(csv_path)} to table '{table_name}' ---")
        db_url = f"{self.server_url}/{self.db_name}"
        engine = create_engine(db_url)

        try:
            # chunksize=1000 reads the file in small pieces to prevent memory crashes
            df_iter = pd.read_csv(csv_path, chunksize=1000)
            
            first_chunk = True
            for chunk in df_iter:
                # 'replace' creates/overwrites the table on the first pass
                # 'append' adds the remaining data to the existing table
                if_exists_param = 'replace' if first_chunk else 'append'
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    index=False,
                    if_exists=if_exists_param
                )
                first_chunk = False
            
            print(f"Successfully exported data to '{table_name}'.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            engine.dispose()

    def export_df_to_sql(self, df, table_name, if_exists='replace'):
            """Exports an existing Pandas DataFrame to the SQL database."""
            # Ensure the dataframe is not empty
            if df.empty:
                print("Error: The provided DataFrame is empty.")
                return

            self.ensure_database_exists()
            
            print(f"--- Exporting DataFrame to table '{table_name}' ---")
            db_url = f"{self.server_url}/{self.db_name}"
            engine = create_engine(db_url)

            try:
                # chunksize=1000 ensures large dataframes are uploaded in batches
                df.to_sql(
                    name=table_name,
                    con=engine,
                    index=False,
                    if_exists=if_exists,
                    chunksize=1000
                )
                print(f"Successfully exported {len(df)} rows to '{table_name}'.")
            except Exception as e:
                print(f"An error occurred during DataFrame export: {e}")
            finally:
                engine.dispose()

# --- Execution ---
if __name__ == "__main__":
    import sys
    import getpass

    # Validate terminal arguments: [db_name, csv_filename, table_name]
    if len(sys.argv) < 4:
        print("\n[ERROR] Missing arguments!")
        print("Usage: python your_script.py <db_name> <csv_filename> <table_name>")
        print("Example: python main.py football matches.csv matches_table")
        sys.exit(1)

    # 1. Map command line arguments to variables
    DB_NAME     = sys.argv[1]
    TARGET_FILE = sys.argv[2]
    TABLE_NAME  = sys.argv[3]
    
    # Static config for the specific Kaggle dataset
    KAGGLE_DATASET = "kamrangayibov/football-data-european-top-5-leagues"
    DB_USER = "root"

    # 2. Securely prompt for the database password
    pwd = getpass.getpass(f"Enter MySQL password for {DB_USER}: ")

    # 3. Instantiate and run the pipeline
    pipeline = KaggleToSQLPipeline(DB_NAME, DB_USER, pwd)

    # 4. Download and locate the dataset
    downloaded_path = pipeline.download_data(KAGGLE_DATASET)
    # Combining the downloaded cache path with the specific dataset folder
    dataset_path = os.path.join(downloaded_path, 'kaggle_dataset')
    
    # 5. Build final path and execute the SQL export
    full_csv_path = os.path.join(dataset_path, TARGET_FILE)
    pipeline.export_csv_to_sql(full_csv_path, TABLE_NAME)