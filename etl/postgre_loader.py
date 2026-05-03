from sqlalchemy import create_engine
import pandas as pd
import time

class PostgresLoader:
    def __init__(self, user, password, host, port, database):
        self.connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        self.wait_for_postgres()
        self.engine = create_engine(self.connection_string)

    def wait_for_postgres(self, retries=10, delay=3):
        print("Validating database connection...")

        for attempt in range(1, retries + 1):
            try:
                engine = create_engine(self.connection_string)
                with engine.connect() as conn:
                    print("Connection successful!")
                    engine.dispose()
                    return
            except Exception as e:
                print(f"Attempt {attempt}/{retries} failed: {e}")
                time.sleep(delay)

        print("\n[CRITICAL ERROR] Could not connect to PostgreSQL after retries.")
        import sys
        sys.exit(1)

    def load_dataframe(self, df, table_name, schema = "public", if_exists = "append"):
        """
        Load dataframe into PostgreSQL.
        if_exists = replace | append | fail
        """
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi"
            )
            print(f"Loaded {len(df)} rows into {schema}.{table_name}")
        except Exception as e:
            print(f"Failed loading {table_name}: {e}")