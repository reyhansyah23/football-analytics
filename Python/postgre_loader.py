from sqlalchemy import create_engine
import pandas as pd


class PostgresLoader:
    def __init__(self, user, password, host, port, database):
        self.connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        self.engine = create_engine(self.connection_string)

    def load_dataframe(self, df, table_name, schema = "public", if_exists = "replace"):
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