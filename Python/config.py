import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "db_name": "football",
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT")
}