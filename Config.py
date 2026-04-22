import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")

#RAW_FILE_PATH = os.getenv("RAW_FILE_PATH", "/opt/airflow/data/raw_logs.json")

RAW_FILE_PATH = os.getenv("RAW_FILE_PATH", "raw_logs.json")