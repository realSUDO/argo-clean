import pandas as pd
import os
from glob import glob
from sqlalchemy import create_engine

# -------------------------------
# CONFIG
# -------------------------------
PARQUET_DIR = os.path.join(os.getcwd(), "argo_parquet")   # folder containing .parquet files
SQL_DB_DIR = os.path.join(os.getcwd(), "SQL-DB")          # folder to store DB
os.makedirs(SQL_DB_DIR, exist_ok=True)

DB_FILE = os.path.join(SQL_DB_DIR, "argo_profiles.db")    # SQLite DB
TABLE_NAME = "profiles"                                   # SQL table name

# Create SQL engine
engine = create_engine(f"sqlite:///{DB_FILE}")

# -------------------------------
# LOOP over all .parquet files
# -------------------------------
parquet_files = glob(os.path.join(PARQUET_DIR, "*.parquet"))

if not parquet_files:
    print(f"No parquet files found in {PARQUET_DIR}")
else:
    for pq_file in parquet_files:
        print(f"\nProcessing: {pq_file}")
        try:
            df = pd.read_parquet(pq_file)
            df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
            print(f"Appended {len(df)} rows to table '{TABLE_NAME}' in {DB_FILE}")
        except Exception as e:
            print(f"❌ Failed to process {pq_file}: {e}")

# -------------------------------
# OPTIONAL: Verify DB
# -------------------------------
try:
    test_df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} LIMIT 5", engine)
    print("\nSample rows from DB:")
    print(test_df)
except Exception as e:
    print(f"❌ Could not query DB: {e}")

