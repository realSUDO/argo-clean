import duckdb
import os
from glob import glob
import pandas as pd
from sqlalchemy import create_engine, inspect

# -------------------------------
# CONFIG
# -------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
parquet_path = os.path.join(script_dir, "..", "argo_parquet", "*.parquet")

pg_conn = "postgresql://multiply:unlocked@localhost:5432/ocean"
TABLE_NAME = "argo_measurements"

# -------------------------------
# CONNECT
# -------------------------------
con = duckdb.connect()
engine = create_engine(pg_conn)

# -------------------------------
# GET existing Postgres columns if table exists
# -------------------------------
try:
    with engine.connect() as conn:
        insp = inspect(conn)
        pg_cols = [col["name"] for col in insp.get_columns(TABLE_NAME)]
    table_exists = True
    print(f"📑 Postgres table `{TABLE_NAME}` exists with {len(pg_cols)} columns: {pg_cols}")
except Exception:
    pg_cols = None
    table_exists = False
    print(f"⚠️ Table `{TABLE_NAME}` does not exist yet. It will be created from first parquet file.")

# -------------------------------
# LOOP over parquet files
# -------------------------------
parquet_files = glob(parquet_path)
if not parquet_files:
    print(f"❌ No parquet files found at {parquet_path}")
    exit(1)
else:
    print(f"📦 Found {len(parquet_files)} parquet files. Starting load...")

total_inserted = 0
for i, pq_file in enumerate(parquet_files, 1):
    try:
        print(f"\n➡️ [{i}/{len(parquet_files)}] {os.path.basename(pq_file)}")

        # Read parquet
        df = con.execute(f"SELECT * FROM read_parquet('{pq_file}')").df()

        # Add source_file column
        if "source_file" not in df.columns:
            df["source_file"] = os.path.basename(pq_file)

        # Align columns if table exists
        if table_exists:
            # Add missing columns
            for col in pg_cols:
                if col not in df.columns:
                    df[col] = None
            # Drop extra columns
            df = df[[c for c in pg_cols if c in df.columns]]
            # Reorder according to pg_cols
            df = df.reindex(columns=pg_cols)
            if df.empty:
                print(f"   ⚠️ No matching columns to insert for this file. Skipping.")
                continue

            # Append
            df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        else:
            # First file creates table
            df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
            pg_cols = df.columns.tolist()
            table_exists = True

        inserted = len(df)
        total_inserted += inserted
        print(f"   ✅ Inserted {inserted} rows")

    except Exception as e:
        print(f"   ❌ Failed on {pq_file}: {e}")

# -------------------------------
# Verify
# -------------------------------
try:
    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        total = result.scalar()
        print(f"\n📊 Total rows in DB: {total}")
        print(f"🏁 Finished inserting {total_inserted} rows from {len(parquet_files)} files")
except Exception as e:
    print(f"❌ Verification failed: {e}")

