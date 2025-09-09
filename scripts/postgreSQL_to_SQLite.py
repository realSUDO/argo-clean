import pandas as pd
from sqlalchemy import create_engine, inspect
import os

# -------------------------------
# CONFIG
# -------------------------------
PG_CONN = "postgresql://multiply:unlocked@localhost:5432/ocean"
SQLITE_DB = os.path.join(os.path.dirname(__file__), "../SQL-DB/argo_meta.db")
TABLE_NAME = "argo_meta"
CHUNK_SIZE = 500_000  # fetch in chunks to avoid memory crash

# -------------------------------
# CREATE engines
# -------------------------------
os.makedirs(os.path.dirname(SQLITE_DB), exist_ok=True)
pg_engine = create_engine(PG_CONN)
sqlite_engine = create_engine(f"sqlite:///{SQLITE_DB}")

# -------------------------------
# GET Postgres columns
# -------------------------------
insp = inspect(pg_engine)
pg_cols = [col["name"] for col in insp.get_columns(TABLE_NAME)]
print(f"📑 Postgres table '{TABLE_NAME}' has {len(pg_cols)} columns: {pg_cols}")

# -------------------------------
# EXPORT in chunks dynamically
# -------------------------------
offset = 0
total_exported = 0

while True:
    query = f"SELECT * FROM {TABLE_NAME} OFFSET {offset} LIMIT {CHUNK_SIZE};"
    chunk = pd.read_sql(query, pg_engine)
    if chunk.empty:
        break

    # Ensure columns match Postgres schema (add missing as None)
    for col in pg_cols:
        if col not in chunk.columns:
            chunk[col] = None
    # Keep only schema columns (reorder)
    chunk = chunk[pg_cols]

    # Write chunk to SQLite
    chunk.to_sql(TABLE_NAME, sqlite_engine, if_exists='append', index=False)

    exported = len(chunk)
    total_exported += exported
    offset += exported
    print(f"✅ Exported {total_exported} rows...")

print(f"🏁 Finished exporting '{TABLE_NAME}' to {SQLITE_DB}")

