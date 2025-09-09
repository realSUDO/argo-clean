import sqlite3
import pandas as pd

# Connect
conn = sqlite3.connect("SQL-DB/argo_profile.db")

# Pick 3 distinct floats
sample_floats = pd.read_sql_query(
    "SELECT DISTINCT float_id_clean FROM argo_measurements LIMIT 3", conn
)
print("🎯 Sample Float IDs:")
print(sample_floats)

# Get data for those floats
float_ids = tuple(sample_floats["float_id_clean"].tolist())
sample_data = pd.read_sql_query(
    f"SELECT * FROM argo_measurements WHERE float_id_clean IN {float_ids} LIMIT 50", conn
)
print("\n🔬 Sample Data:")
print(sample_data.head())

conn.close()

