import sqlite3
import pandas as pd

# Connect to your SQLite database
conn = sqlite3.connect("SQL-DB/argo_profile.db")
cursor = conn.cursor()

# Pick 3 float_ids to test
sample_floats = ["D2902120", "D1900633", "D1900268"]

# Drop table if it exists
cursor.execute("DROP TABLE IF EXISTS argo_sample")
# Create metadata table
cursor.execute("""
CREATE TABLE argo_sample (
    float_id_clean TEXT PRIMARY KEY,
    summary TEXT
)
""")

# For each float, aggregate values with proper date conversion
for fid in sample_floats:
    query = f"""
    SELECT 
        date('1950-01-01', '+' || MIN(time) || ' days') as start_time,
        date('1950-01-01', '+' || MAX(time) || ' days') as end_time,
        MIN(depth) as min_depth,
        MAX(depth) as max_depth,
        AVG(temp) as avg_temp,
        AVG(sal) as avg_sal,
        AVG(lat) as lat,
        AVG(lon) as lon
    FROM argo_measurements
    WHERE float_id_clean = '{fid}'
    """
    df = pd.read_sql(query, conn)

    if df.empty or df.isnull().all().all():
        continue

    row = df.iloc[0]
    summary = (
        f"Float {fid} operated from {row['start_time']} to {row['end_time']} "
        f"near ({row['lat']:.2f}°N, {row['lon']:.2f}°E). "
        f"Profiles range {row['min_depth']:.1f}–{row['max_depth']:.1f} m, "
        f"with mean temperature {row['avg_temp']:.2f} °C and "
        f"mean salinity {row['avg_sal']:.2f} PSU."
    )

    cursor.execute("INSERT INTO argo_sample (float_id_clean, summary) VALUES (?, ?)", (fid, summary))
    print(f"✅ Added metadata for Float {fid}")

conn.commit()
conn.close()
print("🎉 Sample argo_sample table created with 3 floats.")
