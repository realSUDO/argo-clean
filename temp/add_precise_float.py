import psycopg2

# -------------------------------
# CONFIG
# -------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ocean",
    "user": "multiply",
    "password": "unlocked"
}
TABLE_NAME = "argo_meta"

# -------------------------------
# CONNECT
# -------------------------------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
print(f"Connected to PostgreSQL database '{DB_CONFIG['dbname']}' as {DB_CONFIG['user']}")

# -------------------------------
# 1️⃣ Add column if not exists
# -------------------------------
print("[1/3] Adding column float_id_precise if not exists...")
cur.execute(f"""
    ALTER TABLE {TABLE_NAME}
    ADD COLUMN IF NOT EXISTS float_id_precise TEXT;
""")
conn.commit()
print("✅ Column ready.\n")

# -------------------------------
# 2️⃣ Update float_id_precise - extract everything before .parquet
# -------------------------------
print("[2/3] Updating float_id_precise from source_file (extracting everything before .parquet)...")
cur.execute(f"""
    UPDATE {TABLE_NAME}
    SET float_id_precise = substring(source_file FROM '^(.*?)\\.parquet')
    WHERE source_file IS NOT NULL;
""")
conn.commit()
print("✅ All rows updated successfully!\n")

# -------------------------------
# 3️⃣ Verify
# -------------------------------
cur.execute(f"""
    SELECT source_file, float_id_precise 
    FROM {TABLE_NAME} 
    WHERE source_file IS NOT NULL 
    LIMIT 10;
""")
print("🎯 Sample results (source_file -> float_id_precise):")
for row in cur.fetchall():
    source_file, float_id_precise = row
    print(f"  {source_file} -> {float_id_precise}")

# Show some statistics
cur.execute(f"""
    SELECT COUNT(*) as total_rows,
           COUNT(float_id_precise) as populated_rows,
           COUNT(DISTINCT float_id_precise) as unique_values
    FROM {TABLE_NAME};
""")
stats = cur.fetchone()
print(f"\n📊 Statistics:")
print(f"  Total rows: {stats[0]}")
print(f"  Rows with float_id_precise: {stats[1]}")
print(f"  Unique float_id_precise values: {stats[2]}")

cur.close()
conn.close()
