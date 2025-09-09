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
TABLE_NAME = "argo_measurements"

# -------------------------------
# CONNECT
# -------------------------------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
print(f"Connected to PostgreSQL database '{DB_CONFIG['dbname']}' as {DB_CONFIG['user']}")

# -------------------------------
# 1️⃣ Add column if not exists
# -------------------------------
print("[1/3] Adding column float_id_clean if not exists...")
cur.execute(f"""
    ALTER TABLE {TABLE_NAME}
    ADD COLUMN IF NOT EXISTS float_id_clean TEXT;
""")
conn.commit()
print("✅ Column ready.\n")

# -------------------------------
# 2️⃣ Update float_id_clean in one go
# -------------------------------
print("[2/3] Updating float_id_clean from source_file (all rows at once)...")
cur.execute(f"""
    UPDATE {TABLE_NAME}
    SET float_id_clean = substring(source_file FROM '([A-Z]\\d+)_')
    WHERE source_file IS NOT NULL;
""")
conn.commit()
print("✅ All rows updated successfully!\n")

# -------------------------------
# 3️⃣ Verify
# -------------------------------
cur.execute(f"SELECT DISTINCT float_id_clean FROM {TABLE_NAME} LIMIT 10;")
print("🎯 Sample cleaned float IDs:")
for row in cur.fetchall():
    print(row[0])

cur.close()
conn.close()
