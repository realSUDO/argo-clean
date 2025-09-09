import psycopg2

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="ocean",
    user="multiply",
    password="unlocked",
    port="5432"
)
cursor = conn.cursor()

# Count unique float_id_clean values
cursor.execute("SELECT COUNT(DISTINCT float_id_clean) FROM argo_measurements")
unique_count = cursor.fetchone()[0]

print(f"Unique float IDs count: {unique_count}")

# Close connection
conn.close()
